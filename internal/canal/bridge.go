// Package canal 负责将阿里云 Canal 捕获的 MySQL binlog 变更桥接到 Kafka。
//
// 核心流程：
//
//	Canal client -> 订阅 outbox 表 binlog -> 解析 row events -> 打包 JSON -> 写入 Kafka
//
// 当 canal.enabled = true 时，该桥接器会在 background runner 中启动，
// 与 Java 版保持一致的「Canal -> Kafka -> relation/search consumers」链路。
// 当 canal.enabled = false 时，不会启动该桥接器。
package canal

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/withlin/canal-go/client"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/contextutil"
	pbe "github.com/withlin/canal-go/protocol/entry"
)

const (
	defaultCanalSocketTimeoutMs = 60_000
	defaultCanalIdleTimeoutMs   = 60 * 60 * 1000
)

// Bridge 负责把 Canal 中的 outbox 表变更桥接到 Kafka。
//
// 启动后会持续连接到 Canal Server，轮询订阅的 binlog 变更（filter 配置过滤规则）。
// 每次从 Canal 获取一批变更后，会按以下流程处理：
//  1. 调用 parseEntries 将 protobuf Entry 转为 JSON 格式的 CanalEnvelope
//  2. 将消息批量写入 Kafka 的 canal-outbox 主题
//  3. ACK 该 batchID，告知 Canal Server 已完成消费
//  4. 如果写入 Kafka 失败则 ROLLBACK batchID，后续重试会重新获取该批次
//
// 中断重连策略：当连接意外断开时，会以 IntervalMs 间隔无限重试，直到 ctx 被取消。
type Bridge struct {
	cfg    *config.CanalConfig
	writer *kafka.Writer
	logger *zap.Logger
}

// NewBridge 创建 Canal 桥接器实例。
//
// 功能：
//
//	根据配置创建 Bridge 实例。如果 Canal 未启用、配置为 nil 或 writer 未提供，
//	返回 nil（表示不应该启动 Canal 桥接器）。
//
// 参数：
//   - cfg:    Canal 配置（主机、端口、用户名、密码、过滤规则等）
//   - writer: 已配置的 Kafka Writer，用于写入 canal-outbox 主题
//   - logger: zap 结构化日志器
//
// 返回值：
//   - *Bridge: 桥接器实例，如果配置不完整返回 nil
//
// 设计决策：
//
//	返回 nil 而非 error 是因为 Canal 桥接器是可选组件。
//	配置未启用时，调用方只需跳过启动即可，不需要处理错误。
//	这样 App.Run() 中可以直接对 bridge 做 nil 检查：
//	  if b != nil { go b.Start(ctx) }
func NewBridge(cfg *config.CanalConfig, writer *kafka.Writer, logger *zap.Logger) *Bridge {
	if cfg == nil || !cfg.Enabled || writer == nil {
		return nil
	}
	return &Bridge{cfg: cfg, writer: writer, logger: logger}
}

// socketTimeoutMs 返回 Canal Socket 超时（毫秒）。
//
// 功能：优先使用配置值，未配置则返回默认值 60000ms（60秒）。
func (b *Bridge) socketTimeoutMs() int32 {
	if b.cfg.SocketTimeoutMs > 0 {
		return int32(b.cfg.SocketTimeoutMs)
	}
	return defaultCanalSocketTimeoutMs
}

// idleTimeoutMs 返回 Canal 空闲超时（毫秒）。
//
// 功能：优先使用配置值，未配置则返回默认值 3600000ms（1小时）。
func (b *Bridge) idleTimeoutMs() int32 {
	if b.cfg.IdleTimeoutMs > 0 {
		return int32(b.cfg.IdleTimeoutMs)
	}
	return defaultCanalIdleTimeoutMs
}

// Start 持续连接 Canal 并将 outbox 表变更写入 Kafka。
//
// 功能：
//
//	阻塞式运行，在无限循环中：
//	Step 1: 调用 runOnce 建立 Canal 连接并轮询 binlog 变更。
//	Step 2: 如果 runOnce 返回错误（网络断开、认证失败等），
//	        等待 IntervalMs 毫秒后重试。
//	Step 3: 如果 ctx 被取消，立即退出循环。
//
// 参数：
//   - ctx: 上下文。当 ctx 被取消时（服务关闭），方法会退出并关闭 Kafka Writer。
//
// 函数调用说明：
//   - sleepContext(ctx, delay):
//     自定义的休眠函数，在休眠期间监听 ctx.Done()。
//     如果 ctx 被取消，立即返回 false，避免在关闭过程中继续等待。
//   - defer b.writer.Close():
//     延迟关闭 Kafka Writer，确保在 Start 退出时清理连接。
//
// 设计决策：
//
//	retryDelay 使用 cfg.IntervalMs 作为重试间隔（最小 1 秒），
//	而不是指数退避。这是因为 Canal 服务的连接中断通常是临时性的
//	（网络抖动或重启），固定的短间隔重试更简单且足够。
//
//	WHY 不需要 leader 锁：
//	Canal Server 每个 destination 只允许一个客户端连接，自带单实例消费约束。
//	如果多个实例同时连接同一个 destination，Canal Server 会拒绝后续连接。
//	因此应用层不需要额外的分布式锁来选主。
func (b *Bridge) Start(ctx context.Context) {
	if b == nil {
		return
	}
	defer b.writer.Close()

	retryDelay := time.Duration(maxInt(b.cfg.IntervalMs, 1000)) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		err := b.runOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) && b.logger != nil {
			b.logger.Warn("canal bridge stopped unexpectedly, will retry", zap.Error(err))
		}
		if !contextutil.Sleep(ctx, retryDelay) {
			return
		}
	}
}

// runOnce 建立一次 Canal 连接并持续轮询 binlog 变更，直到遇到不可恢复的错误。
//
// 功能：
//
//	建立一次完整的 Canal 会话生命周期：
//	Step 1: 创建 SimpleCanalConnector 并连接 Canal Server。
//	Step 2: 订阅配置指定的 filter 规则（通常是 outbox 表的过滤表达式）。
//	Step 3: 回滚到 0 位置，从最新位置开始消费（不消费历史数据）。
//	Step 4: 轮询循环：
//	  a. 调用 connector.GetWithOutAck 获取一批 binlog 变更。
//	  b. 如果无数据（message.Id == -1 或 entries 为空），休眠 pollDelay 后继续。
//	  c. 调用 parseEntries 将 protobuf Entry 解析为 JSON 格式的 CanalEnvelope。
//	  d. 将 JSON 消息批量写入 Kafka（b.writer.WriteMessages）。
//	  e. 写入成功后调用 connector.Ack(batchID) 确认该批次。
//	  f. 写入失败则调用 connector.RollBack(batchID)，下次重试会重新获取该批次。
//
// 参数：
//   - ctx: 上下文，用于取消长时间运行的轮询循环
//
// 返回值：
//   - error: 遇到的第一个不可恢复错误（网络断开、认证失败等）
//
// 错误处理策略：
//   - RollBack：写入 Kafka 失败时回滚，下次重试能重新获取同一批数据
//   - Ack：写入 Kafka 成功后确认，Canal Server 会移动消费位点
//
// 函数调用说明：
//   - client.NewSimpleCanalConnector:
//     创建 Canal 客户端连接器，支持自动重连。
//   - connector.Subscribe(filter):
//     订阅 binlog 过滤规则，例如 "zhiguang\\.outbox" 只监听 outbox 表。
//   - connector.GetWithOutAck(batchSize):
//     获取一批 binlog 变更但不自动 ACK，需要手动调用 Ack/RollBack。
//   - parseEntries:
//     将 protobuf 格式的 Entry 转为 JSON 格式的 CanalEnvelope。
func (b *Bridge) runOnce(ctx context.Context) error {
	connector := client.NewSimpleCanalConnector(
		b.cfg.Host,
		b.cfg.Port,
		b.cfg.Username,
		b.cfg.Password,
		b.cfg.Destination,
		b.socketTimeoutMs(),
		b.idleTimeoutMs(),
	)
	if err := connector.Connect(); err != nil {
		return err
	}
	defer connector.DisConnection()

	if err := connector.Subscribe(b.cfg.Filter); err != nil {
		return err
	}

	pollDelay := time.Duration(maxInt(b.cfg.IntervalMs, 100)) * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		message, err := connector.GetWithOutAck(int32(maxInt(b.cfg.BatchSize, 1)), nil, nil)
		if err != nil {
			return err
		}

		if message.Id == -1 || len(message.Entries) == 0 {
			contextutil.Sleep(ctx, pollDelay)
			continue
		}

		batchID := message.Id
		payloads, err := parseEntries(entryPtrSlice(message.Entries))
		if err != nil {
			if rbErr := connector.RollBack(batchID); rbErr != nil {
				b.logger.Warn("rollback canal batch failed after parse error", zap.Int64("batchID", batchID), zap.Error(rbErr))
			}
			return err
		}
		if len(payloads) == 0 {
			if err := connector.Ack(batchID); err != nil {
				return err
			}
			continue
		}

		messages := make([]kafka.Message, 0, len(payloads))
		for _, payload := range payloads {
			var key []byte
			rows, err := outbox.ExtractRows(payload)
			if err != nil {
				if rbErr := connector.RollBack(batchID); rbErr != nil {
					b.logger.Warn("rollback canal batch failed after extract error", zap.Int64("batchID", batchID), zap.Error(rbErr))
				}
				return err
			}
			if len(rows) > 0 {
				key = []byte(outbox.MessageKey(rows[0]))
			}
			messages = append(messages, kafka.Message{Key: key, Value: payload})
		}
		if err := b.writer.WriteMessages(ctx, messages...); err != nil {
			if rbErr := connector.RollBack(batchID); rbErr != nil {
				b.logger.Warn("rollback canal batch failed after write error", zap.Int64("batchID", batchID), zap.Error(rbErr))
			}
			return err
		}
		if err := connector.Ack(batchID); err != nil {
			return err
		}
	}
}

// maxInt 返回两个 int 中较大的一个，同时确保有效值至少为 1。
//
// 功能：
//
//	如果 value > 0 返回 value，否则返回 fallback。
//	用于将配置值规范化为正数，避免 0 或负数对语义造成影响。
//
// 参数：
//   - value:    配置值（可能为 0 或负数，表示未设置）
//   - fallback: 回退值（默认值）
//
// 返回值：
//   - int: 规范化后的值，保证 > 0
//
// 设计决策：
//
//	不使用 math.MaxInt 是为了不引入
//	标准库依赖（虽然影响很小，但此处逻辑足够简单）。
func maxInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// entryPtrSlice 将 []pbe.Entry 转为 []*pbe.Entry，避免值拷贝锁问题。
func entryPtrSlice(entries []pbe.Entry) []*pbe.Entry {
	result := make([]*pbe.Entry, len(entries))
	for i := range entries {
		result[i] = &entries[i]
	}
	return result
}

