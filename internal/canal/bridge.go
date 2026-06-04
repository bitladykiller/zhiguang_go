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

	"github.com/zhiguang/app/pkg/config"
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
//   根据配置创建 Bridge 实例。如果 Canal 未启用、配置为 nil 或 writer 未提供，
//   返回 nil（表示不应该启动 Canal 桥接器）。
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
//   返回 nil 而非 error 是因为 Canal 桥接器是可选组件。
//   配置未启用时，调用方只需跳过启动即可，不需要处理错误。
//   这样 App.Run() 中可以直接对 bridge 做 nil 检查：
//     if b != nil { go b.Start(ctx) }
func NewBridge(cfg *config.CanalConfig, writer *kafka.Writer, logger *zap.Logger) *Bridge {
	if cfg == nil || !cfg.Enabled || writer == nil {
		return nil
	}
	return &Bridge{cfg: cfg, writer: writer, logger: logger}
}

// Start 持续连接 Canal 并将 outbox 表变更写入 Kafka。
//
// 功能：
//   阻塞式运行，在无限循环中：
//   Step 1: 调用 runOnce 建立 Canal 连接并轮询 binlog 变更。
//   Step 2: 如果 runOnce 返回错误（网络断开、认证失败等），
//           等待 IntervalMs 毫秒后重试（以 Bridge 级别的重试间隔）。
//   Step 3: 如果 ctx 被取消，立即退出循环。
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
//   retryDelay 使用 cfg.IntervalMs 作为重试间隔（最小 1 秒），
//   而不是指数退避。这是因为 Canal 服务的连接中断通常是临时性的
//   （网络抖动或重启），固定的短间隔重试更简单且足够。
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
		if err := b.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) && b.logger != nil {
			b.logger.Warn("canal bridge stopped unexpectedly, will retry", zap.Error(err))
		}
		if !sleepContext(ctx, retryDelay) {
			return
		}
	}
}

// runOnce 建立一次 Canal 连接并持续轮询 binlog 变更，直到遇到不可恢复的错误。
//
// 功能：
//   建立一次完整的 Canal 会话生命周期：
//   Step 1: 创建 SimpleCanalConnector 并连接 Canal Server。
//   Step 2: 订阅配置指定的 filter 规则（通常是 outbox 表的过滤表达式）。
//   Step 3: 回滚到 0 位置，从最新位置开始消费（不消费历史数据）。
//   Step 4: 轮询循环：
//     a. 调用 connector.GetWithOutAck 获取一批 binlog 变更。
//     b. 如果无数据（message.Id == -1 或 entries 为空），休眠 pollDelay 后继续。
//     c. 调用 parseEntries 将 protobuf Entry 解析为 JSON 格式的 CanalEnvelope。
//     d. 将 JSON 消息批量写入 Kafka（b.writer.WriteMessages）。
//     e. 写入成功后，调用 connector.Ack(batchID) 确认消费。
//     f. 写入失败时，调用 connector.RollBack(batchID) 回滚，后续重试会重新获取该批次。
//   Step 5: 遇到错误（Canal 连接断开、Kafka 写入失败等）时断开连接并返回错误。
//
// 参数：
//   - ctx: 上下文，用于取消轮询
//
// 返回值：
//   - error: 连接失败、订阅失败、Canal 轮询错误或 Kafka 写入错误时返回
//
// 函数调用说明（canal-go client connector）：
//   - client.NewSimpleCanalConnector(host, port, username, password, destination, soTimeout, idleTimeout):
//     创建 Canal 客户端连接器。destination 是 Canal 实例名（对应 Canal Server 配置中的实例名）。
//     soTimeout 是套接字超时（毫秒），idleTimeout 是空闲超时（毫秒）。
//   - connector.Connect():
//     建立与 Canal Server 的 TCP 连接，并进行认证握手。
//     返回 error（连接失败或认证失败）。
//   - connector.Subscribe(filter):
//     订阅指定过滤规则的 binlog 变更。filter 格式为 MySQL 表名模式，
//     例如 "test\\.outbox" 表示只订阅 test 数据库的 outbox 表。
//   - connector.RollBack(batchID):
//     回滚到指定 batchID。传入 0 表示从最新位置开始消费。
//   - connector.GetWithOutAck(batchSize, timeout, unit):
//     获取一批 binlog 变更，不自动确认（需要手动 Ack）。
//     batchSize 指定最大条目数。返回 Message 包含 Id（batchID）和 Entries（变更条目）。
//   - connector.Ack(batchID):
//     确认消费指定的 batchID，告知 Canal Server 可以丢弃该批次。
//   - connector.RollBack(batchID):
//     回滚到指定的 batchID，下次 GetWithOutAck 会重新获取该批次的变更。
//
// 边界情况：
//   - message == nil 或 message.Id == -1: 无可用数据，继续轮询
//   - parseEntries 返回空数组: 可能所有条目都是 DELETE 类型（被过滤），
//     此时直接 Ack 避免累积未确认的消息
//   - Kafka 写入失败: RollBack 让 Canal 保留该批次，重试时重新处理
//   - ctx 被取消: 返回 ctx.Err()，上层 Start 循环会退出
func (b *Bridge) runOnce(ctx context.Context) error {
	connector := client.NewSimpleCanalConnector(
		b.cfg.Host,
		b.cfg.Port,
		b.cfg.Username,
		b.cfg.Password,
		b.cfg.Destination,
		defaultCanalSocketTimeoutMs,
		defaultCanalIdleTimeoutMs,
	)

	if err := connector.Connect(); err != nil {
		return err
	}
	defer connector.DisConnection()

	if err := connector.Subscribe(b.cfg.Filter); err != nil {
		return err
	}
	if err := connector.RollBack(0); err != nil {
		return err
	}

	if b.logger != nil {
		b.logger.Info("canal bridge connected",
			zap.String("host", b.cfg.Host),
			zap.Int("port", b.cfg.Port),
			zap.String("destination", b.cfg.Destination),
			zap.String("filter", b.cfg.Filter),
			zap.Int("batch_size", b.cfg.BatchSize),
		)
	}

	pollDelay := time.Duration(maxInt(b.cfg.IntervalMs, 100)) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		message, err := connector.GetWithOutAck(int32(maxInt(b.cfg.BatchSize, 1)), nil, nil)
		if err != nil {
			return err
		}
		if message == nil || message.Id == -1 || len(message.Entries) == 0 {
			if !sleepContext(ctx, pollDelay) {
				return ctx.Err()
			}
			continue
		}

		batchID := message.Id
		payloads, err := parseEntries(message.Entries)
		if err != nil {
			_ = connector.RollBack(batchID)
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
			messages = append(messages, kafka.Message{Value: payload})
		}
		if err := b.writer.WriteMessages(ctx, messages...); err != nil {
			_ = connector.RollBack(batchID)
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
//   如果 value > 0 返回 value，否则返回 fallback。
//   用于将配置值规范化为正数，避免 0 或负数对语义造成影响。
//
// 参数：
//   - value:    配置值（可能为 0 或负数，表示未设置）
//   - fallback: 回退值（默认值）
//
// 返回值：
//   - int: 规范化后的值，保证 > 0
//
// 设计决策：
//   不使用 math.MaxInt 是为了不引入
//   标准库依赖（虽然影响很小，但此处逻辑足够简单）。
func maxInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// sleepContext 是一个可被上下文取消的休眠函数。
//
// 功能：
//   使用 time.NewTimer 创建一个定时器，通过 select 同时监听
//   timer 到期和 ctx.Done()。如果 ctx 在定时器到期前被取消，
//   立即返回 false（不等待定时器到期）。
//
// 参数：
//   - ctx: 上下文，用于取消休眠
//   - d:   休眠时长
//
// 返回值：
//   - bool: true=正常完成休眠，false=ctx 被取消
//
// 函数调用说明：
//   - time.NewTimer(d):
//     Go 标准库定时器，创建后 d 时长后 timer.C 通道会收到一个值。
//   - defer timer.Stop():
//     确保函数返回时停止定时器，防止 goroutine 泄漏。
//     如果不 Stop，已过期但未消费的 timer 会一直持有资源。
//
// 设计决策：
//   使用 NewTimer + defer Stop 而非 time.After，
//   因为 time.After 创建的定时器如果被 select 抛弃且未到期，
//   会直到到期才被 GC 回收，可能造成内存泄漏。
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
