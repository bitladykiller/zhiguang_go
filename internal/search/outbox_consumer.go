package search

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// OutboxConsumer 消费 canal-outbox 主题中的 search 事件，并驱动搜索索引更新。
//
// 处理流程：
//  1. 从 canal-outbox Kafka 主题拉取消息。
//  2. 通过 outbox.ExtractRows 解析出 outbox 行数组。
//  3. 对每一行，调用 KnowPostProjector.ProjectPayload 执行 upsert/delete 操作。
//  4. 处理成功后 CommitMessages；失败后等待 1 秒重试。
//
// 搜索索引同步是最终一致（eventual consistency）的：
// 写操作完成后到索引可见有一个短暂延迟（通常 < 1s）。
type OutboxConsumer struct {
	reader    *kafka.Reader
	projector *KnowPostProjector
	logger    *zap.Logger
	component string
}

// NewOutboxConsumer 创建 OutboxConsumer 实例，消费 canal-outbox Kafka 主题中的搜索事件。
//
// 参数:
//   - reader: Kafka 读取器，已配置为订阅 canal-outbox 主题并使用搜索专用的消费组
//   - projector: 知文搜索索引投影器，负责将 outbox 事件转换为 ES 索引操作
//   - logger: zap 日志实例
//
// 返回值:
//   - *OutboxConsumer: 消费者实例；reader 或 projector 为 nil 时返回 nil
//
// 说明:
//   使用 kafka-go 库的 Reader（非 ConsumerGroup 接口），
//   由外部在 reader 创建时指定 ConsumerGroup。
//   kafka-go 的 Reader.FetchMessage + CommitMessages 操作模式
//   提供了精确的手动提交控制，确保消息处理完成后再提交 offset。
func NewOutboxConsumer(reader *kafka.Reader, projector *KnowPostProjector, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || projector == nil {
		return nil
	}
	return &OutboxConsumer{reader: reader, projector: projector, logger: logger, component: "search"}
}

// Start 启动后台消费循环，持续从 Kafka 拉取消息并处理搜索索引的 outbox 事件。
//
// 处理流程:
//  1. 使用 c.reader.FetchMessage 轮询拉取消息（阻塞调用）
//  2. 对每一条消息调用 handleMessage 执行 outbox 行解析和投影
//  3. 处理成功后调用 c.reader.CommitMessages 提交 offset
//  4. 处理失败时等待 1 秒后重试，直到上下文取消
//
// 错误处理:
//   - 消息拉取失败（FetchMessage）时打印 warn 日志并等待 1 秒重试
//   - 消息处理失败（handleMessage）时打印 warn 日志并等待 1 秒重试
//   - 上下文取消时（ctx.Done）立即退出循环并关闭 reader
//
// note: 当前为单线程消费模式，适用于中等吞吐量场景。
// 如果未来消息量增大，可考虑使用 goroutine 池并行处理消息，
// 并配合 CommitMessages 的同步机制控制 offset 提交。
//
// 边界情况:
//   - 接收器为 nil（c == nil）时直接返回，避免 nil 指针 panic
//   - defer c.reader.Close() 确保函数退出时释放 Kafka 连接
func (c *OutboxConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	outbox.StartConsumerLoop(ctx, c.reader, c.handleMessage, c.logger, c.component)
}

// handleMessage 解析 Kafka 消息体中的 Canal outbox 事件数据，并调用 projector 执行索引更新。
//
// 参数:
//   - ctx: 上下文对象
//   - value: Kafka 消息的 value 字节切片，包含 Canal JSON 格式的 outbox 事件数据
//
// 返回值:
//   - error: outbox.ExtractRows 解析失败或 ProjectPayload 执行失败时返回
//
// 处理流程:
//  1. 调用 outbox.ExtractRows(value) 从 Canal JSON 中提取 outbox 行数组
//  2. 遍历每一行，跳过 Payload 为空的行
//  3. 对非空 Payload，调用 c.projector.ProjectPayload 执行 upsert/delete
//
// 边界情况:
//   - value 为空或格式不合法 → outbox.ExtractRows 返回错误
//   - 某些 outbox 行的 Payload 可能为空（非 search 相关的事件行）→ 跳过
//   - 其中一行处理失败时立即返回错误，不再处理后续行（事务语义）
func (c *OutboxConsumer) handleMessage(ctx context.Context, value []byte) error {
	rows, err := outbox.ExtractRows(value)
	if err != nil {
		return fmt.Errorf("search outbox consumer: extract rows: %w", err)
	}
	for _, row := range rows {
		if row.Payload == "" {
			continue
		}
		if err := c.projector.ProjectPayload(ctx, []byte(row.Payload)); err != nil {
			return fmt.Errorf("search outbox consumer: project payload: %w", err)
		}
	}
	return nil
}
