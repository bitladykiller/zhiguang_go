package relation

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// OutboxConsumer 消费 canal-outbox 主题中的关系事件。
//
// 处理流程：
//  1. 从 canal-outbox Kafka 主题拉取消息（原始消息是 CanalEnvelope JSON）。
//  2. 解析出 outbox 行，提取 Payload 中的 RelationEvent。
//  3. 调用 EventProcessor.Process 更新 Redis ZSet 缓存和用户计数。
//  4. 处理成功后 CommitMessages 提交偏移量；失败后重试。
//
// 容错策略：
//   - 消费 Kafka 消息失败时，先等待 1 秒再重试，而不是立即重试。
//   - 当 ctx 被取消（服务关闭）时，停止消费循环并清理 Reader。
type OutboxConsumer struct {
	reader    *kafka.Reader
	processor *EventProcessor
	logger    *zap.Logger
}

// NewOutboxConsumer 创建 Kafka outbox 消费者实例，负责消费关系事件。
//
// 参数：
//   - reader: *kafka.Reader，Kafka 消费者，从 canal-outbox 主题读取消息。
//   - processor: *EventProcessor，事件处理器，处理解析后的事件。
//   - logger: *zap.Logger，日志记录器，可为 nil。
//
// 返回值：*OutboxConsumer，如果 reader 或 processor 为 nil 则返回 nil。
//
// 设计决策：
//
//	 当依赖缺失时返回 nil，由调用方（bootstrap）决定如何处理。
//	consumer.Start() 在 nil receiver 时直接返回，不会 panic。
func NewOutboxConsumer(reader *kafka.Reader, processor *EventProcessor, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || processor == nil {
		return nil
	}
	return &OutboxConsumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
	}
}

// Start 启动 Kafka 消费循环，持续处理 outbox 事件直到上下文取消。
//
// 功能：无限循环从 Kafka 拉取消息，解析并处理每个消息中的事件。
//
// 消费流程：
//  1. reader.FetchMessage(ctx) 阻塞等待下一条消息。
//  2. 如果 FetchMessage 出错且 ctx 未取消，等待 1 秒后重试。
//  3. 调用 handleMessage 解析出 outbox 事件并交给 EventProcessor 处理。
//  4. 处理成功后调用 reader.CommitMessages 提交偏移量。
//  5. 处理失败时等待 1 秒后重试同一条消息（最多重试到 ctx 取消）。
//
// 容错策略：
//   - 临时故障（如 Redis 连接超时）会重试，不会丢失消息。
//   - ctx.Done 时（服务关闭），退出循环并关闭 reader。
//   - 使用 sleepRelationConsumer 可取消的等待，避免关闭时挂起。
//
// 参数：
//   - ctx: context.Context，用于控制消费生命周期。取消 ctx 将停止消费。
func (c *OutboxConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if c.logger != nil {
				c.logger.Warn("fetch relation outbox kafka message failed", zap.Error(err))
			}
			if !sleepRelationConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.handleMessage(ctx, msg.Value); err != nil {
			if c.logger != nil {
				c.logger.Warn("process relation outbox kafka message failed", zap.Error(err))
			}
			if !sleepRelationConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil && c.logger != nil {
			c.logger.Warn("commit relation outbox kafka message failed", zap.Error(err))
		}
	}
}

// handleMessage 解析一条 Kafka 消息中的 outbox 事件行，并依次处理。
//
// 功能：Kafka 消息的 value 是 canal-outbox 组件的 CanalEnvelope JSON，
// 需要通过 outbox.ExtractRows 提取出 outbox 行数组。每行包含一个 Payload 字段，
// 其中存储了 RelationEvent 的 JSON 字符串。
//
// 参数：
//   - ctx: context.Context。
//   - value: []byte，Kafka 消息的 value（CanalEnvelope JSON）。
//
// 返回值：
//   - error: 解析失败或事件处理失败时返回错误。
//
// 边界情况：
//   - row.Payload 为空字符串：跳过该行，继续处理下一行。
//   - 解析 RelationEvent 失败：返回错误，触发外层重试机制。
func (c *OutboxConsumer) handleMessage(ctx context.Context, value []byte) error {
	rows, err := outbox.ExtractRows(value)
	if err != nil {
		return err
	}

	for _, row := range rows {
		if row.Payload == "" {
			continue
		}
		if row.AggregateType != "following" && row.Type != "FollowCreated" && row.Type != "FollowCanceled" {
			continue
		}

		var evt RelationEvent
		if err := json.Unmarshal([]byte(row.Payload), &evt); err != nil {
			return err
		}
		if err := c.processor.Process(ctx, evt); err != nil {
			return err
		}
	}

	return nil
}

// sleepRelationConsumer 可中断的休眠等待，用于消费失败后的重试延迟。
//
// 功能：在失败后等待指定时间再重试。如果 ctx 在等待期间被取消（服务关闭），
// 立即返回 false 以终止消费循环。
//
// 参数：
//   - ctx: context.Context，控制生命周期的上下文。
//   - d: time.Duration，等待时长。
//
// 返回值：
//   - bool: true 表示正常等待到超时；false 表示 ctx 被取消。
func sleepRelationConsumer(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
