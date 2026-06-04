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

func (c *OutboxConsumer) handleMessage(ctx context.Context, value []byte) error {
	rows, err := outbox.ExtractRows(value)
	if err != nil {
		return err
	}

	for _, row := range rows {
		if row.Payload == "" {
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
