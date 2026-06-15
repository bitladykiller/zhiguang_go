package relation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// OutboxConsumer 消费 canal-outbox 主题中的关系事件。
//
// 幂等策略：
//   - 使用共享水位线屏蔽 commit 失败后的重复投递。
//   - 保留 EventProcessor 内部的短期去重，覆盖“副作用已执行但水位线未推进”的小窗口。
type OutboxConsumer struct {
	reader    *kafka.Reader
	processor *EventProcessor
	logger    *zap.Logger
	watermark *outbox.WatermarkTracker
}

// NewOutboxConsumer 创建 Kafka outbox 消费者实例。
func NewOutboxConsumer(reader *kafka.Reader, processor *EventProcessor, redisClient *redis.Client, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || processor == nil {
		return nil
	}

	readerCfg := reader.Config()
	return &OutboxConsumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
		watermark: outbox.NewWatermarkTracker(redisClient, readerCfg.GroupID, readerCfg.Topic),
	}
}

// Start 启动 Kafka 消费循环，持续处理 outbox 事件直到上下文取消。
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
			c.logWarn("fetch relation outbox kafka message failed", err)
			if !sleepRelationConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		applied, err := c.isMessageApplied(ctx, msg.Partition, msg.Offset)
		if err != nil {
			c.logWarn("check relation outbox watermark failed", err)
			if !sleepRelationConsumer(ctx, time.Second) {
				return
			}
			continue
		}
		if applied {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logWarn("commit already applied relation outbox kafka message failed", err)
			}
			continue
		}

		if err := c.handleMessage(ctx, msg.Value); err != nil {
			if isMalformedRelationOutboxMessage(err) {
				c.skipMalformedMessage(ctx, msg, err)
				continue
			}
			c.logWarn("process relation outbox kafka message failed", err)
			if !sleepRelationConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.advanceAppliedOffset(ctx, msg.Partition, msg.Offset); err != nil {
			c.logWarn("advance relation outbox watermark failed", err)
			if !sleepRelationConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logWarn("commit relation outbox kafka message failed", err)
		}
	}
}

func (c *OutboxConsumer) handleMessage(ctx context.Context, value []byte) error {
	rows, err := outbox.ExtractRows(value)
	if err != nil {
		return malformedRelationOutboxMessage(err)
	}

	for _, row := range rows {
		if row.Payload == "" {
			continue
		}
		if !isRelationOutboxRow(row) {
			continue
		}

		var evt RelationEvent
		if err := json.Unmarshal([]byte(row.Payload), &evt); err != nil {
			return malformedRelationOutboxMessage(err)
		}
		if err := c.processor.Process(ctx, evt); err != nil {
			return fmt.Errorf("process relation outbox event: %w", err)
		}
	}

	return nil
}

func isRelationOutboxRow(row outbox.CanalRow) bool {
	if row.AggregateType != "following" {
		return false
	}
	return row.Type == "FollowCreated" || row.Type == "FollowCanceled"
}

func (c *OutboxConsumer) isMessageApplied(ctx context.Context, partition int, offset int64) (bool, error) {
	if c == nil || c.watermark == nil {
		return false, nil
	}
	lastApplied, err := c.watermark.LastApplied(ctx, partition)
	if err != nil {
		return false, err
	}
	return offset <= lastApplied, nil
}

func (c *OutboxConsumer) advanceAppliedOffset(ctx context.Context, partition int, offset int64) error {
	if c == nil || c.watermark == nil {
		return nil
	}
	return c.watermark.Advance(ctx, partition, offset)
}

func (c *OutboxConsumer) skipMalformedMessage(ctx context.Context, msg kafka.Message, cause error) {
	c.logWarn("skip malformed relation outbox kafka message", cause)
	if err := c.advanceAppliedOffset(ctx, msg.Partition, msg.Offset); err != nil {
		c.logWarn("advance malformed relation outbox watermark failed", err)
		return
	}
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logWarn("commit malformed relation outbox kafka message failed", err)
	}
}

func (c *OutboxConsumer) logWarn(msg string, err error) {
	if c != nil && c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}

type malformedRelationOutboxError struct {
	err error
}

func (e malformedRelationOutboxError) Error() string {
	return e.err.Error()
}

func (e malformedRelationOutboxError) Unwrap() error {
	return e.err
}

func malformedRelationOutboxMessage(err error) error {
	return malformedRelationOutboxError{err: err}
}

func isMalformedRelationOutboxMessage(err error) bool {
	var target malformedRelationOutboxError
	return errors.As(err, &target)
}

// sleepRelationConsumer 可中断的休眠等待，用于消费失败后的重试延迟。
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
