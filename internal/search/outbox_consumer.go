package search

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// OutboxConsumer 消费 canal-outbox 主题中的 search 事件，并驱动搜索索引更新。
//
// 幂等策略：
//   - 处理成功后推进 `consumer-group + topic + partition` 维度的共享水位线。
//   - commit 失败导致的重复投递会先命中水位线检查，再直接跳过副作用处理。
//   - 对于明确无法解析的坏消息，同样推进水位线，避免卡死整个分区。
type OutboxConsumer struct {
	reader    *kafka.Reader
	projector *KnowPostProjector
	logger    *zap.Logger
	watermark *outbox.WatermarkTracker
}

// NewOutboxConsumer 创建搜索 outbox 消费者。
func NewOutboxConsumer(reader *kafka.Reader, projector *KnowPostProjector, redisClient *redis.Client, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || projector == nil {
		return nil
	}

	readerCfg := reader.Config()
	return &OutboxConsumer{
		reader:    reader,
		projector: projector,
		logger:    logger,
		watermark: outbox.NewWatermarkTracker(redisClient, readerCfg.GroupID, readerCfg.Topic),
	}
}

// Start 启动后台消费循环。
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
			c.logWarn("fetch search outbox kafka message failed", err)
			if !sleepConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		applied, err := c.isMessageApplied(ctx, msg.Partition, msg.Offset)
		if err != nil {
			c.logWarn("check search outbox watermark failed", err)
			if !sleepConsumer(ctx, time.Second) {
				return
			}
			continue
		}
		if applied {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logWarn("commit already applied search outbox kafka message failed", err)
			}
			continue
		}

		if err := c.handleMessage(ctx, msg.Value); err != nil {
			if isMalformedSearchOutboxMessage(err) {
				c.skipMalformedMessage(ctx, msg, err)
				continue
			}
			c.logWarn("process search outbox kafka message failed", err)
			if !sleepConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.advanceAppliedOffset(ctx, msg.Partition, msg.Offset); err != nil {
			c.logWarn("advance search outbox watermark failed", err)
			if !sleepConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logWarn("commit search outbox kafka message failed", err)
		}
	}
}

func (c *OutboxConsumer) handleMessage(ctx context.Context, value []byte) error {
	rows, err := outbox.ExtractRows(value)
	if err != nil {
		return malformedSearchOutboxMessage(err)
	}
	for _, row := range rows {
		if row.Payload == "" {
			continue
		}
		if err := c.projector.ProjectPayload(ctx, []byte(row.Payload)); err != nil {
			return fmt.Errorf("project search outbox payload: %w", err)
		}
	}
	return nil
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
	c.logWarn("skip malformed search outbox kafka message", cause)
	if err := c.advanceAppliedOffset(ctx, msg.Partition, msg.Offset); err != nil {
		c.logWarn("advance malformed search outbox watermark failed", err)
		return
	}
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logWarn("commit malformed search outbox kafka message failed", err)
	}
}

func (c *OutboxConsumer) logWarn(msg string, err error) {
	if c != nil && c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}

type malformedSearchOutboxError struct {
	err error
}

func (e malformedSearchOutboxError) Error() string {
	return e.err.Error()
}

func (e malformedSearchOutboxError) Unwrap() error {
	return e.err
}

func malformedSearchOutboxMessage(err error) error {
	return malformedSearchOutboxError{err: err}
}

func isMalformedSearchOutboxMessage(err error) bool {
	var target malformedSearchOutboxError
	return errors.As(err, &target)
}

// sleepConsumer 在指定时长内等待，并在等待期间监听上下文取消信号。
func sleepConsumer(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
