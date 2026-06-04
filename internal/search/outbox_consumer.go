package search

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// OutboxConsumer 消费 canal-outbox 主题，并驱动搜索索引更新。
type OutboxConsumer struct {
	reader    *kafka.Reader
	projector *KnowPostProjector
	logger    *zap.Logger
}

func NewOutboxConsumer(reader *kafka.Reader, projector *KnowPostProjector, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || projector == nil {
		return nil
	}
	return &OutboxConsumer{reader: reader, projector: projector, logger: logger}
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
				c.logger.Warn("fetch search outbox kafka message failed", zap.Error(err))
			}
			if !sleepConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.handleMessage(ctx, msg.Value); err != nil {
			if c.logger != nil {
				c.logger.Warn("process search outbox kafka message failed", zap.Error(err))
			}
			if !sleepConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil && c.logger != nil {
			c.logger.Warn("commit search outbox kafka message failed", zap.Error(err))
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
		if err := c.projector.ProjectPayload(ctx, []byte(row.Payload)); err != nil {
			return err
		}
	}
	return nil
}

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
