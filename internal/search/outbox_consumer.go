package search

import (
	"context"
	"time"

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
