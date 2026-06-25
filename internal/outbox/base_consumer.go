package outbox

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/contextutil"
)

type MessageHandler func(ctx context.Context, value []byte) error

const defaultConsumerSleepInterval = time.Second

func StartConsumerLoop(ctx context.Context, reader *kafka.Reader, handler MessageHandler, logger *zap.Logger, component string) {
	if reader == nil {
		return
	}
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if logger != nil {
				logger.Warn("fetch "+component+" kafka message failed", zap.Error(err))
			}
			if !contextutil.Sleep(ctx, defaultConsumerSleepInterval) {
				return
			}
			continue
		}

		if err := handler(ctx, msg.Value); err != nil {
			if logger != nil {
				logger.Warn("process "+component+" kafka message failed", zap.Error(err))
			}
			if !contextutil.Sleep(ctx, defaultConsumerSleepInterval) {
				return
			}
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil && logger != nil {
			logger.Warn("commit "+component+" kafka message failed", zap.Error(err))
		}
	}
}
