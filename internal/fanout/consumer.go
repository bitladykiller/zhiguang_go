package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/internal/model"
	"github.com/zhiguang/app/pkg/contextutil"
	"go.uber.org/zap"
)

type FanoutConsumer struct {
	service   *Service
	reader    *kafka.Reader
	logger    *zap.Logger
	closeOnce sync.Once
}

func NewFanoutConsumer(brokers []string, groupID string, topic string, service *Service, logger *zap.Logger) *FanoutConsumer {
	if logger == nil {
		logger = zap.L()
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return &FanoutConsumer{
		service: service,
		reader:  reader,
		logger:  logger,
	}
}

func (fc *FanoutConsumer) Start(ctx context.Context) {
	if fc == nil || fc.reader == nil {
		return
	}
	defer fc.closeOnce.Do(func() { fc.reader.Close() })
	defer func() {
		if r := recover(); r != nil {
			fc.logger.Error("fanout consumer panicked", zap.Any("panic", r), zap.Stack("stack"))
		}
	}()

	fetchLimit := 100
	for {
		msgs := make([]kafka.Message, 0, fetchLimit)
		for i := 0; i < fetchLimit; i++ {
			msg, err := fc.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fc.logger.Warn("fanout consumer: fetch message failed", zap.Error(err))
				if !contextutil.Sleep(ctx, time.Second) {
					return
				}
				break
			}
			msgs = append(msgs, msg)
		}
		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			var event model.FanoutEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				fc.logger.Warn("fanout consumer: unmarshal event failed", zap.Error(err))
				if err := fc.reader.CommitMessages(ctx, msg); err != nil {
					fc.logger.Warn("fanout consumer: commit message failed", zap.Error(err))
				}
				continue
			}
			if err := fc.service.FanoutPost(ctx, &event); err != nil {
				fc.logger.Error("fanout consumer: fanout post failed",
					zap.Uint64("postID", event.PostID),
					zap.Error(err),
				)
			}
			if err := fc.reader.CommitMessages(ctx, msg); err != nil {
				fc.logger.Warn("fanout consumer: commit message failed", zap.Error(err))
			}
		}
	}
}

func (fc *FanoutConsumer) Stop() error {
	if fc == nil || fc.reader == nil {
		return nil
	}
	var err error
	fc.closeOnce.Do(func() { err = fc.reader.Close() })
	return err
}

func (fc *FanoutConsumer) String() string {
	return fmt.Sprintf("fanout-consumer(topic=%s)", fc.reader.Config().Topic)
}
