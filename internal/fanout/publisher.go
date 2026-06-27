package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/internal/model"
	"go.uber.org/zap"
)

type FanoutPublisher struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func NewFanoutPublisher(brokers []string, topic string, logger *zap.Logger) *FanoutPublisher {
	if logger == nil {
		logger = zap.L()
	}
	return &FanoutPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireOne,
			Async:                  true,
			AllowAutoTopicCreation: false,
			WriteTimeout:           10 * time.Second,
		},
		logger: logger,
	}
}

func (p *FanoutPublisher) Publish(ctx context.Context, event *model.FanoutEvent) error {
	if p == nil || p.writer == nil {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("fanout publisher: marshal: %w", err)
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Value: data})
}

func (p *FanoutPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
