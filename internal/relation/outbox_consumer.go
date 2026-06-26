package relation

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

type OutboxConsumer struct {
	inner *outbox.Consumer
}

func NewOutboxConsumer(reader *kafka.Reader, processor *EventProcessor, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || processor == nil {
		return nil
	}
	handler := &RelationRowHandler{Processor: processor}
	inner := outbox.NewConsumer(reader, handler, logger)
	return &OutboxConsumer{inner: inner}
}

func (c *OutboxConsumer) Start(ctx context.Context) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.Start(ctx)
}

type RelationRowHandler struct {
	Processor *EventProcessor
}

func (h *RelationRowHandler) HandleRow(ctx context.Context, row outbox.Row) error {
	if len(row.Payload) == 0 || row.AggregateType != "following" {
		return nil
	}
	if row.Type != "FollowCreated" && row.Type != "FollowCanceled" {
		return nil
	}
	var evt RelationEvent
	if err := json.Unmarshal(row.Payload, &evt); err != nil {
		return err
	}
	return h.Processor.Process(ctx, evt)
}
