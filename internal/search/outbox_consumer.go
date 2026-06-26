package search

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

type OutboxConsumer struct {
	inner *outbox.Consumer
}

func NewOutboxConsumer(reader *kafka.Reader, projector *KnowPostProjector, logger *zap.Logger) *OutboxConsumer {
	if reader == nil || projector == nil {
		return nil
	}
	handler := &SearchRowHandler{Projector: projector}
	inner := outbox.NewConsumer(reader, handler, logger)
	return &OutboxConsumer{inner: inner}
}

func (c *OutboxConsumer) Start(ctx context.Context) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.Start(ctx)
}

type SearchRowHandler struct {
	Projector *KnowPostProjector
}

func (h *SearchRowHandler) HandleRow(ctx context.Context, row outbox.Row) error {
	if len(row.Payload) == 0 {
		return nil
	}
	return h.Projector.ProjectPayload(ctx, row.Payload)
}
