package search

import (
	"context"
	"testing"
)

func TestHandleMessageMarksMalformedEnvelope(t *testing.T) {
	t.Parallel()

	consumer := &OutboxConsumer{}
	err := consumer.handleMessage(context.Background(), []byte("not-json"))
	if err == nil {
		t.Fatal("expected malformed error")
	}
	if !isMalformedSearchOutboxMessage(err) {
		t.Fatalf("expected malformed search outbox error, got %T", err)
	}
}
