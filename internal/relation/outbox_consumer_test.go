package relation

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
	if !isMalformedRelationOutboxMessage(err) {
		t.Fatalf("expected malformed relation outbox error, got %T", err)
	}
}
