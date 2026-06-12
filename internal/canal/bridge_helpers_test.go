package canal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/pkg/config"
)

func TestNewBridge(t *testing.T) {
	logger := zap.NewNop()
	writer := &kafka.Writer{}

	if bridge := NewBridge(nil, writer, logger); bridge != nil {
		t.Fatal("expected nil bridge when config is nil")
	}

	if bridge := NewBridge(&config.CanalConfig{Enabled: false}, writer, logger); bridge != nil {
		t.Fatal("expected nil bridge when canal is disabled")
	}

	if bridge := NewBridge(&config.CanalConfig{Enabled: true}, nil, logger); bridge != nil {
		t.Fatal("expected nil bridge when writer is nil")
	}

	bridge := NewBridge(&config.CanalConfig{Enabled: true}, writer, logger)
	if bridge == nil {
		t.Fatal("expected enabled bridge to be constructed")
	}
}

func TestMaxInt(t *testing.T) {
	if got := maxInt(5, 1); got != 5 {
		t.Fatalf("maxInt positive = %d", got)
	}
	if got := maxInt(0, 7); got != 7 {
		t.Fatalf("maxInt zero fallback = %d", got)
	}
	if got := maxInt(-3, 9); got != 9 {
		t.Fatalf("maxInt negative fallback = %d", got)
	}
}

func TestSleepContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if ok := sleepContext(ctx, time.Second); ok {
		t.Fatal("expected canceled context to interrupt sleep")
	}
}

func TestBuildMessagesUsesOutboxMessageKey(t *testing.T) {
	payload, err := json.Marshal(outbox.CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []outbox.CanalRow{
			{
				ID:            "1",
				AggregateType: "knowpost",
				AggregateID:   "42",
				Type:          "KnowPostPublished",
				Payload:       `{"entity":"knowpost","id":42}`,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal canal envelope: %v", err)
	}

	messages, err := buildMessages([][]byte{payload})
	if err != nil {
		t.Fatalf("buildMessages() error = %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one kafka message, got %d", len(messages))
	}
	if got := string(messages[0].Key); got != "knowpost:42" {
		t.Fatalf("message key = %q, want knowpost:42", got)
	}
}
