package search

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/zhiguang/app/internal/outbox"
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

func TestIsSearchOutboxRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  outbox.CanalRow
		want bool
	}{
		{
			name: "knowpost row",
			row: outbox.CanalRow{
				AggregateType: "knowpost",
			},
			want: true,
		},
		{
			name: "relation row",
			row: outbox.CanalRow{
				AggregateType: "following",
			},
			want: false,
		},
		{
			name: "empty aggregate type",
			row:  outbox.CanalRow{},
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isSearchOutboxRow(tc.row); got != tc.want {
				t.Fatalf("isSearchOutboxRow() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHandleMessageSkipsNonSearchRows(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(outbox.CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []outbox.CanalRow{
			{
				AggregateType: "following",
				Type:          "FollowCreated",
				Payload:       "not-json",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	consumer := &OutboxConsumer{}
	if err := consumer.handleMessage(context.Background(), payload); err != nil {
		t.Fatalf("expected non-search outbox row to be skipped, got error: %v", err)
	}
}

func TestHandleMessageSkipsEmptyPayloadRows(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(outbox.CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []outbox.CanalRow{
			{
				AggregateType: "knowpost",
				Type:          "KnowPostPublished",
				Payload:       "",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	consumer := &OutboxConsumer{}
	if err := consumer.handleMessage(context.Background(), payload); err != nil {
		t.Fatalf("expected empty payload row to be skipped, got error: %v", err)
	}
}

func TestHandleMessageMarksMalformedKnowPostPayload(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(outbox.CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []outbox.CanalRow{
			{
				AggregateType: "knowpost",
				Type:          "KnowPostPublished",
				Payload:       "not-json",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	consumer := &OutboxConsumer{projector: &KnowPostProjector{}}
	err = consumer.handleMessage(context.Background(), payload)
	if err == nil {
		t.Fatal("expected malformed error")
	}
	if !isMalformedSearchOutboxMessage(err) {
		t.Fatalf("expected malformed search outbox error, got %T", err)
	}
}
