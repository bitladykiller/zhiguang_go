package relation

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
	if !isMalformedRelationOutboxMessage(err) {
		t.Fatalf("expected malformed relation outbox error, got %T", err)
	}
}

func TestIsRelationOutboxRow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  outbox.CanalRow
		want bool
	}{
		{
			name: "follow created event",
			row: outbox.CanalRow{
				AggregateType: "following",
				Type:          "FollowCreated",
			},
			want: true,
		},
		{
			name: "follow canceled event",
			row: outbox.CanalRow{
				AggregateType: "following",
				Type:          "FollowCanceled",
			},
			want: true,
		},
		{
			name: "wrong aggregate type",
			row: outbox.CanalRow{
				AggregateType: "knowpost",
				Type:          "FollowCreated",
			},
			want: false,
		},
		{
			name: "wrong event type",
			row: outbox.CanalRow{
				AggregateType: "following",
				Type:          "KnowPostPublished",
			},
			want: false,
		},
		{
			name: "both wrong",
			row: outbox.CanalRow{
				AggregateType: "knowpost",
				Type:          "KnowPostPublished",
			},
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isRelationOutboxRow(tc.row); got != tc.want {
				t.Fatalf("isRelationOutboxRow() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHandleMessageSkipsNonRelationRows(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(outbox.CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []outbox.CanalRow{
			{
				AggregateType: "knowpost",
				Type:          "KnowPostPublished",
				Payload:       `{"entity":"knowpost","op":"upsert","id":1}`,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	consumer := &OutboxConsumer{}
	if err := consumer.handleMessage(context.Background(), payload); err != nil {
		t.Fatalf("expected non-relation outbox row to be skipped, got error: %v", err)
	}
}

func TestHandleMessageSkipsEmptyPayloadRows(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(outbox.CanalEnvelope{
		Table: "outbox",
		Type:  "INSERT",
		Data: []outbox.CanalRow{
			{
				AggregateType: "following",
				Type:          "FollowCreated",
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
