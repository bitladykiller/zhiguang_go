package messaging

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/internal/outbox"
	"github.com/zhiguang/app/pkg/config"
)

func TestNewKafkaWriterUsesMoreReliableCounterSettings(t *testing.T) {
	t.Helper()

	writer := NewKafkaWriter(&config.KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topics: config.KafkaTopicsConfig{
			CounterEvents: "counter-events",
		},
	})
	defer writer.Close()

	if writer.Async {
		t.Fatalf("expected counter writer to wait for broker acknowledgements")
	}
	if writer.RequiredAcks != kafka.RequireAll {
		t.Fatalf("unexpected counter writer acks: got=%d want=%d", writer.RequiredAcks, kafka.RequireAll)
	}
	if writer.MaxAttempts != counterWriterMaxAttempts {
		t.Fatalf("unexpected counter writer max attempts: got=%d want=%d", writer.MaxAttempts, counterWriterMaxAttempts)
	}
}

func TestNewTopicWriterKeepsOutboxHighReliabilitySettings(t *testing.T) {
	t.Helper()

	writer := NewTopicWriter(&config.KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
	}, outbox.CanalOutboxTopic, false)
	defer writer.Close()

	if writer.Async {
		t.Fatalf("expected outbox writer to remain synchronous")
	}
	if writer.RequiredAcks != kafka.RequireAll {
		t.Fatalf("unexpected outbox writer acks: got=%d want=%d", writer.RequiredAcks, kafka.RequireAll)
	}
	if writer.MaxAttempts != outboxWriterMaxAttempts {
		t.Fatalf("unexpected outbox writer max attempts: got=%d want=%d", writer.MaxAttempts, outboxWriterMaxAttempts)
	}
}
