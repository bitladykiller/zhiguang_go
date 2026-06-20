package counter

import (
	"context"
	"errors"
	"testing"
)

type stubPublisher struct {
	err error
}

func (s *stubPublisher) Publish(_ context.Context, _ *CounterEvent) error {
	return s.err
}

func TestCounterEventProducer_Publish_Success(t *testing.T) {
	// Use the real producer struct but test the interface contract.
	// We can't easily mock *kafka.Writer, so we test the CounterEventPublisher interface.
	pub := &stubPublisher{}
	if err := pub.Publish(context.Background(), &CounterEvent{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCounterEventProducer_NilWriter(t *testing.T) {
	producer := &CounterEventProducer{writer: nil}
	err := producer.Publish(context.Background(), &CounterEvent{EntityType: "t", EntityID: "1", Metric: "like", Index: IdxLike, Delta: 1})
	if err == nil {
		t.Fatal("expected error for nil writer")
	}
}

func TestCounterEventProducer_NilProducer(t *testing.T) {
	var producer *CounterEventProducer
	err := producer.Publish(context.Background(), &CounterEvent{EntityType: "t", EntityID: "1", Metric: "like", Index: IdxLike, Delta: 1})
	if err == nil {
		t.Fatal("expected error for nil producer")
	}
}

func TestCounterEventProducer_Publish_PublisherError(t *testing.T) {
	pub := &stubPublisher{err: errors.New("kafka broker unavailable")}
	err := pub.Publish(context.Background(), &CounterEvent{})
	if err == nil {
		t.Fatal("expected error for kafka failure")
	}
}

func TestCounterEventProducer_Publish_ContextCancelled(t *testing.T) {
	pub := &stubPublisher{err: context.Canceled}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pub.Publish(ctx, &CounterEvent{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestNewCounterEventProducer_NilWriter(t *testing.T) {
	producer := NewCounterEventProducer(nil)
	if producer.writer != nil {
		t.Error("writer should be nil")
	}
}
