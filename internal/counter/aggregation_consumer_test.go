package counter

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/zhiguang/app/internal/testutil"
)

func TestFlushRemainingBatchesOnShutdown(t *testing.T) {
	client := testutil.StartRedisServer(t)
	service := NewCounterService(CounterServiceDeps{
		Redis: client,
	})

	committed := 0
	consumer := &AggregationConsumer{
		service:   service,
		commitFn:  func(ctx context.Context, msgs ...kafka.Message) error { committed += len(msgs); return nil },
		groupID:   "counter-agg",
		topic:     "counter-events",
		batchSize: 10,
	}

	batch := newCounterBatch(10)
	if err := batch.addEvent(
		kafka.Message{Partition: 0, Offset: 0},
		CounterEvent{
			EntityType: "knowpost",
			EntityID:   "1001",
			Metric:     "like",
			Index:      IdxLike,
			Delta:      1,
		},
	); err != nil {
		t.Fatalf("add batch event: %v", err)
	}

	batches := map[int]*counterBatch{0: batch}
	consumer.flushRemainingBatches(batches)

	if committed != 1 {
		t.Fatalf("expected one committed message, got %d", committed)
	}
	if len(batches) != 0 {
		t.Fatalf("expected batches to be drained on shutdown, got %d left", len(batches))
	}

	raw, err := client.Get(context.Background(), SdsKey("knowpost", "1001")).Bytes()
	if err != nil {
		t.Fatalf("read flushed cnt snapshot: %v", err)
	}
	if got := readInt32BE(raw, IdxLike*FieldSize); got != 1 {
		t.Fatalf("expected like count 1 after shutdown flush, got %d", got)
	}
}
