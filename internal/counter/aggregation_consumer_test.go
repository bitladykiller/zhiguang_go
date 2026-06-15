package counter

import (
	"context"
	"reflect"
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

func TestCounterBatchCntKeysSortsMembers(t *testing.T) {
	batch := newCounterBatch(4)

	if err := batch.addEvent(
		kafka.Message{Partition: 1, Offset: 10},
		CounterEvent{
			EntityType: "user",
			EntityID:   "2",
			Metric:     "follower",
			Index:      IdxFollower,
			Delta:      1,
		},
	); err != nil {
		t.Fatalf("add first event: %v", err)
	}

	if err := batch.addEvent(
		kafka.Message{Partition: 1, Offset: 11},
		CounterEvent{
			EntityType: "knowpost",
			EntityID:   "1",
			Metric:     "like",
			Index:      IdxLike,
			Delta:      1,
		},
	); err != nil {
		t.Fatalf("add second event: %v", err)
	}

	keys, epochKeys, indexes := batch.entityKeys()
	wantKeys := []string{
		SdsKey("knowpost", "1"),
		SdsKey("user", "2"),
	}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("entityKeys() keys = %v, want %v", keys, wantKeys)
	}
	wantEpochKeys := []string{
		ActiveEpochKey("knowpost", "1"),
		ActiveEpochKey("user", "2"),
	}
	if !reflect.DeepEqual(epochKeys, wantEpochKeys) {
		t.Fatalf("entityKeys() epoch keys = %v, want %v", epochKeys, wantEpochKeys)
	}

	if got := indexes[CounterEntityMember("knowpost", "1")]; got != 0 {
		t.Fatalf("knowpost index = %d, want 0", got)
	}
	if got := indexes[CounterEntityMember("user", "2")]; got != 1 {
		t.Fatalf("user index = %d, want 1", got)
	}
}

func TestApplyBatchSkipsStaleEpochEvents(t *testing.T) {
	client := testutil.StartRedisServer(t)
	service := NewCounterService(CounterServiceDeps{
		Redis: client,
	})
	consumer := &AggregationConsumer{
		service: service,
		groupID: "counter-agg",
		topic:   "counter-events",
	}

	if err := client.Set(context.Background(), ActiveEpochKey("knowpost", "1001"), 1, 0).Err(); err != nil {
		t.Fatalf("seed active epoch: %v", err)
	}

	batch := newCounterBatch(4)
	if err := batch.addEvent(
		kafka.Message{Partition: 0, Offset: 0},
		CounterEvent{
			EntityType: "knowpost",
			EntityID:   "1001",
			Metric:     "like",
			Epoch:      0,
			Index:      IdxLike,
			Delta:      1,
		},
	); err != nil {
		t.Fatalf("add stale event: %v", err)
	}
	if err := batch.addEvent(
		kafka.Message{Partition: 0, Offset: 1},
		CounterEvent{
			EntityType: "knowpost",
			EntityID:   "1001",
			Metric:     "like",
			Epoch:      1,
			Index:      IdxLike,
			Delta:      1,
		},
	); err != nil {
		t.Fatalf("add current event: %v", err)
	}

	if err := consumer.applyBatch(context.Background(), batch); err != nil {
		t.Fatalf("applyBatch() error = %v", err)
	}

	raw, err := client.Get(context.Background(), SdsKey("knowpost", "1001")).Bytes()
	if err != nil {
		t.Fatalf("read cnt snapshot: %v", err)
	}
	if got := readInt32BE(raw, IdxLike*FieldSize); got != 1 {
		t.Fatalf("like count = %d, want 1", got)
	}

	appliedKey, err := consumer.appliedOffsetKey(0)
	if err != nil {
		t.Fatalf("appliedOffsetKey() error = %v", err)
	}
	appliedOffset, err := client.Get(context.Background(), appliedKey).Int64()
	if err != nil {
		t.Fatalf("read applied offset: %v", err)
	}
	if appliedOffset != 1 {
		t.Fatalf("applied offset = %d, want 1", appliedOffset)
	}
}
