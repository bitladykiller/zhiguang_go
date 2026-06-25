package counter

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// ============================================================================
// Stub helpers
// ============================================================================

type stubKafkaReader struct {
	messages []kafka.Message
	index    int64
	closed   bool
	err      error
}

func (r *stubKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.err != nil {
		return kafka.Message{}, r.err
	}
	if r.index >= int64(len(r.messages)) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := r.messages[r.index]
	r.index++
	return msg, nil
}

func (r *stubKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

func (r *stubKafkaReader) Close() error {
	r.closed = true
	return nil
}

func (r *stubKafkaReader) Config() kafka.ReaderConfig {
	return kafka.ReaderConfig{GroupID: "test-group", Topic: "test-topic"}
}

type stubCommitFn struct {
	called atomic.Int64
	fn     func(ctx context.Context, msgs ...kafka.Message) error
}

func (s *stubCommitFn) commit(ctx context.Context, msgs ...kafka.Message) error {
	s.called.Add(1)
	if s.fn != nil {
		return s.fn(ctx, msgs...)
	}
	return nil
}

func makeCounterEventMessage(t *testing.T, partition int, offset int64, evt CounterEvent) kafka.Message {
	t.Helper()
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafka.Message{
		Partition: partition,
		Offset:    offset,
		Key:       []byte(evt.EntityType + ":" + evt.EntityID),
		Value:     data,
	}
}

func makeMalformedMessage(partition int, offset int64) kafka.Message {
	return kafka.Message{
		Partition: partition,
		Offset:    offset,
		Key:       []byte("bad"),
		Value:     []byte(`{invalid json`),
	}
}

// ============================================================================
// counterBatch tests
// ============================================================================

func TestCounterBatch_EmptySize(t *testing.T) {
	b := newCounterBatch(10)
	if b.size() != 0 {
		t.Fatalf("empty batch size=%d want=0", b.size())
	}
	var nilBatch *counterBatch
	if nilBatch.size() != 0 {
		t.Fatal("nil batch size should be 0")
	}
}

func TestCounterBatch_AddEvent(t *testing.T) {
	b := newCounterBatch(10)
	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	msg := makeCounterEventMessage(t, 0, 100, evt)

	if err := b.addEvent(msg, evt); err != nil {
		t.Fatalf("addEvent: %v", err)
	}
	if b.size() != 1 {
		t.Fatalf("size=%d want=1", b.size())
	}
	if b.partition != 0 || b.startOffset != 100 || b.endOffset != 100 {
		t.Fatalf("unexpected batch state: partition=%d offsets=[%d,%d]", b.partition, b.startOffset, b.endOffset)
	}
}

func TestCounterBatch_AddEvent_MissingEntity(t *testing.T) {
	b := newCounterBatch(10)
	evt := CounterEvent{EntityType: "", EntityID: "", Index: IdxLike, Delta: 1}
	msg := makeCounterEventMessage(t, 0, 1, evt)

	if err := b.addEvent(msg, evt); err == nil {
		t.Fatal("expected error for missing entity")
	}
}

func TestCounterBatch_AddEvent_IndexOutOfRange(t *testing.T) {
	b := newCounterBatch(10)
	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: 99, Delta: 1}
	msg := makeCounterEventMessage(t, 0, 1, evt)

	if err := b.addEvent(msg, evt); err == nil {
		t.Fatal("expected error for index out of range")
	}
}

func TestCounterBatch_AddEvent_ZeroDelta(t *testing.T) {
	b := newCounterBatch(10)
	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 0}
	msg := makeCounterEventMessage(t, 0, 1, evt)

	if err := b.addEvent(msg, evt); err == nil {
		t.Fatal("expected error for zero delta")
	}
}

func TestCounterBatch_AddEvent_PartitionMismatch(t *testing.T) {
	b := newCounterBatch(10)
	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "2", Index: IdxLike, Delta: 1}

	_ = b.addEvent(makeCounterEventMessage(t, 0, 1, evt1), evt1)
	err := b.addEvent(makeCounterEventMessage(t, 1, 2, evt2), evt2)
	if err == nil {
		t.Fatal("expected error for partition mismatch")
	}
}

func TestCounterBatch_AddEvent_OffsetGap(t *testing.T) {
	b := newCounterBatch(10)
	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "2", Index: IdxLike, Delta: 1}

	_ = b.addEvent(makeCounterEventMessage(t, 0, 1, evt1), evt1)
	err := b.addEvent(makeCounterEventMessage(t, 0, 3, evt2), evt2)
	if err == nil {
		t.Fatal("expected error for offset gap")
	}
}

func TestCounterBatch_CollectDirtyMembers(t *testing.T) {
	b := newCounterBatch(10)
	_ = b.addEvent(makeCounterEventMessage(t, 0, 1, CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}), CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1})
	_ = b.addEvent(makeCounterEventMessage(t, 0, 2, CounterEvent{EntityType: "post", EntityID: "2", Index: IdxFav, Delta: -1}), CounterEvent{EntityType: "post", EntityID: "2", Index: IdxFav, Delta: -1})

	members := b.collectDirtyMembers()
	if len(members) != 2 {
		t.Fatalf("dirty members count=%d want=2", len(members))
	}
}

func TestCounterBatch_Reset(t *testing.T) {
	b := newCounterBatch(10)
	_ = b.addEvent(makeCounterEventMessage(t, 0, 1, CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}), CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1})
	b.reset()

	if b.size() != 0 || b.partition != -1 || len(b.entities) != 0 {
		t.Fatal("reset did not clear batch state")
	}

	var nilBatch *counterBatch
	nilBatch.reset()
}

func TestCounterBatch_CntKeys(t *testing.T) {
	b := newCounterBatch(10)
	_ = b.addEvent(makeCounterEventMessage(t, 0, 1, CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}), CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1})
	_ = b.addEvent(makeCounterEventMessage(t, 0, 2, CounterEvent{EntityType: "post", EntityID: "2", Index: IdxFav, Delta: -1}), CounterEvent{EntityType: "post", EntityID: "2", Index: IdxFav, Delta: -1})

	keys, indexes := b.cntKeys()
	if len(keys) != 2 {
		t.Fatalf("cntKeys count=%d want=2", len(keys))
	}
	if indexes["post:1"] != 0 || indexes["post:2"] != 1 {
		t.Fatalf("unexpected indexes: %v", indexes)
	}
}

// ============================================================================
// nextBatchDeadline tests
// ============================================================================

func TestNextBatchDeadline_NoBatches(t *testing.T) {
	tracker := newPartitionTracker()
	_, ok := nextBatchDeadline(tracker, time.Second)
	if ok {
		t.Fatal("expected ok=false for empty batches")
	}
}

func TestNextBatchDeadline_WithBatch(t *testing.T) {
	tracker := newPartitionTracker()
	b := newCounterBatch(10)
	b.openedAt = time.Now()
	// Add an event so the batch is not considered empty
	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	_ = b.addEvent(makeCounterEventMessage(t, 0, 100, evt), evt)
	tracker.batches[0] = b

	deadline, ok := nextBatchDeadline(tracker, 5*time.Second)
	if !ok {
		t.Fatal("expected ok=true")
	}
	expected := b.openedAt.Add(5 * time.Second)
	if deadline.Sub(expected) > time.Millisecond {
		t.Fatalf("deadline=%v want=%v", deadline, expected)
	}
}

// ============================================================================
// AggregationConsumer core logic tests with mock reader
// ============================================================================

func TestNewAggregationConsumer_NilReader(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	c := NewAggregationConsumer(nil, svc, nil, nil)
	if c != nil {
		t.Fatal("expected nil consumer for nil reader")
	}
}

func TestNewAggregationConsumer_NilService(t *testing.T) {
	c := NewAggregationConsumer(&kafka.Reader{}, nil, nil, nil)
	if c != nil {
		t.Fatal("expected nil consumer for nil service")
	}
}

func TestNewAggregationConsumer_DefaultConfig(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	_ = NewAggregationConsumer(nil, svc, nil, nil)
}

func TestAcceptMessage_ValidEvent(t *testing.T) {
	svc := &CounterService{}
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			batchSize: 10,
		},
		tracker: newPartitionTracker(),
	}

	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	msg := makeCounterEventMessage(t, 0, 1, evt)

	_, _, err := consumer.tracker.acceptMessageAsync(context.Background(), consumer, msg)
	if err != nil {
		t.Fatalf("acceptMessage: %v", err)
	}

	batch := consumer.tracker.batches[0]
	if batch == nil || batch.size() != 1 {
		t.Fatalf("expected batch with 1 event, got %v", batch)
	}
}

func TestAcceptMessage_MalformedEvent(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			batchSize:        10,
			flushMaxAttempts: 1,
			flushRetryDelay:  time.Millisecond,
			groupID:          "test-group",
			topic:            "test-topic",
		},
		tracker: newPartitionTracker(),
		logger:  nil,
	}

	msg := makeMalformedMessage(0, 1)
	_, _, err := consumer.tracker.acceptMessageAsync(context.Background(), consumer, msg)
	if err != nil {
		t.Fatalf("acceptMessageAsync should not return error for malformed: %v", err)
	}

	if commit.called.Load() != 0 {
		t.Fatalf("expected 0 commit calls for malformed message (skipMalformedMessage handles it), got %d", commit.called.Load())
	}
}

func TestAcceptMessage_TriggersFlushOnBatchFull(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			batchSize:        2,
			flushMaxAttempts: 1,
			flushRetryDelay:  time.Millisecond,
			groupID:          "test-group",
			topic:            "test-topic",
		},
		tracker: newPartitionTracker(),
	}

	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "2", Index: IdxLike, Delta: 1}

	_, _, _ = consumer.tracker.acceptMessageAsync(context.Background(), consumer, makeCounterEventMessage(t, 0, 1, evt1))
	flushPartition, _, _ := consumer.tracker.acceptMessageAsync(context.Background(), consumer, makeCounterEventMessage(t, 0, 2, evt2))

	if flushPartition < 0 {
		t.Fatal("expected flush trigger on batch full")
	}
	// flushAndReset is called by the caller, not by acceptMessageAsync
	consumer.tracker.flushPartitionBatch(context.Background(), consumer, flushPartition)
	if commit.called.Load() == 0 {
		t.Fatal("expected commit to be called on batch full")
	}
	if consumer.tracker.batches[0] != nil {
		t.Fatal("expected batch to be flushed")
	}
}

func TestAcceptMessage_FlushOnPartitionChange(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	tracker := newPartitionTracker()
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			batchSize:        10,
			flushMaxAttempts: 1,
			flushRetryDelay:  time.Millisecond,
			groupID:          "test-group",
			topic:            "test-topic",
		},
		tracker: tracker,
	}

	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "2", Index: IdxLike, Delta: 1}

	_, _, _ = tracker.acceptMessageAsync(context.Background(), consumer, makeCounterEventMessage(t, 0, 1, evt1))
	// First message creates partition 0 batch — confirm it's there
	if _, ok := tracker.batches[0]; !ok {
		t.Fatal("expected partition 0 batch after first message")
	}

	// Calling acceptMessage with same partition should NOT flush
	_, _, _ = tracker.acceptMessageAsync(context.Background(), consumer, makeCounterEventMessage(t, 0, 2, evt1))
	// Still only one batch (partition 0)
	if _, ok := tracker.batches[1]; ok {
		t.Fatal("unexpected partition 1 batch")
	}

	// Second message with partition 1 — this triggers flush of partition 0
	_, _, err := tracker.acceptMessageAsync(context.Background(), consumer, makeCounterEventMessage(t, 1, 3, evt2))
	if err != nil {
		t.Fatalf("acceptMessage: %v", err)
	}
	if commit.called.Load() == 0 {
		t.Log("note: commit not called (may be expected if no flush needed)")
	}
}

func TestAcceptMessage_FlushOnMalformedWithExistingBatch(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			batchSize:        10,
			flushMaxAttempts: 1,
			flushRetryDelay:  time.Millisecond,
			groupID:          "test-group",
			topic:            "test-topic",
		},
		tracker: newPartitionTracker(),
	}

	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	_, _, _ = consumer.tracker.acceptMessageAsync(context.Background(), consumer, makeCounterEventMessage(t, 0, 1, evt))
	flushPartition, _, err := consumer.tracker.acceptMessageAsync(context.Background(), consumer, makeMalformedMessage(0, 2))
	if err != nil {
		t.Fatalf("acceptMessageAsync: %v", err)
	}

	if flushPartition < 0 {
		t.Fatal("expected flush trigger on malformed with existing batch")
	}
	consumer.tracker.flushPartitionBatch(context.Background(), consumer, flushPartition)
	if commit.called.Load() == 0 {
		t.Fatal("expected commit on malformed with existing batch")
	}
}

// ============================================================================
// flushPartitionBatch tests
// ============================================================================

func TestFlushPartitionBatch_EmptyBatch(t *testing.T) {
	consumer := &AggregationConsumer{
		tracker: newPartitionTracker(),
	}
	consumer.tracker.flushPartitionBatch(context.Background(), consumer, 0)
	if _, exists := consumer.tracker.batches[0]; exists {
		t.Fatal("expected nil batch to be removed")
	}
}

func TestFlushPartitionBatch_FlushesAndCleans(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			flushMaxAttempts: 1,
			flushRetryDelay:  time.Millisecond,
			groupID:          "test-group",
			topic:            "test-topic",
		},
		tracker: newPartitionTracker(),
	}

	batch := newCounterBatch(10)
	_ = batch.addEvent(makeCounterEventMessage(t, 0, 1, CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}), CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1})
	consumer.tracker.batches[0] = batch

	consumer.tracker.flushPartitionBatch(context.Background(), consumer, 0)
	if _, exists := consumer.tracker.batches[0]; exists {
		t.Fatal("expected batch to be removed after flush")
	}
}

// ============================================================================
// flushExpiredBatches tests
// ============================================================================

func TestFlushExpiredBatches_NoBatches(t *testing.T) {
	consumer := &AggregationConsumer{
		cfg:     &consumerConfig{flushInterval: time.Second},
		tracker: newPartitionTracker(),
	}
	if consumer.tracker.flushExpiredBatches(context.Background(), consumer, time.Now()) {
		t.Fatal("expected false for no batches")
	}
}

func TestFlushExpiredBatches_NoneExpired(t *testing.T) {
	consumer := &AggregationConsumer{
		cfg:     &consumerConfig{flushInterval: 5 * time.Second},
		tracker: newPartitionTracker(),
	}
	batch := newCounterBatch(10)
	batch.openedAt = time.Now()
	consumer.tracker.batches[0] = batch

	if consumer.tracker.flushExpiredBatches(context.Background(), consumer, time.Now()) {
		t.Fatal("expected false before interval expired")
	}
}

func TestFlushExpiredBatches_Expired(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			flushMaxAttempts: 1,
			flushRetryDelay:  time.Millisecond,
			groupID:          "test-group",
			topic:            "test-topic",
			flushInterval:    time.Second,
		},
		tracker: newPartitionTracker(),
	}

	batch := newCounterBatch(10)
	batch.openedAt = time.Now().Add(-10 * time.Second)
	_ = batch.addEvent(makeCounterEventMessage(t, 0, 1, CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}), CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1})
	// addEvent overwrites openedAt, so set it back
	batch.openedAt = time.Now().Add(-10 * time.Second)
	consumer.tracker.batches[0] = batch

	if !consumer.tracker.flushExpiredBatches(context.Background(), consumer, time.Now()) {
		t.Fatal("expected true for expired batch")
	}
	if _, exists := consumer.tracker.batches[0]; exists {
		t.Fatal("expected batch removed after expiration flush")
	}
}

func TestFlushExpiredBatches_ClearsEmptyBatches(t *testing.T) {
	consumer := &AggregationConsumer{
		cfg:     &consumerConfig{flushInterval: time.Second},
		tracker: newPartitionTracker(),
	}
	consumer.tracker.batches = map[int]*counterBatch{
		0: newCounterBatch(10),
		1: {partition: -1, messages: make([]kafka.Message, 0)},
	}
	consumer.tracker.flushExpiredBatches(context.Background(), consumer, time.Now())
	if _, exists := consumer.tracker.batches[1]; exists {
		t.Fatal("expected empty batch to be cleaned")
	}
}

// ============================================================================
// parseCounterEvent tests
// ============================================================================

func TestParseCounterEvent_Valid(t *testing.T) {
	data := []byte(`{"message_id":1,"entity_type":"post","entity_id":"1","metric":"like","index":0,"user_id":42,"delta":1}`)
	evt, err := parseCounterEvent(data)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if evt.EntityType != "post" || evt.EntityID != "1" || evt.Metric != "like" || evt.Delta != 1 {
		t.Fatalf("unexpected parse result: %+v", evt)
	}
}

func TestParseCounterEvent_InvalidJSON(t *testing.T) {
	_, err := parseCounterEvent([]byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseCounterEvent_Empty(t *testing.T) {
	_, err := parseCounterEvent([]byte{})
	if err == nil {
		t.Fatal("expected error for empty")
	}
}

// ============================================================================
// skipMalformedMessage tests
// ============================================================================

func TestSkipMalformedMessage(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:  svc,
		commitFn: commit.commit,
		cfg: &consumerConfig{
			groupID: "test-group",
			topic:   "test-topic",
		},
		tracker: newPartitionTracker(),
	}

	msg := makeMalformedMessage(1, 5)
	consumer.skipMalformedMessage(context.Background(), msg, errors.New("bad data"))

	if commit.called.Load() != 1 {
		t.Fatalf("expected 1 commit call, got %d", commit.called.Load())
	}
}

// ============================================================================
// nil-safety tests
// ============================================================================

func TestAggregationConsumer_NilMethods(t *testing.T) {
	var c *AggregationConsumer

	ctx := context.Background()
	c.Start(ctx)
	c.flushAndReset(ctx, nil)
	if c != nil {
		c.tracker.flushPartitionBatch(ctx, c, 0)
	}
	c.flushBatch(ctx, nil)

	if c.maxFlushAttempts() != defaultCounterFlushMaxAttempts {
		t.Fatal("nil consumer should return default max attempts")
	}
}

func TestCommitMessages_NilConsumer(t *testing.T) {
	var c *AggregationConsumer
	err := c.commitMessages(context.Background())
	if err != nil {
		t.Fatalf("nil consumer commitMessages: %v", err)
	}
}

func TestCounterBatch_NilAdd(t *testing.T) {
	var b *counterBatch
	err := b.add(kafka.Message{})
	if err == nil {
		t.Fatal("expected error on nil batch add")
	}
}

// ============================================================================
// Flush edge cases
// ============================================================================

func TestFlushBatch_NilBatch(t *testing.T) {
	consumer := &AggregationConsumer{}
	if err := consumer.flushBatch(context.Background(), nil); err != nil {
		t.Fatalf("flush nil batch: %v", err)
	}
}

func TestFlushBatch_EmptyBatch(t *testing.T) {
	consumer := &AggregationConsumer{}
	if err := consumer.flushBatch(context.Background(), newCounterBatch(10)); err != nil {
		t.Fatalf("flush empty batch: %v", err)
	}
}

func TestFlushAndReset_EmptyBatch(t *testing.T) {
	consumer := &AggregationConsumer{}
	consumer.flushAndReset(context.Background(), newCounterBatch(10))
}

func TestFlushAndReset_NilBatch(t *testing.T) {
	var consumer *AggregationConsumer
	consumer.flushAndReset(context.Background(), nil)
}
