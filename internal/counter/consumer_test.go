package counter

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zhiguang/app/pkg/testutil"
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
	_, ok := nextBatchDeadline(map[int]*counterBatch{}, time.Second)
	if ok {
		t.Fatal("expected ok=false for empty batches")
	}
}

func TestNextBatchDeadline_WithBatch(t *testing.T) {
	b := newCounterBatch(10)
	b.openedAt = time.Now()
	// Add an event so the batch is not considered empty
	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	_ = b.addEvent(makeCounterEventMessage(t, 0, 100, evt), evt)
	batches := map[int]*counterBatch{0: b}

	deadline, ok := nextBatchDeadline(batches, 5*time.Second)
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
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
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
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	_ = NewAggregationConsumer(nil, svc, nil, nil)
}

// ============================================================================
// handleMessage tests (replaces acceptMessage)
// ============================================================================

func TestHandleMessage_ValidEvent(t *testing.T) {
	consumer := &AggregationConsumer{
		batchSize: 10,
		batches:   make(map[int]*counterBatch),
	}

	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	msg := makeCounterEventMessage(t, 0, 1, evt)

	batch := consumer.handleMessage(context.Background(), msg)
	if batch != nil {
		t.Fatal("expected nil batch (not full yet)")
	}

	batch = consumer.batches[0]
	if batch == nil || batch.size() != 1 {
		t.Fatalf("expected batch with 1 event, got %v", batch)
	}
}

func TestHandleMessage_MalformedEvent(t *testing.T) {
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:   svc,
		commitFn:  commit.commit,
		batchSize: 10,
		groupID:   "test-group",
		topic:     "test-topic",
		batches:   make(map[int]*counterBatch),
		logger:    nil,
	}

	msg := makeMalformedMessage(0, 1)
	batch := consumer.handleMessage(context.Background(), msg)
	if batch != nil {
		t.Fatal("handleMessage should return nil for malformed with no existing batch")
	}

	if commit.called.Load() != 1 {
		t.Fatalf("expected 1 commit call for malformed message, got %d", commit.called.Load())
	}
}

func TestHandleMessage_TriggersFlushOnBatchFull(t *testing.T) {
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:          svc,
		commitFn:         commit.commit,
		batchSize:        2,
		flushMaxAttempts: 1,
		flushRetryDelay:  time.Millisecond,
		groupID:          "test-group",
		topic:            "test-topic",
		batches:          make(map[int]*counterBatch),
	}

	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "2", Index: IdxLike, Delta: 1}

	batch1 := consumer.handleMessage(context.Background(), makeCounterEventMessage(t, 0, 1, evt1))
	if batch1 != nil {
		t.Fatal("expected nil for first message")
	}

	batch2 := consumer.handleMessage(context.Background(), makeCounterEventMessage(t, 0, 2, evt2))
	if batch2 == nil {
		t.Fatal("expected non-nil batch on batch full")
	}

	// The batch should have been removed from map
	if consumer.batches[0] != nil {
		t.Fatal("expected batch to be removed from map")
	}
}

func TestHandleMessage_FlushOnPartitionChange(t *testing.T) {
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	commit := &stubCommitFn{}
	batches := make(map[int]*counterBatch)
	consumer := &AggregationConsumer{
		service:          svc,
		commitFn:         commit.commit,
		batchSize:        10,
		flushMaxAttempts: 1,
		flushRetryDelay:  time.Millisecond,
		groupID:          "test-group",
		topic:            "test-topic",
		batches:          batches,
	}

	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "2", Index: IdxLike, Delta: 1}

	_ = consumer.handleMessage(context.Background(), makeCounterEventMessage(t, 0, 1, evt1))
	if _, ok := consumer.batches[0]; !ok {
		t.Fatal("expected partition 0 batch after first message")
	}

	_ = consumer.handleMessage(context.Background(), makeCounterEventMessage(t, 0, 2, evt1))
	if _, ok := consumer.batches[1]; ok {
		t.Fatal("unexpected partition 1 batch")
	}

	// Second message with partition 1 — creates new batch, does NOT flush 0 batch
	// (cross-partition messages don't trigger flush in current design)
	batch := consumer.handleMessage(context.Background(), makeCounterEventMessage(t, 1, 3, evt2))
	if batch != nil {
		t.Fatal("expected nil batch when adding to new partition")
	}
	if _, ok := consumer.batches[0]; !ok {
		t.Fatal("expected partition 0 batch to remain after partition 1 message")
	}
	if _, ok := consumer.batches[1]; !ok {
		t.Fatal("expected partition 1 batch to be created")
	}
}

func TestHandleMessage_FlushOnMalformedWithExistingBatch(t *testing.T) {
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:          svc,
		commitFn:         commit.commit,
		batchSize:        10,
		flushMaxAttempts: 1,
		flushRetryDelay:  time.Millisecond,
		groupID:          "test-group",
		topic:            "test-topic",
		batches:          make(map[int]*counterBatch),
	}

	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	_ = consumer.handleMessage(context.Background(), makeCounterEventMessage(t, 0, 1, evt))

	// Malformed on same partition should return existing batch
	batch := consumer.handleMessage(context.Background(), makeMalformedMessage(0, 2))
	if batch == nil {
		t.Fatal("expected non-nil batch on malformed with existing batch")
	}
}

// ============================================================================
// takeExpiredBatch tests (replaces flushExpiredBatches)
// ============================================================================

func TestTakeExpiredBatch_NoBatches(t *testing.T) {
	consumer := &AggregationConsumer{
		batches: make(map[int]*counterBatch),
	}
	batch := consumer.takeExpiredBatch(context.Background())
	if batch != nil {
		t.Fatal("expected nil for no batches")
	}
}

func TestTakeExpiredBatch_NoneExpired(t *testing.T) {
	consumer := &AggregationConsumer{
		flushInterval: time.Minute,
		batches:       make(map[int]*counterBatch),
	}
	batch := newCounterBatch(10)
	batch.openedAt = time.Now()
	// Add event so size() > 0
	_ = batch.addEvent(makeCounterEventMessage(t, 0, 1, CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}), CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1})
	consumer.batches[0] = batch

	result := consumer.takeExpiredBatch(context.Background())
	if result != nil {
		t.Fatal("expected nil before interval expired")
	}
	if consumer.batches[0] == nil {
		t.Fatal("expected batch to remain in map")
	}
}

func TestTakeExpiredBatch_Expired(t *testing.T) {
	consumer := &AggregationConsumer{
		flushInterval: time.Minute,
		batches:       make(map[int]*counterBatch),
	}
	batch := newCounterBatch(10)
	batch.openedAt = time.Now().Add(-2 * time.Minute)
	consumer.batches[0] = batch

	result := consumer.takeExpiredBatch(context.Background())
	// Empty batch (size()==0) gets cleaned, not returned
	if result != nil {
		t.Fatal("expected nil for expired empty batch (cleaned)")
	}
	if consumer.batches[0] != nil {
		t.Fatal("expected batch removed from map after expiration")
	}
}

func TestTakeExpiredBatch_ClearsEmptyBatches(t *testing.T) {
	consumer := &AggregationConsumer{
		flushInterval: time.Minute,
		batches:       make(map[int]*counterBatch),
	}
	consumer.batches[0] = newCounterBatch(10) // empty
	consumer.batches[1] = &counterBatch{partition: -1, messages: make([]kafka.Message, 0)} // empty

	batch := consumer.takeExpiredBatch(context.Background())
	if batch != nil {
		t.Fatal("expected nil for empty batches")
	}
	if _, exists := consumer.batches[0]; exists {
		t.Fatal("expected empty batch 0 to be cleaned")
	}
	if _, exists := consumer.batches[1]; exists {
		t.Fatal("expected empty batch 1 to be cleaned")
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
	rdb := testutil.StartTestRedis(t)

	svc := NewCounterService(rdb, nil, nil, nil, "", nil, nil)
	commit := &stubCommitFn{}
	consumer := &AggregationConsumer{
		service:   svc,
		commitFn:  commit.commit,
		groupID:   "test-group",
		topic:     "test-topic",
		batches:   make(map[int]*counterBatch),
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

// ============================================================================
// addToBatch tests
// ============================================================================

func TestAddToBatch_NewBatch(t *testing.T) {
	consumer := &AggregationConsumer{
		batchSize: 10,
		batches:   make(map[int]*counterBatch),
	}

	evt := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	msg := makeCounterEventMessage(t, 0, 1, evt)

	consumer.addToBatch(msg, evt)

	batch := consumer.batches[0]
	if batch == nil || batch.size() != 1 {
		t.Fatalf("expected batch with 1 event, got %v", batch)
	}
}

func TestAddToBatch_ExistingBatch(t *testing.T) {
	consumer := &AggregationConsumer{
		batchSize: 10,
		batches:   make(map[int]*counterBatch),
	}

	evt1 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}
	evt2 := CounterEvent{EntityType: "post", EntityID: "1", Index: IdxLike, Delta: 1}

	consumer.addToBatch(makeCounterEventMessage(t, 0, 1, evt1), evt1)
	consumer.addToBatch(makeCounterEventMessage(t, 0, 2, evt2), evt2)

	batch := consumer.batches[0]
	if batch == nil || batch.size() != 2 {
		t.Fatalf("expected batch with 2 events, got %d", batch.size())
	}
}