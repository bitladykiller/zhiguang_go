package counter

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func TestTogglePublishFailureMarksDirty(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	recorder := &stubCounterFailureRecorder{}
	svc := NewCounterService(rdb, &stubCounterPublisher{err: errors.New("kafka down")}, nil, recorder, "counter-events", nil)
	ctx := context.Background()
	entityType := "knowpost"
	entityID := "42"

	changed, err := svc.Like(ctx, 1001, entityType, entityID)
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if !changed {
		t.Fatalf("expected toggle to change bitmap state")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		ok, err := rdb.SIsMember(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Result()
		return err == nil && ok
	})
	waitForCondition(t, 2*time.Second, func() bool {
		return recorder.totalRecords() == 1
	})

	record := recorder.singleRecord(t)
	if record.Stage != counterFailureStagePublish {
		t.Fatalf("unexpected publish failure stage: got=%s want=%s", record.Stage, counterFailureStagePublish)
	}
	if record.Topic != "counter-events" {
		t.Fatalf("unexpected publish failure topic: got=%s want=counter-events", record.Topic)
	}
	if record.EntityType != entityType || record.EntityID != entityID || record.Metric != "like" || record.Delta != 1 {
		t.Fatalf("unexpected publish failure record: %+v", record)
	}
}

func TestTogglePublishesSnowflakeMessageID(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	publisher := &stubCapturingCounterPublisher{published: make(chan *CounterEvent, 1)}
	svc := NewCounterService(rdb, publisher, nil, nil, "", stubMessageIDGenerator{next: 987654321})

	changed, err := svc.Like(context.Background(), 1001, "knowpost", "66")
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if !changed {
		t.Fatalf("expected toggle to change bitmap state")
	}

	select {
	case event := <-publisher.published:
		if event.MessageID != 987654321 {
			t.Fatalf("unexpected message id: got=%d want=987654321", event.MessageID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for published event")
	}
}

func TestApplyBatchWritesDeltaIntoSds(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "99"
	cntKey := SdsKey(entityType, entityID)

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 5)
	writeInt32BE(raw, IdxFav*FieldSize, 1)
	if err := rdb.Set(ctx, cntKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed sds: %v", err)
	}

	svc := NewCounterService(rdb, nil, nil, nil, "", nil)
	consumer := &AggregationConsumer{service: svc, groupID: "counter-group", topic: "counter-events"}
	batch := newCounterBatch(2)
	if err := batch.add(mustCounterMessageAt(t, 3, 10, CounterEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Metric:     "like",
		Index:      IdxLike,
		Delta:      2,
	})); err != nil {
		t.Fatalf("add like event: %v", err)
	}
	if err := batch.add(mustCounterMessageAt(t, 3, 11, CounterEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Metric:     "fav",
		Index:      IdxFav,
		Delta:      -1,
	})); err != nil {
		t.Fatalf("add fav event: %v", err)
	}
	if err := consumer.applyBatch(ctx, batch); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	gotRaw, err := rdb.Get(ctx, cntKey).Bytes()
	if err != nil {
		t.Fatalf("get sds after apply: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 7 {
		t.Fatalf("unexpected like count after apply: got=%d want=7", got)
	}
	if got := readInt32BE(gotRaw, IdxFav*FieldSize); got != 0 {
		t.Fatalf("unexpected fav count after apply: got=%d want=0", got)
	}

	offset, err := rdb.Get(ctx, AppliedOffsetKey("counter-group", "counter-events", 3)).Int64()
	if err != nil {
		t.Fatalf("get applied offset after apply: %v", err)
	}
	if offset != 11 {
		t.Fatalf("unexpected applied offset after apply: got=%d want=11", offset)
	}
}

func TestApplyBatchSkipsAlreadyAppliedPrefix(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "101"
	cntKey := SdsKey(entityType, entityID)
	offsetKey := AppliedOffsetKey("counter-group", "counter-events", 2)

	if err := rdb.Set(ctx, offsetKey, 10, 0).Err(); err != nil {
		t.Fatalf("seed applied offset: %v", err)
	}

	svc := NewCounterService(rdb, nil, nil, nil, "", nil)
	consumer := &AggregationConsumer{service: svc, groupID: "counter-group", topic: "counter-events"}
	batch := newCounterBatch(4)
	for offset := int64(9); offset <= 12; offset++ {
		if err := batch.add(mustCounterMessageAt(t, 2, offset, CounterEvent{
			EntityType: entityType,
			EntityID:   entityID,
			Metric:     "like",
			Index:      IdxLike,
			Delta:      1,
		})); err != nil {
			t.Fatalf("add event offset=%d: %v", offset, err)
		}
	}

	if err := consumer.applyBatch(ctx, batch); err != nil {
		t.Fatalf("apply batch with replayed prefix: %v", err)
	}

	gotRaw, err := rdb.Get(ctx, cntKey).Bytes()
	if err != nil {
		t.Fatalf("get sds after replayed prefix: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 2 {
		t.Fatalf("unexpected like count after replayed prefix: got=%d want=2", got)
	}

	offset, err := rdb.Get(ctx, offsetKey).Int64()
	if err != nil {
		t.Fatalf("get applied offset after replayed prefix: %v", err)
	}
	if offset != 12 {
		t.Fatalf("unexpected applied offset after replayed prefix: got=%d want=12", offset)
	}
}

func TestRepairDirtyMemberOverwritesSnapshotFromBitmap(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "77"

	svc := NewCounterService(rdb, nil, nil, nil, "", nil)
	if _, err := svc.Like(ctx, 1001, entityType, entityID); err != nil {
		t.Fatalf("like first user: %v", err)
	}
	if _, err := svc.Like(ctx, 1002, entityType, entityID); err != nil {
		t.Fatalf("like second user: %v", err)
	}

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 9)
	if err := rdb.Set(ctx, SdsKey(entityType, entityID), raw, 0).Err(); err != nil {
		t.Fatalf("seed wrong sds: %v", err)
	}
	if err := svc.markDirty(ctx, entityType, entityID); err != nil {
		t.Fatalf("mark dirty: %v", err)
	}

	consumer := &AggregationConsumer{service: svc}
	if err := consumer.repairDirtyMember(ctx, DirtyMember(entityType, entityID)); err != nil {
		t.Fatalf("repair dirty member: %v", err)
	}

	gotRaw, err := rdb.Get(ctx, SdsKey(entityType, entityID)).Bytes()
	if err != nil {
		t.Fatalf("get repaired sds: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 2 {
		t.Fatalf("unexpected like count after repair: got=%d want=2", got)
	}

	ok, err := rdb.SIsMember(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Result()
	if err != nil {
		t.Fatalf("check dirty set: %v", err)
	}
	if ok {
		t.Fatalf("expected dirty marker to be removed after repair")
	}
}

func TestFlushBatchRetriesCommitWithoutReapplyingDelta(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "108"
	cntKey := SdsKey(entityType, entityID)

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 5)
	if err := rdb.Set(ctx, cntKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed sds: %v", err)
	}

	recorder := &stubCounterFailureRecorder{}
	svc := NewCounterService(rdb, nil, nil, recorder, "counter-events", nil)
	commitCalls := 0
	consumer := &AggregationConsumer{
		service:          svc,
		groupID:          "counter-group",
		topic:            "counter-events",
		flushMaxAttempts: 3,
		flushRetryDelay:  time.Millisecond,
		commitFn: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCalls++
			if commitCalls < 3 {
				return errors.New("commit failed")
			}
			return nil
		},
	}

	batch := newCounterBatch(1)
	if err := batch.add(mustCounterMessageAt(t, 1, 20, CounterEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Metric:     "like",
		Index:      IdxLike,
		Delta:      2,
	})); err != nil {
		t.Fatalf("add like event: %v", err)
	}

	consumer.flushAndReset(ctx, batch)

	if commitCalls != 3 {
		t.Fatalf("unexpected commit attempts: got=%d want=3", commitCalls)
	}
	if batch.size() != 0 {
		t.Fatalf("expected batch to be reset after flush attempts")
	}

	gotRaw, err := rdb.Get(ctx, cntKey).Bytes()
	if err != nil {
		t.Fatalf("get sds after retries: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 7 {
		t.Fatalf("unexpected like count after retries: got=%d want=7", got)
	}

	offset, err := rdb.Get(ctx, AppliedOffsetKey("counter-group", "counter-events", 1)).Int64()
	if err != nil {
		t.Fatalf("get applied offset after retries: %v", err)
	}
	if offset != 20 {
		t.Fatalf("unexpected applied offset after retries: got=%d want=20", offset)
	}
	if recorder.totalRecords() != 0 {
		t.Fatalf("did not expect failed records after successful retry, got=%d", recorder.totalRecords())
	}
}

func TestFlushBatchExhaustedRetriesStoresFailedMessages(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	entityType := "knowpost"
	entityID := "109"
	cntKey := SdsKey(entityType, entityID)

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 5)
	if err := rdb.Set(ctx, cntKey, raw, 0).Err(); err != nil {
		t.Fatalf("seed sds: %v", err)
	}

	recorder := &stubCounterFailureRecorder{}
	svc := NewCounterService(rdb, nil, nil, recorder, "counter-events", nil)

	commitCalls := 0
	consumer := &AggregationConsumer{
		service:          svc,
		groupID:          "counter-group",
		topic:            "counter-events",
		flushMaxAttempts: 3,
		flushRetryDelay:  time.Millisecond,
		commitFn: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCalls++
			return errors.New("commit failed")
		},
	}

	batch := newCounterBatch(1)
	if err := batch.add(mustCounterMessageAt(t, 4, 30, CounterEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Metric:     "like",
		Index:      IdxLike,
		Delta:      2,
	})); err != nil {
		t.Fatalf("add like event: %v", err)
	}

	consumer.flushAndReset(ctx, batch)

	if commitCalls != 3 {
		t.Fatalf("unexpected commit attempts: got=%d want=3", commitCalls)
	}

	gotRaw, err := rdb.Get(ctx, cntKey).Bytes()
	if err != nil {
		t.Fatalf("get sds after failed retries: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 7 {
		t.Fatalf("unexpected like count after failed retries: got=%d want=7", got)
	}

	offset, err := rdb.Get(ctx, AppliedOffsetKey("counter-group", "counter-events", 4)).Int64()
	if err != nil {
		t.Fatalf("get applied offset after failed retries: %v", err)
	}
	if offset != 30 {
		t.Fatalf("unexpected applied offset after failed retries: got=%d want=30", offset)
	}

	if recorder.totalRecords() != 1 {
		t.Fatalf("expected one failed record after exhausted retries, got=%d", recorder.totalRecords())
	}
	record := recorder.singleRecord(t)
	if record.Stage != counterFailureStageFlush {
		t.Fatalf("unexpected flush failure stage: got=%s want=%s", record.Stage, counterFailureStageFlush)
	}
	if record.Topic != "counter-events" {
		t.Fatalf("unexpected flush failure topic: got=%s want=counter-events", record.Topic)
	}
	if record.EntityType != entityType || record.EntityID != entityID || record.Metric != "like" || record.Delta != 2 {
		t.Fatalf("unexpected flush failure record: %+v", record)
	}

	ok, err := rdb.SIsMember(ctx, DirtySetKey(), DirtyMember(entityType, entityID)).Result()
	if err != nil {
		t.Fatalf("check dirty set after exhausted retries: %v", err)
	}
	if !ok {
		t.Fatalf("expected dirty marker after exhausted retries")
	}
}

type stubCounterPublisher struct {
	err error
}

func (p *stubCounterPublisher) Publish(ctx context.Context, event *CounterEvent) error {
	return p.err
}

type stubCapturingCounterPublisher struct {
	published chan *CounterEvent
}

func (p *stubCapturingCounterPublisher) Publish(ctx context.Context, event *CounterEvent) error {
	if p != nil && p.published != nil {
		p.published <- cloneCounterEvent(event)
	}
	return nil
}

type stubMessageIDGenerator struct {
	next uint64
}

func (g stubMessageIDGenerator) NextID() uint64 {
	return g.next
}

type stubCounterFailureRecorder struct {
	mu      sync.Mutex
	records []*CounterFailedMessage
}

func (r *stubCounterFailureRecorder) Create(ctx context.Context, message *CounterFailedMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, cloneCounterFailedMessage(message))
	return nil
}

func (r *stubCounterFailureRecorder) CreateBatch(ctx context.Context, messages []*CounterFailedMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, message := range messages {
		r.records = append(r.records, cloneCounterFailedMessage(message))
	}
	return nil
}

func (r *stubCounterFailureRecorder) totalRecords() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

func (r *stubCounterFailureRecorder) singleRecord(t *testing.T) *CounterFailedMessage {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.records) != 1 {
		t.Fatalf("expected exactly one failed record, got=%d", len(r.records))
	}
	return cloneCounterFailedMessage(r.records[0])
}

func cloneCounterFailedMessage(message *CounterFailedMessage) *CounterFailedMessage {
	if message == nil {
		return nil
	}
	cloned := *message
	return &cloned
}

func cloneCounterEvent(event *CounterEvent) *CounterEvent {
	if event == nil {
		return nil
	}
	cloned := *event
	return &cloned
}

func mustCounterMessageAt(t *testing.T, partition int, offset int64, event CounterEvent) kafka.Message {
	t.Helper()

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return kafka.Message{
		Partition: partition,
		Offset:    offset,
		Key:       []byte(event.EntityType + ":" + event.EntityID),
		Value:     data,
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}

func startTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	return client, func() {
		client.Close()
		mr.Close()
	}
}
