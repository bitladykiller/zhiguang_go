package counter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func TestTogglePublishFailureStoresFailureTask(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	svc := NewCounterService(rdb, &stubCounterPublisher{err: errors.New("kafka down")}, nil)
	recorder := &stubCounterFailureRecorder{}
	svc.SetFailureRecorder(recorder, "counter-events")
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
	if record.NextRetryAt.IsZero() {
		t.Fatalf("expected publish failure task next retry time to be set")
	}
}

func TestTogglePublishesSnowflakeMessageID(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	publisher := &stubCapturingCounterPublisher{published: make(chan *CounterEvent, 1)}
	svc := NewCounterService(rdb, publisher, nil)
	svc.SetMessageIDGenerator(stubMessageIDGenerator{next: 987654321})

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

	svc := NewCounterService(rdb, nil, nil)
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

	svc := NewCounterService(rdb, nil, nil)
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

	svc := NewCounterService(rdb, nil, nil)
	recorder := &stubCounterFailureRecorder{}
	svc.SetFailureRecorder(recorder, "counter-events")
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

func TestFlushBatchExhaustedCommitRetriesDoesNotStoreFailedMessages(t *testing.T) {
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

	svc := NewCounterService(rdb, nil, nil)
	recorder := &stubCounterFailureRecorder{}
	svc.SetFailureRecorder(recorder, "counter-events")

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

	if recorder.totalRecords() != 0 {
		t.Fatalf("did not expect failed records after exhausted commit retries, got=%d", recorder.totalRecords())
	}
}

func TestFlushBatchExhaustedApplyRetriesStoresFailureTask(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	svc := NewCounterService(rdb, nil, nil)
	recorder := &stubCounterFailureRecorder{}
	svc.SetFailureRecorder(recorder, "counter-events")

	consumer := &AggregationConsumer{
		service:          svc,
		groupID:          "",
		topic:            "",
		flushMaxAttempts: 3,
		flushRetryDelay:  time.Millisecond,
	}

	batch := newCounterBatch(1)
	if err := batch.add(mustCounterMessageAt(t, 5, 41, CounterEvent{
		EntityType: "knowpost",
		EntityID:   "110",
		Metric:     "like",
		Index:      IdxLike,
		Delta:      1,
	})); err != nil {
		t.Fatalf("add like event: %v", err)
	}

	consumer.flushAndReset(ctx, batch)

	if recorder.totalRecords() != 1 {
		t.Fatalf("expected one apply failure task after exhausted retries, got=%d", recorder.totalRecords())
	}

	record := recorder.singleRecord(t)
	if record.Stage != counterFailureStageApply {
		t.Fatalf("unexpected apply failure stage: got=%s want=%s", record.Stage, counterFailureStageApply)
	}
	if record.Topic != "counter-events" {
		t.Fatalf("unexpected apply failure topic: got=%s want=counter-events", record.Topic)
	}
	if record.EntityType != "knowpost" || record.EntityID != "110" || record.Metric != "like" || record.Delta != 1 {
		t.Fatalf("unexpected apply failure record: %+v", record)
	}
	if record.NextRetryAt.IsZero() {
		t.Fatalf("expected apply failure task next retry time to be set")
	}
}

func TestRecordFailedKafkaMessagesDeduplicatesApplyTasksByEntityMetric(t *testing.T) {
	t.Helper()

	recorder := &stubCounterFailureRecorder{}
	svc := &CounterService{failureRecorder: recorder, failureTopic: "counter-events"}

	messages := []kafka.Message{
		mustCounterMessageAt(t, 6, 51, CounterEvent{
			EntityType: "knowpost",
			EntityID:   "111",
			Metric:     "like",
			Index:      IdxLike,
			Delta:      1,
		}),
		mustCounterMessageAt(t, 6, 52, CounterEvent{
			EntityType: "knowpost",
			EntityID:   "111",
			Metric:     "like",
			Index:      IdxLike,
			Delta:      1,
		}),
	}

	if err := svc.recordFailedKafkaMessages(context.Background(), counterFailureStageApply, messages, errors.New("apply failed")); err != nil {
		t.Fatalf("record failed kafka messages: %v", err)
	}

	if recorder.totalRecords() != 1 {
		t.Fatalf("expected apply repair task to be deduplicated, got=%d", recorder.totalRecords())
	}

	record := recorder.singleRecord(t)
	if record.Delta != 2 {
		t.Fatalf("unexpected aggregated delta: got=%d want=2", record.Delta)
	}
}

type stubCounterPublisher struct {
	err error
}

func (p *stubCounterPublisher) Publish(event *CounterEvent) error {
	return p.err
}

type stubCapturingCounterPublisher struct {
	published chan *CounterEvent
}

func (p *stubCapturingCounterPublisher) Publish(event *CounterEvent) error {
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

	port := reservePort(t)
	dataDir := t.TempDir()
	cmd := exec.Command(
		"redis-server",
		"--save", "",
		"--appendonly", "no",
		"--bind", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--dir", dataDir,
	)

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}

	addr := "127.0.0.1:" + strconv.Itoa(port)
	client := redis.NewClient(&redis.Options{Addr: addr})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := client.Ping(context.Background()).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			_ = client.Close()
			t.Fatalf("redis-server did not become ready: %s", output.String())
		}
		time.Sleep(50 * time.Millisecond)
	}

	return client, func() {
		_ = client.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
}

func reservePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp port: %v", err)
	}
	defer ln.Close()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected addr type: %T", ln.Addr())
	}
	return addr.Port
}
