package counter

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/zhiguang/app/pkg/config"
)

func TestCounterFailureWorkerRepublishEventMarksTaskDone(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	publisher := &stubCapturingCounterPublisher{published: make(chan *CounterEvent, 1)}
	svc := NewCounterService(rdb, publisher, nil)
	store := &stubCounterFailureTaskStore{
		tasks: []*CounterFailedMessage{
			newPublishFailureTask(t, 1, CounterEvent{
				MessageID:  123,
				EntityType: "knowpost",
				EntityID:   "201",
				Metric:     "like",
				Index:      IdxLike,
				UserID:     1001,
				Delta:      1,
			}),
		},
	}

	worker := NewCounterFailureWorker(store, svc, nil, &config.CounterConfig{
		Repair: config.RepairConfig{
			Enabled:            true,
			BatchSize:          10,
			IntervalMs:         1000,
			CleanupIntervalMs:  3600000,
			CleanupBatchSize:   500,
			DoneRetentionHours: 168,
		},
	})
	if worker == nil {
		t.Fatalf("expected failure worker to be created")
	}

	worker.processOnce(context.Background())

	if len(store.doneIDs()) != 1 || store.doneIDs()[0] != 1 {
		t.Fatalf("expected publish failure task to be marked done, got=%v", store.doneIDs())
	}
	if len(store.retryCalls()) != 0 {
		t.Fatalf("did not expect publish failure task retry, got=%d", len(store.retryCalls()))
	}

	select {
	case event := <-publisher.published:
		if event.MessageID != 123 || event.EntityType != "knowpost" || event.EntityID != "201" || event.Metric != "like" || event.Delta != 1 {
			t.Fatalf("unexpected republished event: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for republished event")
	}
}

func TestCounterFailureWorkerRepairMetricMarksTaskDoneAndOverwritesSnapshot(t *testing.T) {
	t.Helper()

	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	svc := NewCounterService(rdb, nil, nil)
	if _, err := svc.Like(ctx, 1001, "knowpost", "202"); err != nil {
		t.Fatalf("like first user: %v", err)
	}
	if _, err := svc.Like(ctx, 1002, "knowpost", "202"); err != nil {
		t.Fatalf("like second user: %v", err)
	}

	raw := make([]byte, SchemaLen*FieldSize)
	writeInt32BE(raw, IdxLike*FieldSize, 9)
	if err := rdb.Set(ctx, SdsKey("knowpost", "202"), raw, 0).Err(); err != nil {
		t.Fatalf("seed wrong sds: %v", err)
	}

	store := &stubCounterFailureTaskStore{
		tasks: []*CounterFailedMessage{
			{
				ID:          2,
				Stage:       counterFailureStageApply,
				Topic:       "counter-events",
				MessageKey:  "knowpost:202",
				EntityType:  "knowpost",
				EntityID:    "202",
				Metric:      "like",
				Delta:       2,
				Payload:     `{"entity_type":"knowpost","entity_id":"202","metric":"like"}`,
				Status:      counterFailureStatusPending,
				NextRetryAt: time.Now(),
			},
		},
	}

	worker := NewCounterFailureWorker(store, svc, nil, &config.CounterConfig{
		Repair: config.RepairConfig{
			Enabled:            true,
			BatchSize:          10,
			IntervalMs:         1000,
			CleanupIntervalMs:  3600000,
			CleanupBatchSize:   500,
			DoneRetentionHours: 168,
		},
	})
	if worker == nil {
		t.Fatalf("expected failure worker to be created")
	}

	worker.processOnce(ctx)

	gotRaw, err := rdb.Get(ctx, SdsKey("knowpost", "202")).Bytes()
	if err != nil {
		t.Fatalf("get repaired sds: %v", err)
	}
	if got := readInt32BE(gotRaw, IdxLike*FieldSize); got != 2 {
		t.Fatalf("unexpected like count after repair: got=%d want=2", got)
	}
	if len(store.doneIDs()) != 1 || store.doneIDs()[0] != 2 {
		t.Fatalf("expected apply repair task to be marked done, got=%v", store.doneIDs())
	}
	if len(store.retryCalls()) != 0 {
		t.Fatalf("did not expect apply repair task retry, got=%d", len(store.retryCalls()))
	}
}

func newPublishFailureTask(t *testing.T, id uint64, event CounterEvent) *CounterFailedMessage {
	t.Helper()

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal publish task payload: %v", err)
	}

	return &CounterFailedMessage{
		ID:          id,
		Stage:       counterFailureStagePublish,
		Topic:       "counter-events",
		MessageKey:  event.EntityType + ":" + event.EntityID,
		EntityType:  event.EntityType,
		EntityID:    event.EntityID,
		Metric:      event.Metric,
		Delta:       event.Delta,
		Payload:     string(payload),
		Status:      counterFailureStatusPending,
		NextRetryAt: time.Now(),
	}
}

type stubCounterFailureTaskStore struct {
	mu                sync.Mutex
	tasks             []*CounterFailedMessage
	done              []uint64
	retries           []stubCounterFailureRetryCall
	deleteDoneCalls   []stubCounterFailureDeleteCall
	claimErr          error
	markDoneErr       error
	markRetryErr      error
	deleteDoneErr     error
	deleteDoneResults int64
}

type stubCounterFailureRetryCall struct {
	id          uint64
	retryCount  int
	nextRetryAt time.Time
	message     string
}

type stubCounterFailureDeleteCall struct {
	before time.Time
	limit  int
}

func (s *stubCounterFailureTaskStore) ClaimPending(ctx context.Context, limit int) ([]*CounterFailedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return nil, s.claimErr
	}

	result := make([]*CounterFailedMessage, 0, len(s.tasks))
	for _, task := range s.tasks {
		result = append(result, cloneCounterFailedMessage(task))
	}
	s.tasks = nil
	return result, nil
}

func (s *stubCounterFailureTaskStore) MarkDone(ctx context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markDoneErr != nil {
		return s.markDoneErr
	}
	s.done = append(s.done, id)
	return nil
}

func (s *stubCounterFailureTaskStore) MarkRetry(ctx context.Context, id uint64, retryCount int, nextRetryAt time.Time, errorMessage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markRetryErr != nil {
		return s.markRetryErr
	}
	s.retries = append(s.retries, stubCounterFailureRetryCall{
		id:          id,
		retryCount:  retryCount,
		nextRetryAt: nextRetryAt,
		message:     errorMessage,
	})
	return nil
}

func (s *stubCounterFailureTaskStore) DeleteDoneBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteDoneErr != nil {
		return 0, s.deleteDoneErr
	}
	s.deleteDoneCalls = append(s.deleteDoneCalls, stubCounterFailureDeleteCall{
		before: before,
		limit:  limit,
	})
	return s.deleteDoneResults, nil
}

func (s *stubCounterFailureTaskStore) doneIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.done...)
}

func (s *stubCounterFailureTaskStore) retryCalls() []stubCounterFailureRetryCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stubCounterFailureRetryCall(nil), s.retries...)
}
