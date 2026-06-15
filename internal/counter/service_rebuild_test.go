package counter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/internal/testutil"
)

type stubCounterFailureTaskStore struct {
	markErr error
	marked  []counterFailureMarkCall
}

type counterFailureMarkCall struct {
	entityType string
	entityID   string
	maxEpoch   uint64
}

type stubCounterEventPublisher struct {
	events []*CounterEvent
	err    error
}

func (s *stubCounterFailureTaskStore) ClaimPending(ctx context.Context, limit int) ([]*CounterFailedMessage, error) {
	return nil, nil
}

func (s *stubCounterFailureTaskStore) MarkDone(ctx context.Context, id uint64) error {
	return nil
}

func (s *stubCounterFailureTaskStore) MarkRetry(ctx context.Context, id uint64, retryCount int, nextRetryAt time.Time, errorMessage string) error {
	return nil
}

func (s *stubCounterFailureTaskStore) DeleteDoneBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	return 0, nil
}

func (s *stubCounterFailureTaskStore) MarkEntityTasksDoneThroughEpoch(ctx context.Context, entityType, entityID string, maxEpoch uint64) (int64, error) {
	s.marked = append(s.marked, counterFailureMarkCall{
		entityType: entityType,
		entityID:   entityID,
		maxEpoch:   maxEpoch,
	})
	return 1, s.markErr
}

func (s *stubCounterEventPublisher) Publish(event *CounterEvent) error {
	if event != nil {
		cloned := *event
		s.events = append(s.events, &cloned)
	}
	return s.err
}

func TestRebuildSdsMarksFailureTasksDoneAfterSnapshotWrite(t *testing.T) {
	client := testutil.StartRedisServer(t)
	store := &stubCounterFailureTaskStore{}
	service := NewCounterService(CounterServiceDeps{
		Redis:        client,
		FailureTasks: store,
	})

	setCounterBitmapBit(t, client, "like", "knowpost", "1001", 1)
	setCounterBitmapBit(t, client, "like", "knowpost", "1001", 2)
	setCounterBitmapBit(t, client, "fav", "knowpost", "1001", 9)

	raw, err := service.rebuildSds(context.Background(), "knowpost", "1001")
	if err != nil {
		t.Fatalf("rebuildSds() error = %v", err)
	}

	if got := readInt32BE(raw, IdxLike*FieldSize); got != 2 {
		t.Fatalf("like count = %d, want 2", got)
	}
	if got := readInt32BE(raw, IdxFav*FieldSize); got != 1 {
		t.Fatalf("fav count = %d, want 1", got)
	}

	if len(store.marked) != 1 {
		t.Fatalf("marked call count = %d, want 1", len(store.marked))
	}
	if got := store.marked[0]; got.entityType != "knowpost" || got.entityID != "1001" || got.maxEpoch != 0 {
		t.Fatalf("marked call = %+v", got)
	}
	if epoch, err := client.Get(context.Background(), ActiveEpochKey("knowpost", "1001")).Uint64(); err != nil || epoch != 1 {
		t.Fatalf("active epoch = %d, err = %v, want 1", epoch, err)
	}

	saved, err := client.Get(context.Background(), SdsKey("knowpost", "1001")).Bytes()
	if err != nil {
		t.Fatalf("read rebuilt sds: %v", err)
	}
	if got := readInt32BE(saved, IdxLike*FieldSize); got != 2 {
		t.Fatalf("saved like count = %d, want 2", got)
	}
}

func TestCounterFailureWorkerPublishTaskRepublishesDeltaEvent(t *testing.T) {
	client := testutil.StartRedisServer(t)
	producer := &stubCounterEventPublisher{}
	service := NewCounterService(CounterServiceDeps{
		Redis:    client,
		Producer: producer,
	})
	worker := &CounterFailureWorker{service: service}

	payload, err := json.Marshal(&CounterEvent{
		EntityType: "knowpost",
		EntityID:   "2002",
		Metric:     "like",
		Index:      IdxLike,
		UserID:     42,
		Delta:      1,
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	if err := worker.handleTask(context.Background(), &CounterFailedMessage{
		Stage:      counterFailureStagePublish,
		Payload:    string(payload),
		EntityType: "knowpost",
		EntityID:   "2002",
		Metric:     "like",
	}); err != nil {
		t.Fatalf("handleTask() error = %v", err)
	}

	if len(producer.events) != 1 {
		t.Fatalf("published event count = %d, want 1", len(producer.events))
	}
	if got := producer.events[0]; got.EntityType != "knowpost" || got.EntityID != "2002" || got.Metric != "like" || got.Delta != 1 || got.UserID != 42 {
		t.Fatalf("published event = %+v", got)
	}
}

func TestToggleUsesActiveEpoch(t *testing.T) {
	client := testutil.StartRedisServer(t)
	producer := &stubCounterEventPublisher{}
	service := NewCounterService(CounterServiceDeps{
		Redis:    client,
		Producer: producer,
	})

	if err := client.Set(context.Background(), ActiveEpochKey("knowpost", "3003"), 7, 0).Err(); err != nil {
		t.Fatalf("seed active epoch: %v", err)
	}

	changed, err := service.Like(context.Background(), 99, "knowpost", "3003")
	if err != nil {
		t.Fatalf("Like() error = %v", err)
	}
	if !changed {
		t.Fatal("Like() changed = false, want true")
	}

	waitPublishedEvent(t, producer, 1)
	if got := producer.events[0].Epoch; got != 7 {
		t.Fatalf("published epoch = %d, want 7", got)
	}
	liked, err := service.IsLiked(context.Background(), 99, "knowpost", "3003")
	if err != nil {
		t.Fatalf("IsLiked() error = %v", err)
	}
	if !liked {
		t.Fatal("IsLiked() = false, want true")
	}
}

func waitPublishedEvent(t *testing.T, producer *stubCounterEventPublisher, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(producer.events) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("published event count = %d, want >= %d", len(producer.events), want)
}

func setCounterBitmapBit(t *testing.T, client *redis.Client, metric, entityType, entityID string, userID uint64) {
	t.Helper()
	if err := client.SetBit(context.Background(), BitmapKey(metric, entityType, entityID, ChunkOf(userID)), int64(BitOf(userID)), 1).Err(); err != nil {
		t.Fatalf("set bitmap bit: %v", err)
	}
}
