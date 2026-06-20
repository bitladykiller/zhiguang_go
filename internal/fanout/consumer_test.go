package fanout

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/internal/model"
	"go.uber.org/zap"
)

type stubFollowerLister2 struct {
	fans []uint64
}

func (s *stubFollowerLister2) Followers(_ context.Context, _ uint64, _, _ int) ([]uint64, error) {
	return s.fans, nil
}

func TestFanoutConsumer_StartStop(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	cfg := DefaultConfig()
	svc := NewService(rdb, &stubFollowerLister2{fans: []uint64{1001}}, zap.NewNop(), cfg)

	consumer := NewFanoutConsumer([]string{"127.0.0.1:1"}, "test-group", "fanout-test", svc, zap.NewNop())
	if consumer == nil {
		t.Fatal("NewFanoutConsumer returned nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	consumer.Start(ctx)
}

func TestFanoutConsumer_ProcessMessage(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	cfg := DefaultConfig()
	svc := NewService(rdb, &stubFollowerLister2{fans: []uint64{1001}}, zap.NewNop(), cfg)

	event := model.FanoutEvent{PostID: 42, CreatorID: 1, CreatedAt: time.Now().Unix()}
	data, _ := json.Marshal(event)

	if err := svc.FanoutPost(context.Background(), &event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}

	timelineKey := "timeline:1001"
	members, err := rdb.ZRevRange(context.Background(), timelineKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRevRange: %v", err)
	}
	if len(members) != 1 || members[0] != "42" {
		t.Errorf("timeline members = %v, want [42]", members)
	}

	_ = data
}