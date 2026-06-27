package fanout

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/internal/model"
	"go.uber.org/zap"
)

type stubFollowerLister struct {
	fans []uint64
	err  error
}

func (s *stubFollowerLister) Followers(_ context.Context, _ uint64, _, _ int) ([]uint64, error) {
	return s.fans, s.err
}

func TestFanoutPost_Normal(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	cfg := DefaultConfig()
	cfg.FanoutBatchSize = 2
	cfg.FanoutMaxFans = 100
	cfg.TimelineMaxItems = 10

	fans := []uint64{1001, 1002, 1003}
	svc := NewService(rdb, &stubFollowerLister{fans: fans}, zap.NewNop(), cfg)

	event := &model.FanoutEvent{
		PostID:    42,
		CreatorID: 1,
		CreatedAt: 1700000000,
	}

	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}

	for _, fid := range fans {
		timelineKey := "timeline:" + itoa(fid)
		members, err := rdb.ZRevRange(context.Background(), timelineKey, 0, -1).Result()
		if err != nil {
			t.Fatalf("ZRevRange: %v", err)
		}
		if len(members) != 1 || members[0] != "42" {
			t.Errorf("timeline %s members = %v, want [42]", timelineKey, members)
		}
	}
}

func TestFanoutPost_TooManyFans(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	cfg := DefaultConfig()
	cfg.FanoutMaxFans = 2

	fans := []uint64{1001, 1002, 1003}
	svc := NewService(rdb, &stubFollowerLister{fans: fans}, zap.NewNop(), cfg)

	event := &model.FanoutEvent{
		PostID:    42,
		CreatorID: 1,
		CreatedAt: 1700000000,
	}

	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}

	for _, fid := range fans {
		timelineKey := "timeline:" + itoa(fid)
		exists := srv.Exists(timelineKey)
		if exists {
			t.Errorf("timeline %s should not exist for fan %d (too many fans)", timelineKey, fid)
		}
	}
}

func TestFanoutPost_EmptyFans(t *testing.T) {
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	cfg := DefaultConfig()

	svc := NewService(rdb, &stubFollowerLister{fans: nil}, zap.NewNop(), cfg)

	event := &model.FanoutEvent{
		PostID:    42,
		CreatorID: 1,
		CreatedAt: 1700000000,
	}

	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}
}

func TestFanoutPost_NilDeps(t *testing.T) {
	cfg := DefaultConfig()
	svc := NewService(nil, nil, zap.NewNop(), cfg)

	event := &model.FanoutEvent{
		PostID:    42,
		CreatorID: 1,
		CreatedAt: 1700000000,
	}

	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost with nil deps should not error: %v", err)
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
