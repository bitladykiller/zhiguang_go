package counter

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestApplyFollowDeltaOnce_AtomicMarkAndCount 验证「落标+计数」的一次性语义。
//
// HIncrBy 不幂等：若与去重标记分两次调用，崩溃窗口会留下「标记了但没数」的中间态，
// 重投将跳过计数、关注数永久少一。合进单段 Lua 后只有两种终态，重投各得其所。
func TestApplyFollowDeltaOnce_AtomicMarkAndCount(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()
	svc := NewCounterService(rdb, nil, nil, nil, "counter-events", nil, zap.NewNop(), nil)
	uc := NewUserCounter(svc)
	ctx := context.Background()

	first, err := uc.ApplyFollowDeltaOnce(ctx, "dedup:rel:test:1", 10*time.Minute, 1, 2, 1)
	if err != nil || !first {
		t.Fatalf("first=%v err=%v, want first=true", first, err)
	}
	// 重复投递：不再计数
	first, err = uc.ApplyFollowDeltaOnce(ctx, "dedup:rel:test:1", 10*time.Minute, 1, 2, 1)
	if err != nil || first {
		t.Fatalf("dup first=%v err=%v, want first=false", first, err)
	}

	following, _ := rdb.HGet(ctx, "cnt:user:1", "following").Int64()
	follower, _ := rdb.HGet(ctx, "cnt:user:2", "follower").Int64()
	if following != 1 || follower != 1 {
		t.Fatalf("following=%d follower=%d, want 1/1 after duplicate delivery", following, follower)
	}
}

// TestApplyFollowDeltaOnce_NilReceiverSafe 零依赖安全。
func TestApplyFollowDeltaOnce_NilReceiverSafe(t *testing.T) {
	var uc *UserCounter
	if first, err := uc.ApplyFollowDeltaOnce(context.Background(), "k", time.Minute, 1, 2, 1); err != nil || first {
		t.Fatalf("nil receiver: first=%v err=%v", first, err)
	}
}
