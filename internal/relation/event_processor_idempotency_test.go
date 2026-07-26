package relation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// countingStub 记录 ApplyFollowDeltaOnce 的调用与失败注入。
type countingStub struct {
	calls int
	fail  bool
	seen  map[string]bool
}

func (c *countingStub) ApplyFollowDeltaOnce(_ context.Context, dedupeKey string, _ time.Duration, _, _ uint64, _ int64) (bool, error) {
	c.calls++
	if c.fail {
		return false, errors.New("redis hiccup")
	}
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	if c.seen[dedupeKey] {
		return false, nil
	}
	c.seen[dedupeKey] = true
	return true, nil
}

func newProcessorForTest(t *testing.T) (*EventProcessor, *countingStub, *miniredis.Miniredis) {
	t.Helper()
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	stub := &countingStub{}
	return NewEventProcessor(rdb, stub, zap.NewNop()), stub, srv
}

// TestProcess_RetryAfterCountFailureIsNotLost 验证幂等协议纠序的核心：
// 计数失败 → 返回错误 → 重投后计数**仍会执行**，事件不会因“标记已落”而永久丢失。
//
// 早期协议是「SetNX 落标 → ZSet → 计数」：标记先落，后续任一步失败，
// 重投命中标记被跳过，那次关注在计数里永久少一。
func TestProcess_RetryAfterCountFailureIsNotLost(t *testing.T) {
	p, stub, _ := newProcessorForTest(t)
	evt := RelationEvent{EventType: "FollowCreated", FromUserID: 1, ToUserID: 2}

	stub.fail = true
	if err := p.Process(context.Background(), evt); err == nil {
		t.Fatal("first attempt should surface the counting error for redelivery")
	}

	stub.fail = false
	if err := p.Process(context.Background(), evt); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if len(stub.seen) != 1 {
		t.Fatalf("counting must eventually happen exactly once, seen=%v", stub.seen)
	}
}

// TestProcess_DuplicateDeliveryCountsOnce 验证重复投递只计一次（ZSet 重放无害）。
func TestProcess_DuplicateDeliveryCountsOnce(t *testing.T) {
	p, stub, srv := newProcessorForTest(t)
	evt := RelationEvent{EventType: "FollowCreated", FromUserID: 1, ToUserID: 2}

	for i := 0; i < 3; i++ {
		if err := p.Process(context.Background(), evt); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if len(stub.seen) != 1 {
		t.Fatalf("dedupe keys seen=%v, want exactly one", stub.seen)
	}
	// ZSet 投影存在且只有一个成员
	members, _ := srv.ZMembers("z:following:1")
	if len(members) != 1 || members[0] != "2" {
		t.Fatalf("following zset=%v", members)
	}
}
