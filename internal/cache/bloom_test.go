package cache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func newTestBloom(t *testing.T) (*RedisBloom, *miniredis.Miniredis) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := BloomConfig{
		Enabled:           true,
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
		Key:               "cuckoo:test",
	}
	return NewRedisBloom(rdb, cfg, zap.NewNop()), srv
}

func TestRedisBloom_NilSafe(t *testing.T) {
	var b *RedisBloom
	b.AddUint64(context.Background(), 1)
	b.DeleteUint64(context.Background(), 1)
	ok, err := b.MightContainUint64(context.Background(), 1)
	if err != nil || !ok {
		t.Fatalf("nil bloom should fail-open: ok=%v err=%v", ok, err)
	}
}

func TestRedisBloom_DisabledReturnsNil(t *testing.T) {
	srv, _ := miniredis.Run()
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	if got := NewRedisBloom(rdb, BloomConfig{Enabled: false}, nil); got != nil {
		t.Fatal("disabled should return nil")
	}
}

func TestRedisBloom_ColdStartFailOpen(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	// 空过滤器必须 fail-open，避免冷启动误拦全站
	ok, err := b.MightContainUint64(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cold bloom must MightContain=true (fail-open)")
	}
	if b.IsWarm(ctx) {
		t.Fatal("empty bloom should not be warm")
	}
}

func TestRedisBloom_AddAndMightContain(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()

	b.AddUint64(ctx, 42)
	if !b.IsWarm(ctx) {
		t.Fatal("after Add should be warm")
	}
	ok, err := b.MightContainUint64(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("added id must MightContain=true")
	}

	// 未添加的 id 应报告一定不存在（极低概率误判）
	ok, err = b.MightContainUint64(ctx, 999999)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("unrelated id should MightContain=false after warm")
	}
}

func TestRedisBloom_DeleteRemovesMember(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()

	b.AddUint64(ctx, 42)
	b.AddUint64(ctx, 43)
	ok, err := b.MightContainUint64(ctx, 42)
	if err != nil || !ok {
		t.Fatalf("pre-delete: ok=%v err=%v", ok, err)
	}

	b.DeleteUint64(ctx, 42)
	ok, err = b.MightContainUint64(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("deleted id should MightContain=false")
	}

	// 删除不应误伤其它成员
	ok, err = b.MightContainUint64(ctx, 43)
	if err != nil || !ok {
		t.Fatalf("sibling must remain: ok=%v err=%v", ok, err)
	}
	if !b.IsWarm(ctx) {
		t.Fatal("filter should stay warm while other members remain")
	}
}

func TestRedisBloom_DeleteIdempotent(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	b.DeleteUint64(ctx, 1) // no-op on empty
	b.AddUint64(ctx, 7)
	b.DeleteUint64(ctx, 7)
	b.DeleteUint64(ctx, 7) // second delete no-op
	ok, err := b.MightContainUint64(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("after double delete should be absent")
	}
}

func TestRedisBloom_NoFalseNegative(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	for i := uint64(1); i <= 200; i++ {
		b.AddUint64(ctx, i)
	}
	for i := uint64(1); i <= 200; i++ {
		ok, err := b.MightContainUint64(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("false negative on id=%d", i)
		}
	}
}

func TestRedisBloom_AddDeleteCycleNoFalseNegative(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	for i := uint64(1); i <= 100; i++ {
		b.AddUint64(ctx, i)
	}
	for i := uint64(1); i <= 50; i++ {
		b.DeleteUint64(ctx, i)
	}
	for i := uint64(51); i <= 100; i++ {
		ok, err := b.MightContainUint64(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("false negative after partial delete id=%d", i)
		}
	}
	for i := uint64(1); i <= 50; i++ {
		ok, err := b.MightContainUint64(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatalf("deleted id still present id=%d", i)
		}
	}
}

func TestBloomHashesStable(t *testing.T) {
	h1a, h2a := bloomHashes("123")
	h1b, h2b := bloomHashes("123")
	if h1a != h1b || h2a != h2b {
		t.Fatal("hashes must be stable")
	}
	h1c, _ := bloomHashes("124")
	if h1a == h1c {
		t.Fatal("different members should differ in hash (almost always)")
	}
}

func TestDefaultBloomConfig(t *testing.T) {
	cfg := DefaultBloomConfig()
	if !cfg.Enabled || cfg.ExpectedItems == 0 || cfg.Key == "" {
		t.Fatalf("bad defaults: %+v", cfg)
	}
}

func TestRedisBloom_Stats(t *testing.T) {
	b, _ := newTestBloom(t)
	key, m, k := b.Stats()
	if key == "" || m == 0 || k == 0 {
		t.Fatalf("stats key=%s m=%d k=%d", key, m, k)
	}
	var nilB *RedisBloom
	if k2, m2, h2 := nilB.Stats(); k2 != "" || m2 != 0 || h2 != 0 {
		t.Fatal("nil stats should be zero")
	}
}

func TestNextPowerOfTwo(t *testing.T) {
	if got := nextPowerOfTwo(1); got != 1 {
		t.Fatalf("1 -> %d", got)
	}
	if got := nextPowerOfTwo(3); got != 4 {
		t.Fatalf("3 -> %d", got)
	}
	if got := nextPowerOfTwo(1000); got != 1024 {
		t.Fatalf("1000 -> %d", got)
	}
}

func TestNewRedisBloom_ConfigDefaultsAndClamp(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	b := NewRedisBloom(rdb, BloomConfig{
		Enabled:           true,
		ExpectedItems:     0, // default 1e6
		FalsePositiveRate: 0, // default 0.01
		Key:               "",
		BucketSize:        100, // clamp to 16
		MaxKicks:          0,
	}, nil)
	if b == nil {
		t.Fatal("expected non-nil")
	}
	if b.key != "cuckoo:knowpost:ids" {
		t.Fatalf("default key=%s", b.key)
	}
	if b.bucketSize != 16 {
		t.Fatalf("bucketSize=%d", b.bucketSize)
	}
	if b.maxKicks != cuckooMaxKicks {
		t.Fatalf("maxKicks=%d", b.maxKicks)
	}

	// 更严 FPR 会放大 m
	b2 := NewRedisBloom(rdb, BloomConfig{
		Enabled: true, ExpectedItems: 1000, FalsePositiveRate: 0.001, Key: "cuckoo:strict",
	}, zap.NewNop())
	_, m1, _ := b.Stats()
	_, m2, _ := b2.Stats()
	// b 用 1e6 项，m2 是 1000 项但 FPR 更严；只断言 b2 可构造
	if m2 == 0 || m1 == 0 {
		t.Fatal("m should be positive")
	}
}

func TestRedisBloom_EmptyMemberNoop(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	b.Add(ctx, "")
	b.Delete(ctx, "")
	ok, err := b.MightContain(ctx, "")
	if err != nil || !ok {
		t.Fatalf("empty member fail-open: ok=%v err=%v", ok, err)
	}
	if b.IsWarm(ctx) {
		t.Fatal("empty member must not initialize table")
	}
}

func TestRedisBloom_ReAddAfterDelete(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	b.AddUint64(ctx, 9)
	b.DeleteUint64(ctx, 9)
	b.AddUint64(ctx, 9)
	ok, err := b.MightContainUint64(ctx, 9)
	if err != nil || !ok {
		t.Fatalf("re-add: ok=%v err=%v", ok, err)
	}
}

func TestRedisBloom_NilRdbReturnsNil(t *testing.T) {
	if NewRedisBloom(nil, DefaultBloomConfig(), nil) != nil {
		t.Fatal("nil rdb should return nil")
	}
}

func TestRedisBloom_DuplicateAddIdempotent(t *testing.T) {
	b, _ := newTestBloom(t)
	ctx := context.Background()
	b.AddUint64(ctx, 55)
	b.AddUint64(ctx, 55)
	ok, err := b.MightContainUint64(ctx, 55)
	if err != nil || !ok {
		t.Fatalf("dup add: ok=%v err=%v", ok, err)
	}
}
