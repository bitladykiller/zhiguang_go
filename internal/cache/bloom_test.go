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
		Key:               "bloom:test",
	}
	return NewRedisBloom(rdb, cfg, zap.NewNop()), srv
}

func TestRedisBloom_NilSafe(t *testing.T) {
	var b *RedisBloom
	b.AddUint64(context.Background(), 1)
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
