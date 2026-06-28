package cacheutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/redislock"
)

func startTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return rdb, func() { rdb.Close(); mr.Close() }
}

func TestCacheReadThrough_HitFirst(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	result, err := CacheReadThrough(
		ctx, rdb,
		"lock:test", redislock.Options{TTL: time.Second},
		time.Millisecond,
		func(ctx context.Context) (string, bool, error) {
			return "cached_value", true, nil
		},
		func(ctx context.Context) (string, error) {
			return "miss_value", nil
		},
	)
	if err != nil {
		t.Fatalf("CacheReadThrough: %v", err)
	}
	if result != "cached_value" {
		t.Fatalf("result = %q, want cached_value", result)
	}
}

func TestCacheReadThrough_MissThenHit(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	callCount := 0
	result, err := CacheReadThrough(
		ctx, rdb,
		"lock:miss", redislock.Options{TTL: time.Second},
		time.Millisecond,
		func(ctx context.Context) (string, bool, error) {
			callCount++
			return "", false, nil
		},
		func(ctx context.Context) (string, error) {
			return "from_source", nil
		},
	)
	if err != nil {
		t.Fatalf("CacheReadThrough: %v", err)
	}
	if result != "from_source" {
		t.Fatalf("result = %q, want from_source", result)
	}
}

func TestCacheReadThrough_CheckCacheError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	_, err := CacheReadThrough(
		ctx, rdb,
		"lock:err", redislock.Options{TTL: time.Second},
		time.Millisecond,
		func(ctx context.Context) (string, bool, error) {
			return "", false, errors.New("cache error")
		},
		func(ctx context.Context) (string, error) {
			return "val", nil
		},
	)
	if err == nil {
		t.Fatal("expected error from checkCache")
	}
}

func TestCacheReadThrough_MissHandlerError(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	_, err := CacheReadThrough(
		ctx, rdb,
		"lock:misserr", redislock.Options{TTL: time.Second},
		time.Millisecond,
		func(ctx context.Context) (string, bool, error) {
			return "", false, nil
		},
		func(ctx context.Context) (string, error) {
			return "", errors.New("source error")
		},
	)
	if err == nil {
		t.Fatal("expected error from missHandler")
	}
}

func TestCacheReadThrough_ContextCancelled(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx, cancel := context.WithCancel(context.Background())

	lock1, ok, err := redislock.TryAcquire(ctx, rdb, "lock:cancel", redislock.Options{TTL: 5 * time.Second}, nil)
	if err != nil || !ok {
		t.Fatalf("pre-acquire lock: %v, ok=%v", err, ok)
	}
	defer lock1.Release()

	cancel()

	_, err = CacheReadThrough(
		ctx, rdb,
		"lock:cancel", redislock.Options{TTL: time.Second},
		10*time.Millisecond,
		func(ctx context.Context) (string, bool, error) {
			return "", false, nil
		},
		func(ctx context.Context) (string, error) {
			return "val", nil
		},
	)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}