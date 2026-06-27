package redislock

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/testutil"
)

func startTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	rdb := testutil.StartTestRedis(t)
	return rdb, func() { rdb.Close() }
}

func TestTryAcquire_Success(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	lock, ok, err := TryAcquire(ctx, rdb, "lock:test", Options{TTL: time.Second})
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !ok {
		t.Fatal("expected lock acquired")
	}
	if lock == nil {
		t.Fatal("expected non-nil lock")
	}
	lock.Release()
}

func TestTryAcquire_Conflict(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	lock1, ok1, err := TryAcquire(ctx, rdb, "lock:conflict", Options{TTL: time.Second})
	if err != nil || !ok1 {
		t.Fatalf("first acquire: %v, ok=%v", err, ok1)
	}
	defer lock1.Release()

	lock2, ok2, err := TryAcquire(ctx, rdb, "lock:conflict", Options{TTL: time.Second})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if ok2 {
		t.Fatal("expected second acquire to fail")
	}
	if lock2 != nil {
		t.Fatal("expected nil lock on conflict")
	}
}

func TestTryAcquire_ReleaseThenReacquire(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	lock, ok, err := TryAcquire(ctx, rdb, "lock:release", Options{TTL: time.Second})
	if err != nil || !ok {
		t.Fatalf("acquire: %v, ok=%v", err, ok)
	}
	lock.Release()

	lock2, ok2, err := TryAcquire(ctx, rdb, "lock:release", Options{TTL: time.Second})
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	if !ok2 {
		t.Fatal("expected reacquire after release")
	}
	lock2.Release()
}

func TestAcquireWithRetry_Success(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	lock, err := AcquireWithRetry(ctx, rdb, "lock:retry", Options{TTL: time.Second}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireWithRetry: %v", err)
	}
	if lock == nil {
		t.Fatal("expected non-nil lock")
	}
	lock.Release()
}

func TestAcquireWithRetry_ContextCancelled(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := AcquireWithRetry(ctx, rdb, "lock:cancel", Options{TTL: time.Second}, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestLock_WatchdogRenews(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	shortTTL := 200 * time.Millisecond
	lock, ok, err := TryAcquire(ctx, rdb, "lock:watchdog", Options{TTL: shortTTL, WatchdogInterval: 50 * time.Millisecond})
	if err != nil || !ok {
		t.Fatalf("acquire: %v, ok=%v", err, ok)
	}
	defer lock.Release()

	time.Sleep(300 * time.Millisecond)

	val, err := rdb.Get(ctx, "lock:watchdog").Result()
	if err != nil {
		t.Fatalf("get lock key after watchdog: %v", err)
	}
	if val == "" {
		t.Fatal("expected lock key to still exist after watchdog renewal")
	}
}

func TestLock_ReleaseRemovesKey(t *testing.T) {
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	ctx := context.Background()
	lock, ok, err := TryAcquire(ctx, rdb, "lock:remove", Options{TTL: time.Second})
	if err != nil || !ok {
		t.Fatalf("acquire: %v, ok=%v", err, ok)
	}
	lock.Release()

	exists, err := rdb.Exists(ctx, "lock:remove").Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists != 0 {
		t.Fatal("expected lock key to be removed after release")
	}
}

func TestLock_NilSafe(t *testing.T) {
	var l *Lock
	l.Release()
}

func TestOptions_Normalized(t *testing.T) {
	opts := Options{}.normalized()
	if opts.TTL != defaultTTL {
		t.Fatalf("expected default TTL %v, got %v", defaultTTL, opts.TTL)
	}
	if opts.WatchdogInterval != defaultTTL/3 {
		t.Fatalf("expected default watchdog %v, got %v", defaultTTL/3, opts.WatchdogInterval)
	}
	if opts.OpTimeout != defaultOpTimeout {
		t.Fatalf("expected default op timeout %v, got %v", defaultOpTimeout, opts.OpTimeout)
	}
}