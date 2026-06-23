package contextutil

import (
	"context"
	"testing"
	"time"
)

func TestSleep_Normal(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	ok := Sleep(ctx, 50*time.Millisecond)
	elapsed := time.Since(start)

	if !ok {
		t.Fatal("expected true for normal sleep")
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("elapsed = %v, want >= 40ms", elapsed)
	}
}

func TestSleep_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	ok := Sleep(ctx, 5*time.Second)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected false for cancelled context")
	}
	if elapsed > time.Second {
		t.Fatalf("elapsed = %v, want near 0 for cancelled ctx", elapsed)
	}
}

func TestSleep_ZeroDuration(t *testing.T) {
	ctx := context.Background()
	ok := Sleep(ctx, 0)
	if !ok {
		t.Fatal("expected true for zero duration")
	}
}

func TestSleep_NegativeDuration(t *testing.T) {
	ctx := context.Background()
	ok := Sleep(ctx, -1*time.Second)
	if !ok {
		t.Fatal("expected true for negative duration")
	}
}

func TestSleep_CancelDuringSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	ok := Sleep(ctx, 5*time.Second)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected false when cancelled during sleep")
	}
	if elapsed > time.Second {
		t.Fatalf("elapsed = %v, want near 20ms", elapsed)
	}
}