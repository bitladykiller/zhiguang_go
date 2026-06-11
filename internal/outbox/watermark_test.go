package outbox

import (
	"context"
	"testing"

	"github.com/zhiguang/app/internal/testutil"
)

func TestWatermarkTrackerAdvanceAndRead(t *testing.T) {
	client := testutil.StartRedisServer(t)
	tracker := NewWatermarkTracker(client, "group-a", "topic-a")
	if tracker == nil {
		t.Fatal("expected watermark tracker to be created")
	}

	ctx := context.Background()

	lastApplied, err := tracker.LastApplied(ctx, 2)
	if err != nil {
		t.Fatalf("LastApplied initial: %v", err)
	}
	if lastApplied != -1 {
		t.Fatalf("expected initial applied offset -1, got %d", lastApplied)
	}

	if err := tracker.Advance(ctx, 2, 0); err != nil {
		t.Fatalf("advance to 0: %v", err)
	}
	if err := tracker.Advance(ctx, 2, 0); err != nil {
		t.Fatalf("duplicate advance to 0 should be ignored: %v", err)
	}

	lastApplied, err = tracker.LastApplied(ctx, 2)
	if err != nil {
		t.Fatalf("LastApplied after advance: %v", err)
	}
	if lastApplied != 0 {
		t.Fatalf("expected applied offset 0, got %d", lastApplied)
	}

	if err := tracker.Advance(ctx, 2, 2); err == nil {
		t.Fatal("expected offset gap to fail")
	}
	if err := tracker.Advance(ctx, 2, 1); err != nil {
		t.Fatalf("advance to 1: %v", err)
	}
}

func TestAppliedOffsetKey(t *testing.T) {
	t.Parallel()

	got := AppliedOffsetKey("group-a", "topic-a", 3)
	want := "outbox:applied-offset:group-a:topic-a:3"
	if got != want {
		t.Fatalf("unexpected key, got %q want %q", got, want)
	}
}
