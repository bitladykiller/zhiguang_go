package counter

import (
	"testing"
	"time"
)

func TestLocalCounterDeduperExpiresAfterSixBuckets(t *testing.T) {
	t.Helper()

	base := time.Unix(1_700_000_000, 0)
	deduper := newLocalCounterDeduper(6, 10*time.Second)
	deduper.lastRotate = base

	deduper.rememberAt(base, 101)

	if !deduper.seenAt(base, 101) {
		t.Fatalf("expected message to be visible in current bucket")
	}
	if !deduper.seenAt(base.Add(59*time.Second), 101) {
		t.Fatalf("expected message to remain visible within the 60s dedup window")
	}
	if deduper.seenAt(base.Add(60*time.Second), 101) {
		t.Fatalf("expected message to expire after six 10s buckets")
	}
}
