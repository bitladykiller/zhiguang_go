package knowpost

import (
	"context"
	"testing"

	"github.com/coocood/freecache"

	"github.com/zhiguang/app/internal/testutil"
)

func TestInvalidateCacheBumpsDetailVersion(t *testing.T) {
	client := testutil.StartRedisServer(t)
	service := &KnowPostService{
		redis:   client,
		l1Cache: freecache.NewCache(1 << 20),
	}

	ctx := context.Background()
	const postID uint64 = 42

	initialVersion := service.currentDetailVersion(ctx, postID)
	if initialVersion != 0 {
		t.Fatalf("expected initial detail version 0, got %d", initialVersion)
	}

	initialKey := detailCacheKey(postID, initialVersion)
	if err := client.Set(ctx, initialKey, "detail-v0", 0).Err(); err != nil {
		t.Fatalf("seed redis detail cache: %v", err)
	}
	if err := service.l1Cache.Set([]byte(initialKey), []byte("detail-v0"), 60); err != nil {
		t.Fatalf("seed l1 detail cache: %v", err)
	}

	service.invalidateCache(postID)

	if _, err := service.l1Cache.Get([]byte(initialKey)); err == nil {
		t.Fatal("expected local L1 old detail cache to be deleted")
	}
	if _, err := client.Get(ctx, initialKey).Result(); err == nil {
		t.Fatal("expected redis old detail cache to be deleted")
	}

	nextVersion := service.currentDetailVersion(ctx, postID)
	if nextVersion != 1 {
		t.Fatalf("expected detail version 1 after invalidation, got %d", nextVersion)
	}

	nextKey := detailCacheKey(postID, nextVersion)
	if nextKey == initialKey {
		t.Fatal("expected detail cache key to change after version bump")
	}
}
