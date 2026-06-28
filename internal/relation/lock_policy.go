package relation

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/zhiguang/app/pkg/redislock"
)

const (
	relationListCacheWarmLimit      = 2000
	relationListCacheTTL            = 2 * time.Hour
	relationListCacheLockTTL        = 5 * time.Second
	relationListCacheLockRetry      = 50 * time.Millisecond
	relationListCacheLockOpTimeout  = time.Second
	relationInvalidateLockWaitLimit = 2 * time.Second
)

type relationListCacheTarget struct {
	listType string
	userID   uint64
}

// listCacheLockKey returns the distributed lock key for relationship list cache.
//
// WHY lock by listType + userID:
//   - "Following list" and "Followers list" are two independent caches and should not block each other.
//   - For the same user and same list type, cold-start backfill and post-write invalidation need global serialization.
func listCacheLockKey(listType string, userID uint64) string {
	return fmt.Sprintf("lock:relation:list:%s:%d", listType, userID)
}

func relationListCacheLockOptions() redislock.Options {
	return redislock.Options{
		TTL:              relationListCacheLockTTL,
		WatchdogInterval: relationListCacheLockTTL / 3,
		OpTimeout:        relationListCacheLockOpTimeout,
	}
}

// acquireListCacheLock acquires a single relationship list cache lock.
func (s *RelationService) acquireListCacheLock(ctx context.Context, listType string, userID uint64) (*redislock.Lock, error) {
	return redislock.AcquireWithRetry(
		ctx,
		s.redis,
		listCacheLockKey(listType, userID),
		relationListCacheLockOptions(),
		relationListCacheLockRetry,
		s.logger,
	)
}

// acquireListCacheLocks acquires multiple relationship list cache locks, sorted by lock key to avoid deadlocks.
func (s *RelationService) acquireListCacheLocks(ctx context.Context, targets []relationListCacheTarget) ([]*redislock.Lock, error) {
	sorted := append([]relationListCacheTarget(nil), targets...)
	sort.Slice(sorted, func(i, j int) bool {
		return listCacheLockKey(sorted[i].listType, sorted[i].userID) < listCacheLockKey(sorted[j].listType, sorted[j].userID)
	})

	locks := make([]*redislock.Lock, 0, len(sorted))
	for _, target := range sorted {
		lock, err := s.acquireListCacheLock(ctx, target.listType, target.userID)
		if err != nil {
			for i := len(locks) - 1; i >= 0; i-- {
				locks[i].Release()
			}
			return nil, fmt.Errorf("acquire list cache locks: %w", err)
		}
		locks = append(locks, lock)
	}
	return locks, nil
}
