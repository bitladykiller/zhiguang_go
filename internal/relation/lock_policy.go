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

// listCacheLockKey 返回关系列表缓存的分布式锁键。
//
// WHY 按 listType + userID 加锁：
//   - “关注列表”和“粉丝列表”是两套独立缓存，彼此不应互相阻塞。
//   - 同一用户的同一类列表缓存，在冷启动回填和写后失效时需要全局串行化。
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

// acquireListCacheLock 获取单个关系列表缓存锁。
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

// acquireListCacheLocks 获取多个关系列表缓存锁，并按锁键排序避免死锁。
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
