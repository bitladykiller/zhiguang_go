package relation

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

type relationListCacheTarget struct {
	listType string
	userID   uint64
}

// listCacheLockKey 返回关系列表缓存的分布式锁键。
func listCacheLockKey(listType string, userID uint64) string {
	return fmt.Sprintf("lock:relation:list:%s:%d", listType, userID)
}

func relationListCacheLockOptions(cfg *config.RelationConfig) redislock.Options {
	ttl := 5 * time.Second
	opTimeout := time.Second
	if cfg != nil && cfg.CacheLock.TTLMs > 0 {
		ttl = time.Duration(cfg.CacheLock.TTLMs) * time.Millisecond
	}
	if cfg != nil && cfg.CacheLock.OpTimeoutMs > 0 {
		opTimeout = time.Duration(cfg.CacheLock.OpTimeoutMs) * time.Millisecond
	}
	return redislock.Options{
		TTL:              ttl,
		WatchdogInterval: ttl / 3,
		OpTimeout:        opTimeout,
	}
}

func relationListCacheLockRetryInterval(cfg *config.RelationConfig) time.Duration {
	if cfg != nil && cfg.CacheLock.RetryIntervalMs > 0 {
		return time.Duration(cfg.CacheLock.RetryIntervalMs) * time.Millisecond
	}
	return 50 * time.Millisecond
}

func relationInvalidateLockWaitLimit(cfg *config.RelationConfig) time.Duration {
	if cfg != nil && cfg.InvalidateLock.WaitLimitMs > 0 {
		return time.Duration(cfg.InvalidateLock.WaitLimitMs) * time.Millisecond
	}
	return 2 * time.Second
}

func relationListCacheWarmLimit(cfg *config.RelationConfig) int {
	if cfg != nil && cfg.ZSetWarmLimit > 0 {
		return cfg.ZSetWarmLimit
	}
	return 2000
}

func relationListCacheTTL(cfg *config.RelationConfig) time.Duration {
	if cfg != nil && cfg.CacheTTL > 0 {
		return time.Duration(cfg.CacheTTL) * time.Second
	}
	return 2 * time.Hour
}

func relationL1CacheTTL(cfg *config.RelationConfig) int {
	if cfg != nil && cfg.L1Cache.TTLSeconds > 0 {
		return cfg.L1Cache.TTLSeconds
	}
	return 600
}

func relationFillL1Limit(cfg *config.RelationConfig) int {
	if cfg != nil && cfg.L1Cache.FillLimit > 0 {
		return cfg.L1Cache.FillLimit
	}
	return 500
}

func relationMaxOffset(cfg *config.RelationConfig) int {
	if cfg != nil && cfg.MaxOffset > 0 {
		return cfg.MaxOffset
	}
	return 1000
}

func relationFallbackExhaustedTTL(cfg *config.RelationConfig) time.Duration {
	if cfg != nil && cfg.Fallback.ExhaustedTTLMinutes > 0 {
		return time.Duration(cfg.Fallback.ExhaustedTTLMinutes) * time.Minute
	}
	return 10 * time.Minute
}

// acquireListCacheLock 获取单个关系列表缓存锁。
func (s *RelationService) acquireListCacheLock(ctx context.Context, listType string, userID uint64) (*redislock.Lock, error) {
	return redislock.AcquireWithRetry(
		ctx,
		s.redis,
		listCacheLockKey(listType, userID),
		relationListCacheLockOptions(s.cfg),
		relationListCacheLockRetryInterval(s.cfg),
		s.logger,
	)
}

// acquireListCacheLocks 获取多个关系列表缓存锁，按键排序以避免死锁。
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
