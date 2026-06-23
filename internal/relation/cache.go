package relation

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	l1CacheTTL          = 600
	fallbackExhaustedTTL = 10 * time.Minute
)

// fillL1 将 BigV 用户的前 500 个关注/粉丝 ID 写入 freecache（L1）。
func (s *RelationService) fillL1(ctx context.Context, listType string, userID uint64) {
	key := s.l1KeyStr(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, 500, 0)
	if err != nil || len(entries) == 0 {
		return
	}
	idStrs := make([]string, len(entries))
	for i, e := range entries {
		idStrs[i] = strconv.FormatUint(e.UserID, 10)
	}
	s.l1.Set([]byte(key), []byte(strings.Join(idStrs, ",")), l1CacheTTL)
}

// invalidateCaches 在关注/取关操作后，失效涉及用户的 L1（freecache）和 L2（Redis ZSet）缓存。
func (s *RelationService) invalidateCaches(ctx context.Context, fromUserID, toUserID uint64) {
	cacheCtx, cancel := context.WithTimeout(ctx, relationInvalidateLockWaitLimit)
	defer cancel()

	targets := []relationListCacheTarget{
		{listType: "following", userID: fromUserID},
		{listType: "followers", userID: toUserID},
	}
	locks, err := s.acquireListCacheLocks(cacheCtx, targets)
	if err != nil {
		return
	}
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Release()
		}
	}()

	if err := s.redis.Del(cacheCtx, s.zsetKey("following", fromUserID)).Err(); err != nil {
		s.logger.Warn("failed to invalidate following zset cache", zap.Uint64("fromUserID", fromUserID), zap.Error(err))
	}
	s.l1.Del([]byte(s.l1KeyStr("following", fromUserID)))
	if err := s.redis.Del(cacheCtx, s.zsetKey("followers", toUserID)).Err(); err != nil {
		s.logger.Warn("failed to invalidate followers zset cache", zap.Uint64("toUserID", toUserID), zap.Error(err))
	}
	s.l1.Del([]byte(s.l1KeyStr("followers", toUserID)))

	if err := s.redis.Del(cacheCtx, fmt.Sprintf("follower:fallback:exhausted:%d", toUserID)).Err(); err != nil {
		s.logger.Warn("failed to invalidate follower fallback exhausted key", zap.Uint64("toUserID", toUserID), zap.Error(err))
	}
}

// fillZSet 从数据库读取关注/粉丝列表并回填到 Redis ZSet。
func (s *RelationService) fillZSet(ctx context.Context, listType string, userID uint64) (bool, error) {
	zsetKey := s.zsetKey(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, relationListCacheWarmLimit, 0)
	if err != nil {
		return false, fmt.Errorf("fill zset: read from db: %w", err)
	}
	if len(entries) == 0 {
		return false, nil
	}

	members := make([]redis.Z, len(entries))
	for i, entry := range entries {
		members[i] = redis.Z{
			Score:  float64(entry.CreatedAt),
			Member: strconv.FormatUint(entry.UserID, 10),
		}
	}

	if err := s.redis.ZAdd(ctx, zsetKey, members...).Err(); err != nil {
		return false, fmt.Errorf("fill zset: redis zadd: %w", err)
	}
	s.redis.Expire(ctx, zsetKey, relationListCacheTTL)
	return true, nil
}

// ensureListCacheWarm 在分布式锁保护下回填某个用户的关注/粉丝 ZSet。
func (s *RelationService) ensureListCacheWarm(ctx context.Context, listType string, userID uint64) (bool, error) {
	zsetKey := s.zsetKey(listType, userID)
	exists, err := s.redis.Exists(ctx, zsetKey).Result()
	if err == nil && exists > 0 {
		return true, nil
	}

	lock, err := s.acquireListCacheLock(ctx, listType, userID)
	if err != nil {
		return false, fmt.Errorf("ensure list cache warm: %w", err)
	}
	defer lock.Release()

	exists, err = s.redis.Exists(ctx, zsetKey).Result()
	if err == nil && exists > 0 {
		return true, nil
	}
	return s.fillZSet(ctx, listType, userID)
}

// cacheEndReached 判断当前 offset 是否已经超过预热缓存的可覆盖范围。
func (s *RelationService) cacheEndReached(ctx context.Context, zsetKey string, offset int) bool {
	size, err := s.redis.ZCard(ctx, zsetKey).Result()
	if err != nil {
		return false
	}
	if size < relationListCacheWarmLimit {
		return int64(offset) >= size
	}
	return false
}

// isBigV 判断某个用户是否是 BigV（粉丝数 >= 500）。
func (s *RelationService) isBigV(ctx context.Context, userID uint64) bool {
	key := s.zsetKey("followers", userID)
	size, err := s.redis.ZCard(ctx, key).Result()
	if err != nil {
		return false
	}
	return size >= bigVThreshold
}

// shouldFallbackToFollowing 判断是否需要从 following 表降级查询粉丝。
func (s *RelationService) shouldFallbackToFollowing(ctx context.Context, userID uint64) bool {
	key := fmt.Sprintf("follower:fallback:exhausted:%d", userID)
	exists, err := s.redis.Exists(ctx, key).Result()
	if err != nil {
		return true
	}
	return exists == 0
}

// markFollowerFallbackExhausted 标记该用户的粉丝降级查询已穷尽。
func (s *RelationService) markFollowerFallbackExhausted(ctx context.Context, userID uint64) {
	key := fmt.Sprintf("follower:fallback:exhausted:%d", userID)
	if err := s.redis.Set(ctx, key, "1", fallbackExhaustedTTL).Err(); err != nil {
		s.logger.Warn("failed to mark follower fallback exhausted", zap.String("key", key), zap.Error(err))
	}
}