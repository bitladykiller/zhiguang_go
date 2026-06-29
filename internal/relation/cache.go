package relation

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// fillL1 将 BigV 用户的前 fillLimit 个关注/粉丝 ID 写入 freecache（L1）。
func (s *RelationService) fillL1(ctx context.Context, listType string, userID uint64) {
	key := s.l1KeyStr(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, relationFillL1Limit(s.cfg), 0)
	if err != nil || len(entries) == 0 {
		return
	}
	idStrs := make([]string, len(entries))
	for i, e := range entries {
		idStrs[i] = strconv.FormatUint(e.UserID, 10)
	}
	s.l1.Set([]byte(key), []byte(strings.Join(idStrs, ",")), relationL1CacheTTL(s.cfg))
}

// invalidateCaches 在关注/取关操作后，使涉及的用户的 L1（freecache）和 L2（Redis ZSet）缓存失效。
func (s *RelationService) invalidateCaches(ctx context.Context, fromUserID, toUserID uint64) {
	cacheCtx, cancel := context.WithTimeout(ctx, relationInvalidateLockWaitLimit(s.cfg))
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

	pipe := s.redis.Pipeline()
	pipe.Del(cacheCtx, s.zsetKey("following", fromUserID))
	pipe.Del(cacheCtx, s.zsetKey("followers", toUserID))
	pipe.Del(cacheCtx, fmt.Sprintf("follower:fallback:exhausted:%d", toUserID))
	if _, err := pipe.Exec(cacheCtx); err != nil {
		s.logger.Warn("failed to invalidate caches via pipeline", zap.Error(err))
	}
	s.l1.Del([]byte(s.l1KeyStr("following", fromUserID)))
	s.l1.Del([]byte(s.l1KeyStr("followers", toUserID)))
}

// fillZSet 从数据库读取关注/粉丝列表并回填到 Redis ZSet 中。
func (s *RelationService) fillZSet(ctx context.Context, listType string, userID uint64) (bool, error) {
	zsetKey := s.zsetKey(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, relationListCacheWarmLimit(s.cfg), 0)
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

	pipe := s.redis.Pipeline()
	pipe.ZAdd(ctx, zsetKey, members...)
	pipe.Expire(ctx, zsetKey, relationListCacheTTL(s.cfg))
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("fill zset: pipeline exec: %w", err)
	}
	return true, nil
}

// ensureListCacheWarm 在分布式锁保护下回填用户的关注/粉丝 ZSet。
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
	if lock != nil {
		defer lock.Release()
	}

	exists, err = s.redis.Exists(ctx, zsetKey).Result()
	if err == nil && exists > 0 {
		return true, nil
	}
	return s.fillZSet(ctx, listType, userID)
}

// cacheEndReached 检查当前 offset 是否已超过暖缓存的可覆盖范围。
func (s *RelationService) cacheEndReached(ctx context.Context, zsetKey string, offset int) bool {
	size, err := s.redis.ZCard(ctx, zsetKey).Result()
	if err != nil {
		return false
	}
	if size < int64(relationListCacheWarmLimit(s.cfg)) {
		return int64(offset) >= size
	}
	return false
}

// isBigV 判断用户是否为 BigV（粉丝数 >= 500）。
func (s *RelationService) isBigV(ctx context.Context, userID uint64) bool {
	cacheKey := fmt.Sprintf("bigv:%d", userID)
	if data, err := s.l1.Get([]byte(cacheKey)); err == nil && len(data) > 0 {
		return string(data) == "1"
	}

	key := s.zsetKey("followers", userID)
	size, err := s.redis.ZCard(ctx, key).Result()
	if err != nil {
		return false
	}
	bigV := size >= s.bigVThreshold
	val := []byte("0")
	if bigV {
		val = []byte("1")
	}
	if err := s.l1.Set([]byte(cacheKey), val, relationL1CacheTTL(s.cfg)); err != nil {
		s.logger.Warn("L1 cache set failed", zap.String("key", cacheKey), zap.Error(err))
	}
	return bigV
}

// shouldFallbackToFollowing 检查是否需要从 following 表降级查询粉丝。
func (s *RelationService) shouldFallbackToFollowing(ctx context.Context, userID uint64) bool {
	key := fmt.Sprintf("follower:fallback:exhausted:%d", userID)
	exists, err := s.redis.Exists(ctx, key).Result()
	if err != nil {
		return true
	}
	return exists == 0
}

// markFollowerFallbackExhausted 标记该用户的粉丝降级查询已耗尽。
func (s *RelationService) markFollowerFallbackExhausted(ctx context.Context, userID uint64) {
	key := fmt.Sprintf("follower:fallback:exhausted:%d", userID)
	if err := s.redis.Set(ctx, key, "1", relationFallbackExhaustedTTL(s.cfg)).Err(); err != nil {
		s.logger.Warn("failed to mark follower fallback exhausted", zap.String("key", key), zap.Error(err))
	}
}
