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
	l1CacheTTL             = 600
	fallbackExhaustedTTL   = 10 * time.Minute
	defaultFillL1Limit     = 500
)

// fillL1 writes the first 500 follow/follower IDs of a BigV user into freecache (L1).
func (s *RelationService) fillL1(ctx context.Context, listType string, userID uint64) {
	key := s.l1KeyStr(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, defaultFillL1Limit, 0)
	if err != nil || len(entries) == 0 {
		return
	}
	idStrs := make([]string, len(entries))
	for i, e := range entries {
		idStrs[i] = strconv.FormatUint(e.UserID, 10)
	}
	s.l1.Set([]byte(key), []byte(strings.Join(idStrs, ",")), l1CacheTTL)
}

// invalidateCaches invalidates L1 (freecache) and L2 (Redis ZSet) caches for the involved users after follow/unfollow operations.
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

// fillZSet reads follow/follower lists from the database and backfills them into the Redis ZSet.
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
	if err := s.redis.Expire(ctx, zsetKey, relationListCacheTTL).Err(); err != nil {
		s.logger.Warn("failed to set expire on zset", zap.String("zsetKey", zsetKey), zap.Error(err))
	}
	return true, nil
}

// ensureListCacheWarm backfills a user's follow/follower ZSet under distributed lock protection.
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

// cacheEndReached checks whether the current offset has exceeded the coverable range of the warm cache.
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

// isBigV determines whether a user is a BigV (follower count >= 500).
// Uses local L1 cache for 5 minutes to avoid querying Redis ZCard every time.
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
	bigV := size >= bigVThreshold
	val := []byte("0")
	if bigV {
		val = []byte("1")
	}
	if err := s.l1.Set([]byte(cacheKey), val, 300); err != nil {
		s.logger.Warn("L1 cache set failed", zap.String("key", cacheKey), zap.Error(err))
	}
	return bigV
}

// shouldFallbackToFollowing checks whether a fallback query from the following table for followers is needed.
func (s *RelationService) shouldFallbackToFollowing(ctx context.Context, userID uint64) bool {
	key := fmt.Sprintf("follower:fallback:exhausted:%d", userID)
	exists, err := s.redis.Exists(ctx, key).Result()
	if err != nil {
		return true
	}
	return exists == 0
}

// markFollowerFallbackExhausted marks that the follower fallback query for this user is exhausted.
func (s *RelationService) markFollowerFallbackExhausted(ctx context.Context, userID uint64) {
	key := fmt.Sprintf("follower:fallback:exhausted:%d", userID)
	if err := s.redis.Set(ctx, key, "1", fallbackExhaustedTTL).Err(); err != nil {
		s.logger.Warn("failed to mark follower fallback exhausted", zap.String("key", key), zap.Error(err))
	}
}