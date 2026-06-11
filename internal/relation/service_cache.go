package relation

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// listEntry 是关系列表在服务层内部使用的统一读模型。
//
// repository 层分别返回 following/follower 记录，
// service 层把它们投影成同一个结构，后续缓存回填逻辑就不需要关心表差异。
type listEntry struct {
	UserID    uint64
	CreatedAt time.Time
}

// fillZSet 从数据库读取关注/粉丝列表并回填到 Redis ZSet。
//
// 预热窗口只覆盖前 relationListCacheWarmLimit 条，
// 目的是给高频前几页请求提速，而不是把完整社交图全部镜像进 Redis。
func (s *RelationService) fillZSet(ctx context.Context, listType string, userID uint64) (bool, error) {
	zsetKey := s.zsetKey(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, relationListCacheWarmLimit, 0)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}

	members := make([]redis.Z, len(entries))
	for i, entry := range entries {
		members[i] = redis.Z{
			Score:  float64(entry.CreatedAt.UnixMilli()),
			Member: strconv.FormatUint(entry.UserID, 10),
		}
	}

	if err := s.redis.ZAdd(ctx, zsetKey, members...).Err(); err != nil {
		return false, err
	}
	s.redis.Expire(ctx, zsetKey, relationListCacheTTL)
	return true, nil
}

// fillL1 只为 BigV 用户缓存前 500 条数据。
//
// L1 的目标不是完整分页承载，而是把最常访问的头部窗口直接顶到进程内内存。
func (s *RelationService) fillL1(ctx context.Context, listType string, userID uint64) {
	key := s.l1KeyStr(listType, userID)
	entries, err := s.readFromDB(ctx, listType, userID, 500, 0)
	if err != nil || len(entries) == 0 {
		return
	}
	idStrs := make([]string, len(entries))
	for i, entry := range entries {
		idStrs[i] = strconv.FormatUint(entry.UserID, 10)
	}
	if err := s.l1.Set([]byte(key), []byte(strings.Join(idStrs, ",")), 600); err != nil {
		return
	}
}

// readFromDB 根据列表类型统一读取底层关系数据。
//
// follower 表为空时会回退到 following 表反查，
// 这是为了兼容早期只有单边索引的数据。
func (s *RelationService) readFromDB(ctx context.Context, listType string, userID uint64, limit, offset int) ([]listEntry, error) {
	if listType == "following" {
		rows, err := s.repo.ListFollowingRows(ctx, userID, limit, offset)
		if err != nil {
			return nil, err
		}
		entries := make([]listEntry, len(rows))
		for i, row := range rows {
			entries[i] = listEntry{UserID: row.ToUserID, CreatedAt: row.CreatedAt}
		}
		return entries, nil
	}

	rows, err := s.repo.ListFollowerRows(ctx, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		rows, err = s.repo.ListFollowerRowsFromFollowing(ctx, userID, limit, offset)
		if err != nil {
			return nil, err
		}
	}

	entries := make([]listEntry, len(rows))
	for i, row := range rows {
		entries[i] = listEntry{UserID: row.FromUserID, CreatedAt: row.CreatedAt}
	}
	return entries, nil
}

// isBigV 判断某个用户是否需要启用 L1 头部缓存。
func (s *RelationService) isBigV(ctx context.Context, userID uint64) bool {
	key := s.zsetKey("followers", userID)
	size, err := s.redis.ZCard(ctx, key).Result()
	if err != nil {
		return false
	}
	return size >= bigVThreshold
}

func (s *RelationService) zsetKey(listType string, userID uint64) string {
	return fmt.Sprintf("z:%s:%d", listType, userID)
}

func (s *RelationService) l1KeyStr(listType string, userID uint64) string {
	return fmt.Sprintf("l1:%s:%d", listType, userID)
}

// toLongList 解析 L1 缓存里的逗号分隔 ID 字符串。
func (s *RelationService) toLongList(data string) []uint64 {
	parts := strings.Split(data, ",")
	result := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if value, err := strconv.ParseUint(part, 10, 64); err == nil {
			result = append(result, value)
		}
	}
	return result
}

// toIDList 把 Redis ZSet 成员列表转换为 uint64。
func (s *RelationService) toIDList(members []string) []uint64 {
	result := make([]uint64, 0, len(members))
	for _, member := range members {
		if value, err := strconv.ParseUint(member, 10, 64); err == nil {
			result = append(result, value)
		}
	}
	return result
}

// invalidateCaches 在关注/取关后失效受影响的关注/粉丝列表缓存。
//
// 这里只删 fromUserID 的 following 和 toUserID 的 followers，
// 因为其它两个方向在这次关系变更里没有发生任何事实变化。
func (s *RelationService) invalidateCaches(fromUserID, toUserID uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), relationInvalidateLockWaitLimit)
	defer cancel()

	targets := []relationListCacheTarget{
		{listType: "following", userID: fromUserID},
		{listType: "followers", userID: toUserID},
	}
	locks, err := s.acquireListCacheLocks(ctx, targets)
	if err == nil {
		defer func() {
			for i := len(locks) - 1; i >= 0; i-- {
				locks[i].Release()
			}
		}()
	}

	s.redis.Del(ctx, s.zsetKey("following", fromUserID))
	s.l1.Del([]byte(s.l1KeyStr("following", fromUserID)))
	s.redis.Del(ctx, s.zsetKey("followers", toUserID))
	s.l1.Del([]byte(s.l1KeyStr("followers", toUserID)))
}

// ensureListCacheWarm 在分布式锁保护下回填某个用户的列表缓存。
func (s *RelationService) ensureListCacheWarm(ctx context.Context, listType string, userID uint64) (bool, error) {
	zsetKey := s.zsetKey(listType, userID)
	exists, err := s.redis.Exists(ctx, zsetKey).Result()
	if err == nil && exists > 0 {
		return true, nil
	}

	lock, err := s.acquireListCacheLock(ctx, listType, userID)
	if err != nil {
		return false, err
	}
	defer lock.Release()

	exists, err = s.redis.Exists(ctx, zsetKey).Result()
	if err == nil && exists > 0 {
		return true, nil
	}
	return s.fillZSet(ctx, listType, userID)
}

// cacheEndReached 判断 offset 是否已经超过当前预热窗口的真实边界。
//
// 只有 ZSet 总量本身小于预热上限时，空结果才可以被解释为“真的没有更多数据”。
// 否则更可能只是请求翻到了 Redis 预热窗口之外，此时应回退 DB。
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
