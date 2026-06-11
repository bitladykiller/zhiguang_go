package relation

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Following 返回 userID 关注的人列表，使用 offset 分页。
func (s *RelationService) Following(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.getListWithOffset(ctx, userID, "following", limit, offset)
}

// Followers 返回粉丝列表，使用 offset 分页。
func (s *RelationService) Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.getListWithOffset(ctx, userID, "followers", limit, offset)
}

// FollowingCursor 返回基于游标分页的关注列表。
func (s *RelationService) FollowingCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.getListWithCursor(ctx, userID, "following", limit, cursor)
}

// FollowersCursor 返回基于游标分页的粉丝列表。
func (s *RelationService) FollowersCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.getListWithCursor(ctx, userID, "followers", limit, cursor)
}

// getListWithOffset 使用 L1/L2/L3 三级链路读取列表。
//
// 读路径顺序：
//   - BigV 先读 L1 freecache；
//   - 再读 Redis ZSet；
//   - 缓存缺失或深页越界时回退 MySQL。
//
// 这样设计的原因是：
//   - 热点大 V 用户需要尽量绕开 Redis；
//   - 普通用户的前几页由 ZSet 承接；
//   - 深分页不强行塞进 Redis 预热窗口，直接回 DB 更可控。
func (s *RelationService) getListWithOffset(ctx context.Context, userID uint64, listType string, limit, offset int) ([]uint64, error) {
	if s.isBigV(ctx, userID) {
		l1Key := s.l1KeyStr(listType, userID)
		if data, err := s.l1.Get([]byte(l1Key)); err == nil {
			ids := s.toLongList(string(data))
			if offset < len(ids) {
				end := offset + limit
				if end > len(ids) {
					end = len(ids)
				}
				return ids[offset:end], nil
			}
		}
	}

	zsetKey := s.zsetKey(listType, userID)
	exists, _ := s.redis.Exists(ctx, zsetKey).Result()
	if exists == 0 {
		warmed, err := s.ensureListCacheWarm(ctx, listType, userID)
		if err != nil {
			return nil, err
		}
		if !warmed {
			return []uint64{}, nil
		}
		if s.isBigV(ctx, userID) {
			s.fillL1(ctx, listType, userID)
		}
	}

	members, err := s.redis.ZRevRange(ctx, zsetKey, int64(offset), int64(offset+limit-1)).Result()
	if err == nil && len(members) > 0 {
		return s.toIDList(members), nil
	}
	if s.cacheEndReached(ctx, zsetKey, offset) {
		return []uint64{}, nil
	}

	rows, err := s.readFromDB(ctx, listType, userID, limit+offset, 0)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(rows))
	for _, entry := range rows {
		ids = append(ids, entry.UserID)
	}
	if offset >= len(ids) {
		return []uint64{}, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], nil
}

// getListWithCursor 通过 Redis ZSet 的 score 实现稳定游标分页。
//
// score 使用 created_at 毫秒时间戳。只要同一用户列表内的时间顺序稳定，
// 就能避免 offset 翻页在数据变动时出现跳页和重复。
func (s *RelationService) getListWithCursor(ctx context.Context, userID uint64, listType string, limit int, cursor int64) ([]uint64, int64, error) {
	zsetKey := s.zsetKey(listType, userID)
	exists, _ := s.redis.Exists(ctx, zsetKey).Result()
	if exists == 0 {
		warmed, err := s.ensureListCacheWarm(ctx, listType, userID)
		if err != nil {
			return nil, 0, err
		}
		if !warmed {
			return []uint64{}, 0, nil
		}
	}

	maxVal := "+inf"
	if cursor > 0 {
		maxVal = fmt.Sprintf("(%d", cursor)
	}

	members, err := s.redis.ZRevRangeByScore(ctx, zsetKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    maxVal,
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, 0, err
	}

	result := s.toIDList(members)
	var nextCursor int64
	if len(result) > 0 {
		lastID := fmt.Sprintf("%d", result[len(result)-1])
		score, _ := s.redis.ZScore(ctx, zsetKey, lastID).Result()
		nextCursor = int64(score)
	}

	return result, nextCursor, nil
}
