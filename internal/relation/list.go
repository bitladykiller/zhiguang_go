package relation

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// getListWithOffset reads the follow/follower list with Offset pagination (with three-level cache).
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
	exists, err := s.redis.Exists(ctx, zsetKey).Result()
	if err != nil {
		s.logger.Warn("redis exists check failed for zset cache warm", zap.String("zsetKey", zsetKey), zap.Error(err))
	}
	if exists == 0 {
		warmed, err := s.ensureListCacheWarm(ctx, listType, userID)
		if err != nil {
			return nil, fmt.Errorf("get list with offset: ensure cache warm: %w", err)
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
		return nil, fmt.Errorf("get list with offset: read from db: %w", err)
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

// getListWithCursor reads the follow/follower list with cursor-based pagination.
func (s *RelationService) getListWithCursor(ctx context.Context, userID uint64, listType string, limit int, cursor int64) ([]uint64, int64, error) {
	zsetKey := s.zsetKey(listType, userID)
	exists, err := s.redis.Exists(ctx, zsetKey).Result()
	if err != nil {
		s.logger.Warn("redis exists check failed for cursor-based list", zap.String("zsetKey", zsetKey), zap.Error(err))
	}
	if exists == 0 {
		warmed, err := s.ensureListCacheWarm(ctx, listType, userID)
		if err != nil {
			return nil, 0, err
		}
		if !warmed {
			return []uint64{}, 0, nil
		}
	}

	var maxVal string
	if cursor > 0 {
		maxVal = fmt.Sprintf("(%d", cursor)
	} else {
		maxVal = "+inf"
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
		lastID := strconv.FormatUint(result[len(result)-1], 10)
		score, err := s.redis.ZScore(ctx, zsetKey, lastID).Result()
		if err != nil {
			s.logger.Warn("failed to get zscore for cursor pagination", zap.String("zsetKey", zsetKey), zap.String("lastID", lastID), zap.Error(err))
			return result, 0, nil
		}
		nextCursor = int64(score)
	}

	return result, nextCursor, nil
}

// zsetKey generates the Redis ZSet cache key.
func (s *RelationService) zsetKey(listType string, userID uint64) string {
	return fmt.Sprintf("z:%s:%d", listType, userID)
}

// readFromDB reads the user's follow/follower list from the database.
func (s *RelationService) readFromDB(ctx context.Context, listType string, userID uint64, limit, offset int) ([]listEntry, error) {
	if listType == "following" {
		if s.repo == nil {
			return nil, fmt.Errorf("relation: repository is nil")
		}
		rows, err := s.repo.ListFollowingRows(ctx, userID, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("read from db: list following rows: %w", err)
		}
		entries := make([]listEntry, len(rows))
		for i, r := range rows {
			entries[i] = listEntry{UserID: r.ToUserID, CreatedAt: r.CreatedAt.UnixMilli()}
		}
		return entries, nil
	}
	rows, err := s.repo.ListFollowerRows(ctx, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("read from db: list follower rows: %w", err)
	}
	if len(rows) == 0 {
		if s.shouldFallbackToFollowing(ctx, userID) {
			rows, err = s.repo.ListFollowerRowsFromFollowing(ctx, userID, limit, offset)
			if err != nil {
				return nil, fmt.Errorf("read from db: list follower rows from following: %w", err)
			}
			if len(rows) == 0 {
				s.markFollowerFallbackExhausted(ctx, userID)
			}
		}
	}
	entries := make([]listEntry, len(rows))
	for i, r := range rows {
		entries[i] = listEntry{UserID: r.FromUserID, CreatedAt: r.CreatedAt.UnixMilli()}
	}
	return entries, nil
}