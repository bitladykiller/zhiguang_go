package counter

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// BatchIsLiked 批量检查用户是否对多个实体点过赞，使用一次 Pipeline 完成。
// 返回 map[entityID]bool，其中 entityID 键与输入的 entityIDs 一致。
func (s *CounterService) BatchIsLiked(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error) {
	return s.batchGetBit(ctx, userID, entityType, entityIDs, "like")
}

// BatchIsFaved 批量检查用户是否收藏了多个实体，使用一次 Pipeline 完成。
// 返回 map[entityID]bool，其中 entityID 键与输入的 entityIDs 一致。
func (s *CounterService) BatchIsFaved(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error) {
	return s.batchGetBit(ctx, userID, entityType, entityIDs, "fav")
}

func (s *CounterService) batchGetBit(ctx context.Context, userID uint64, entityType string, entityIDs []string, metric string) (map[string]bool, error) {
	if len(entityIDs) == 0 {
		return nil, nil
	}

	chunk := ChunkOf(userID)
	offset := int64(BitOf(userID))

	pipe := s.redis.Pipeline()
	cmds := make([]*redis.IntCmd, len(entityIDs))
	for i, eid := range entityIDs {
		bmKey := BitmapKey(metric, entityType, eid, chunk)
		cmds[i] = pipe.GetBit(ctx, bmKey, offset)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("batch get bit: pipeline exec: %w", err)
	}

	result := make(map[string]bool, len(entityIDs))
	for i, cmd := range cmds {
		bitValue, err := cmd.Result()
		if err != nil {
			continue
		}
		result[entityIDs[i]] = bitValue == 1
	}
	return result, nil
}
