package counter

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// BatchIsLiked checks if a user liked multiple entities in one pipeline.
// Returns map[entityID]bool where entityID keys match the entityIDs input.
func (s *CounterService) BatchIsLiked(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error) {
	return s.batchGetBit(ctx, userID, entityType, entityIDs, "like")
}

// BatchIsFaved checks if a user faved multiple entities in one pipeline.
// Returns map[entityID]bool where entityID keys match the entityIDs input.
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
		return nil, err
	}

	result := make(map[string]bool, len(entityIDs))
	for i, cmd := range cmds {
		val, err := cmd.Result()
		if err != nil {
			continue
		}
		result[entityIDs[i]] = val == 1
	}
	return result, nil
}
