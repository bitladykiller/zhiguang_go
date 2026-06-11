package counter

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func (s *CounterService) GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error) {
	sdsKey := SdsKey(entityType, entityID)

	raw, err := s.redis.Get(ctx, sdsKey).Bytes()
	if err == redis.Nil || len(raw) != SchemaLen*FieldSize {
		raw, err = s.rebuildSds(ctx, entityType, entityID)
		if err != nil {
			return s.emptyCounts(metrics), nil
		}
		if len(raw) != SchemaLen*FieldSize {
			return s.emptyCounts(metrics), nil
		}
	} else if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}

	result := make(map[string]int32, len(metrics))
	for _, m := range metrics {
		idx, ok := NameToIdx[m]
		if !ok {
			continue
		}
		result[m] = readInt32BE(raw, idx*FieldSize)
	}
	return result, nil
}

func (s *CounterService) IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey("like", entityType, entityID, chunk)
	val, err := s.redis.GetBit(ctx, bmKey, int64(offset)).Result()
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

func (s *CounterService) IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey("fav", entityType, entityID, chunk)
	val, err := s.redis.GetBit(ctx, bmKey, int64(offset)).Result()
	if err != nil {
		return false, err
	}
	return val == 1, nil
}

func (s *CounterService) GetCountsBatch(ctx context.Context, entityType string, entityIDs, metrics []string) (map[string]map[string]int32, error) {
	if len(entityIDs) == 0 {
		return nil, nil
	}

	keys := make([]string, len(entityIDs))
	keyToEID := make(map[string]string, len(entityIDs))
	for i, eid := range entityIDs {
		k := SdsKey(entityType, eid)
		keys[i] = k
		keyToEID[k] = eid
	}

	pipe := s.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Get(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	result := make(map[string]map[string]int32, len(entityIDs))
	for i, cmd := range cmds {
		raw, err := cmd.Bytes()
		if err != nil || len(raw) != SchemaLen*FieldSize {
			continue
		}
		counts := make(map[string]int32, len(metrics))
		for _, m := range metrics {
			idx, ok := NameToIdx[m]
			if !ok {
				continue
			}
			counts[m] = readInt32BE(raw, idx*FieldSize)
		}
		result[keyToEID[keys[i]]] = counts
	}
	return result, nil
}

func (s *CounterService) emptyCounts(metrics []string) map[string]int32 {
	m := make(map[string]int32, len(metrics))
	for _, k := range metrics {
		m[k] = 0
	}
	return m
}
