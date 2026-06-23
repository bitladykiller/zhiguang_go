// Package counter  — SDS 二进制序列化和读操作。
//
// 本文件包含 SDS（Serial Data Structure）的编解码辅助函数和读操作：
//   - readInt32BE / writeInt32BE：大端序 int32 编解码
//   - GetCounts / IsLiked / IsFaved / GetCountsBatch：基础读操作
//
// 重建逻辑已拆分到 sds_rebuild.go。
package counter

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// readInt32BE 从字节数组中按大端序读取 int32 值。
func readInt32BE(b []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(b[offset:]))
}

// writeInt32BE 将 int32 值按大端序写入字节数组的指定偏移位置。
func writeInt32BE(b []byte, offset int, val int32) {
	binary.BigEndian.PutUint32(b[offset:], uint32(val))
}

// emptyCounts 为请求的指标列表生成全零的计数值映射。
func (s *CounterService) emptyCounts(metrics []string) map[string]int32 {
	m := make(map[string]int32, len(metrics))
	for _, k := range metrics {
		m[k] = 0
	}
	return m
}

// GetCounts 读取指定实体的 SDS 计数值。
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
		idx, ok := nameToIdx[m]
		if !ok {
			continue
		}
		result[m] = readInt32BE(raw, idx*FieldSize)
	}
	return result, nil
}

// IsLiked 判断指定用户是否已给该实体点赞。
func (s *CounterService) IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey("like", entityType, entityID, chunk)
	val, err := s.redis.GetBit(ctx, bmKey, int64(offset)).Result()
	if err != nil {
		return false, fmt.Errorf("is liked: getbit: %w", err)
	}
	return val == 1, nil
}

// IsFaved 判断指定用户是否已收藏该实体。
func (s *CounterService) IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey("fav", entityType, entityID, chunk)
	val, err := s.redis.GetBit(ctx, bmKey, int64(offset)).Result()
	if err != nil {
		return false, fmt.Errorf("is faved: getbit: %w", err)
	}
	return val == 1, nil
}

// GetCountsBatch 使用 Redis Pipeline 批量获取多个实体的 SDS 计数。
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
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pipeline exec: %w", err)
	}

	result := make(map[string]map[string]int32, len(entityIDs))
	for i, cmd := range cmds {
		raw, err := cmd.Bytes()
		if err != nil || len(raw) != SchemaLen*FieldSize {
			continue
		}
		counts := make(map[string]int32, len(metrics))
		for _, m := range metrics {
			idx, ok := nameToIdx[m]
			if !ok {
				continue
			}
			counts[m] = readInt32BE(raw, idx*FieldSize)
		}
		result[keyToEID[keys[i]]] = counts
	}
	return result, nil
}
