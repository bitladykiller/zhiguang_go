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

// GetCounts 读取指定实体的 SDS 计数值（Hash 版本）。
func (s *CounterService) GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error) {
	sdsKey := SdsKey(entityType, entityID)

	if len(metrics) == 0 {
		return make(map[string]int32), nil
	}

	vals, err := s.redis.HMGet(ctx, sdsKey, metrics...).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis hmget: %w", err)
	}

	// 检查键是否作为 Hash 存在；若任一值为非 nil，则键有效
	allNil := true
	if err == nil {
		for _, v := range vals {
			if v != nil {
				allNil = false
				break
			}
		}
	}

	// 键不存在或完全为空 → 触发重建
	if err != nil || allNil {
		if _, rebuildErr := s.rebuildSds(ctx, entityType, entityID); rebuildErr != nil {
			return s.emptyCounts(metrics), nil
		}
		vals, err = s.redis.HMGet(ctx, sdsKey, metrics...).Result()
		if err != nil {
			return s.emptyCounts(metrics), nil
		}
	}

	result := s.parseHashValues(metrics, vals)
	return result, nil
}

func (s *CounterService) parseHashValues(metrics []string, vals []any) map[string]int32 {
	result := make(map[string]int32, len(metrics))
	for i, m := range metrics {
		if vals[i] == nil {
			result[m] = 0
		} else {
			v, ok := vals[i].(string)
			if !ok {
				result[m] = 0
			} else {
				n, convErr := parseInt32(v)
				if convErr != nil {
					result[m] = 0
				} else {
					result[m] = n
				}
			}
		}
	}
	return result
}

// parseInt32 将 Redis 返回的字符串解析为 int32。
func parseInt32(s string) (int32, error) {
	var n int32
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid number: %s", s)
		}
		n = n*10 + int32(c-'0')
	}
	return n, nil
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

// IsLikedAndFaved 通过 Pipeline 合并两组 GetBit 调用，减少一次网络往返。
func (s *CounterService) IsLikedAndFaved(ctx context.Context, userID uint64, entityType, entityID string) (liked bool, faved bool, err error) {
	chunk := ChunkOf(userID)
	offset := int64(BitOf(userID))
	likeKey := BitmapKey("like", entityType, entityID, chunk)
	favKey := BitmapKey("fav", entityType, entityID, chunk)

	pipe := s.redis.Pipeline()
	likeCmd := pipe.GetBit(ctx, likeKey, offset)
	favCmd := pipe.GetBit(ctx, favKey, offset)
	if _, err = pipe.Exec(ctx); err != nil {
		return false, false, fmt.Errorf("is liked and faved: pipeline: %w", err)
	}
	likeVal, _ := likeCmd.Result()
	favVal, _ := favCmd.Result()
	return likeVal == 1, favVal == 1, nil
}

// GetCountsBatch 使用 Redis Pipeline 批量获取多个实体的 Hash 计数。
func (s *CounterService) GetCountsBatch(ctx context.Context, entityType string, entityIDs, metrics []string) (map[string]map[string]int32, error) {
	if len(entityIDs) == 0 {
		return nil, nil
	}

	pipe := s.redis.Pipeline()
	type slot struct {
		key string
		eid string
		cmd *redis.SliceCmd
	}
	cmds := make([]slot, len(entityIDs))
	for i, eid := range entityIDs {
		k := SdsKey(entityType, eid)
		cmds[i] = slot{key: k, eid: eid, cmd: pipe.HMGet(ctx, k, metrics...)}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pipeline exec: %w", err)
	}

	result := make(map[string]map[string]int32, len(entityIDs))
	for _, sl := range cmds {
		vals, err := sl.cmd.Result()
		if err != nil || len(vals) != len(metrics) {
			continue
		}
		counts := make(map[string]int32, len(metrics))
		for i, m := range metrics {
			if vals[i] == nil {
				counts[m] = 0
			} else {
				v, ok := vals[i].(string)
				if !ok {
					counts[m] = 0
				} else {
					n, convErr := parseInt32(v)
					if convErr != nil {
						counts[m] = 0
					} else {
						counts[m] = n
					}
				}
			}
		}
		result[sl.eid] = counts
	}
	return result, nil
}