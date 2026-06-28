// Package counter — SDS 重建逻辑。
//
// 本文件包含从位图重建 SDS 快照的完整流程：
//   - rebuildSds：带退避 + 限流 + 分布式锁保护的重建入口
//   - bitCountShards：按指标统计所有位图片段的 BITCOUNT
//   - buildSnapshotFromBitmap：遍历所有指标构建完整 SDS 字节数组
//
// 位图是权威数据源，SDS 是通过聚合位图得到的正式快照。
// 正常情况下快照由 Kafka 批量消费链路持续维护；
// 只有缺失、损坏或异常漂移时才回退到位图重建。
package counter

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"github.com/zhiguang/app/pkg/redislock"
	"golang.org/x/sync/errgroup"
)

// rebuildSds 从位图重建 SDS 计数。
//
// 重建流程（带退避 + 限流 + 分布式锁保护）：
//  1. 检查是否处于退避期（inBackoff），如果是则拒绝重建。
//  2. 检查是否超过限流水位（allowedByRateLimiter），否则提升退避等级并拒绝。
//  3. 通过 Redis 看门狗分布式锁获取重建权限，防止多实例同时重建同一 SDS。
//     抢到锁后会再次 double-check SDS，避免前一个实例刚刚重建成功时重复查位图。
//  4. 遍历所有指标，对每个指标调用 bitCountShards 汇总所有位图片段的 BITCOUNT 值。
//  5. 将汇总结果写入 SDS 字节数组并写回 Redis。
//  6. 释放锁并重置退避状态。
func (s *CounterService) rebuildSds(ctx context.Context, entityType, entityID string) (map[string]int32, error) {
	sdsKey := SdsKey(entityType, entityID)

	if s.inBackoff(ctx, entityType, entityID) {
		return nil, fmt.Errorf("in backoff")
	}

	if !s.allowedByRateLimiter(ctx, entityType, entityID) {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("rate limited")
	}

	lockKey := fmt.Sprintf("lock:sds-rebuild:%s:%s", entityType, entityID)
	lock, err := redislock.AcquireWithRetry(ctx, s.redis, lockKey, s.rebuildLockOptions, rebuildLockRetryInterval, s.logger)
	if err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("acquire rebuild lock: %w", err)
	}
	defer lock.Release()

	// double-check：可能在当前请求等待锁期间，前一个实例已经完成了重建。
	snapshot, err := s.redis.HGetAll(ctx, sdsKey).Result()
	if err == nil && len(snapshot) > 0 {
		s.resetBackoff(ctx, entityType, entityID)
		counts := make(map[string]int32, len(snapshot))
		for k, v := range snapshot {
			n, convErr := parseInt32(v)
			if convErr == nil {
				counts[k] = n
			}
		}
		return counts, nil
	}

	snapshotData, err := s.buildSnapshotFromBitmap(ctx, entityType, entityID)
	if err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("rebuild sds: build snapshot: %w", err)
	}

	mapHSet := make(map[string]any, len(snapshotData))
	for k, v := range snapshotData {
		mapHSet[k] = v
	}
	if err := s.redis.HSet(ctx, sdsKey, mapHSet).Err(); err != nil {
		s.escalateBackoff(ctx, entityType, entityID)
		return nil, fmt.Errorf("rebuild sds: hset: %w", err)
	}

	s.resetBackoff(ctx, entityType, entityID)
	return snapshotData, nil
}

// bitCountShards 统计指定指标的所有位图片段的 SETBIT 总数量。
//
// 使用 Redis SCAN 命令迭代匹配模式 `bm:{metric}:{entityType}:{entityID}:*`，
// 对每个匹配的位图键执行 BITCOUNT，通过 Pipeline 批量发送并汇总。
func (s *CounterService) bitCountShards(ctx context.Context, metric, entityType, entityID string) (int64, error) {
	pattern := fmt.Sprintf("bm:%s:%s:%s:*", metric, entityType, entityID)

	var total int64
	var cursor uint64
	for {
		keys, next, err := s.redis.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return 0, fmt.Errorf("bit count shards: scan: %w", err)
		}
		if len(keys) > 0 {
			pipe := s.redis.Pipeline()
			cmds := make([]*redis.IntCmd, len(keys))
			for i, k := range keys {
				cmds[i] = pipe.BitCount(ctx, k, nil)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return 0, fmt.Errorf("bit count shards: pipeline exec: %w", err)
			}
			for i, cmd := range cmds {
				val, err := cmd.Result()
				if err != nil {
					s.logger.Warn("bitcount shard failed", zap.String("key", keys[i]), zap.Error(err))
					continue
				}
				total += val
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return total, nil
}

// buildSnapshotFromBitmap 遍历所有指标，从位图构建完整快照并写入 Redis Hash。
func (s *CounterService) buildSnapshotFromBitmap(ctx context.Context, entityType, entityID string) (map[string]int32, error) {
	snapshot := make(map[string]int32, SchemaLen)
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(5)
	for i := 0; i < SchemaLen; i++ {
		idx := i
		metric := indexToName[idx]
		g.Go(func() error {
			total, err := s.bitCountShards(ctx, metric, entityType, entityID)
			if err != nil {
				return fmt.Errorf("build snapshot: bit count %s: %w", metric, err)
			}
			mu.Lock()
			snapshot[metric] = int32(total)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return snapshot, nil
}