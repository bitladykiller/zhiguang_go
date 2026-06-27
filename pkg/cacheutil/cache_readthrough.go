// Package cacheutil 提供缓存穿透防护的通用「锁 → 双重检查缓存 → 回源」模式。
//
// 最初从 knowpost 包中抽取，现在被 knowpost（详情/Feed 读取）和 relation（缓存回源）共享。
//
// 核心函数 cacheReadThrough 封装了完整的回源流程：
//  1. 通过 Redis 分布式锁（TryAcquire + 自动续约）防止缓存击穿
//  2. 抢锁失败时回查缓存（可能被其他实例回填）
//  3. 抢锁成功后双重检查缓存，避免重复查库
//  4. 缓存仍未命中时调用 missHandler 执行回源
package cacheutil

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/contextutil"
	"github.com/zhiguang/app/pkg/redislock"
)

// CacheReadThrough 封装「锁 → 双重检查缓存 → 回源」的通用模式。
//
// 用于任何需要防止缓存击穿的读取场景——详情读取、Feed 读取、关系列表读取等。
//
// 类型参数：
//   - T: 缓存值类型（如 *KnowPostDetailResponse、*FeedPageResponse 等）
//
// 参数：
//   - ctx: 请求上下文
//   - rdb: Redis 客户端
//   - lockKey: 分布式锁的 key
//   - lockOpts: 锁策略（TTL、续约间隔、操作超时）
//   - retryInterval: 抢锁失败后的重试等待时间
//   - checkCache: 检查缓存是否命中，返回 (result, hit, error)
//   - missHandler: 缓存未命中时的回源逻辑，返回 (result, error)
//
// 返回值：
//   - T: 缓存命中或回源得到的结果
//   - error: 锁获取失败、缓存检查失败或回源失败时返回
//
// 锁策略：
//   - 使用 TryAcquire 非阻塞抢锁，带有自动续约（watchdog）
//   - 抢锁失败时先检查缓存（可能已被其他实例回填），命中则直接返回
//   - 抢锁成功后再次检查缓存（双重检查），命中则释放锁返回
//   - 仍未命中则调用 missHandler 回源
//
// 使用示例：
//
//	result, err := cacheutil.CacheReadThrough(ctx, rdb, lockKey, lockOpts, retryInterval,
//	    func(ctx context.Context) (*MyType, bool, error) {
//	        // 检查 L2 缓存
//	    },
//	    func(ctx context.Context) (*MyType, error) {
//	        // 回源 DB
//	    },
//	)
func CacheReadThrough[T any](
	ctx context.Context,
	rdb *redis.Client,
	lockKey string,
	lockOpts redislock.Options,
	retryInterval time.Duration,
	checkCache func(ctx context.Context) (T, bool, error),
	missHandler func(ctx context.Context) (T, error),
) (T, error) {
	for {
		lock, locked, err := redislock.TryAcquire(ctx, rdb, lockKey, lockOpts)
		if err != nil {
			var zero T
			return zero, err
		}

		if !locked {
			result, hit, err := checkCache(ctx)
			if err != nil {
				var zero T
				return zero, err
			}
			if hit {
				return result, nil
			}
			if !contextutil.Sleep(ctx, retryInterval) {
				var zero T
				return zero, ctx.Err()
			}
			continue
		}

		result, hit, err := checkCache(ctx)
		if err != nil {
			lock.Release()
			var zero T
			return zero, err
		}
		if hit {
			lock.Release()
			return result, nil
		}

		ret, err := missHandler(ctx)
		lock.Release()
		return ret, err
	}
}
