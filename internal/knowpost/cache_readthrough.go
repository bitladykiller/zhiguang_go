package knowpost

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/pkg/cacheutil"
)

// cacheReadThrough 是对 cacheutil.CacheReadThrough 的便捷包装，
// 使用 knowpost 域的锁策略。
func cacheReadThrough[T any](
	ctx context.Context,
	rdb *redis.Client,
	lockKey string,
	checkCache func(ctx context.Context) (T, bool, error),
	missHandler func(ctx context.Context) (T, error),
) (T, error) {
	return cacheutil.CacheReadThrough[T](ctx, rdb, lockKey, knowPostLockOptions(), knowPostLockRetryInterval, checkCache, missHandler)
}
