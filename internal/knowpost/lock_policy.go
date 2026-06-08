package knowpost

import (
	"context"
	"time"

	"github.com/zhiguang/app/pkg/redislock"
)

const (
	knowPostLockTTL           = 5 * time.Second
	knowPostLockRetryInterval = 50 * time.Millisecond
	knowPostLockOpTimeout     = time.Second
)

// knowPostLockOptions 返回 knowpost 缓存回源场景使用的分布式锁策略。
//
// WHY 把策略留在业务域内：
//   - 锁实现本身属于可复用基础设施，应放在 pkg。
//   - 但租期、续约频率、操作超时这些参数仍然是业务权衡，
//     由 knowpost 自己决定更清晰。
//   - knowpost 读取链路不会直接使用 AcquireWithRetry，
//     因为它在每次抢锁失败后还要先回查缓存，再决定是否继续等待。
func knowPostLockOptions() redislock.Options {
	return redislock.Options{
		TTL:              knowPostLockTTL,
		WatchdogInterval: knowPostLockTTL / 3,
		OpTimeout:        knowPostLockOpTimeout,
	}
}

func sleepDistributedLockRetry(ctx context.Context) bool {
	timer := time.NewTimer(knowPostLockRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
