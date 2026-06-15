// Package redislock 提供基于 go-redis 的可复用分布式锁实现。
//
// 当前实现特性：
//   - 使用 SET NX PX 抢锁
//   - 使用随机 token 标识锁归属
//   - 使用 Lua 脚本做安全续约与安全释放
//   - 内置手写看门狗，持锁期间周期性续租
package redislock

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTTL           = 5 * time.Second
	defaultOpTimeout     = time.Second
	defaultRetryInterval = 50 * time.Millisecond
)

// Options 定义分布式锁的行为参数。
type Options struct {
	// TTL 是单次加锁成功后的初始租期。
	TTL time.Duration

	// WatchdogInterval 是看门狗续租周期。
	// 若未设置，则默认使用 TTL/3。
	WatchdogInterval time.Duration

	// OpTimeout 控制续约、释放等单次 Redis 操作的超时。
	OpTimeout time.Duration

	// OnRenewFailed 是看门狗续约失败时的回调函数。
	// 可用于记录日志、发送告警或主动放弃操作。
	// 注意：这是"通知"机制，不是"解决"锁过期。业务操作仍需在 TTL 内完成或幂等。
	OnRenewFailed func(err error)

	// MaxRenewErrors 是连续续约失败多少次后停止看门狗。
	// 0 表示不限制（一直重试直到锁释放）。
	// 设置为 N 表示连续失败 N 次后看门狗退出，锁会自然过期。
	MaxRenewErrors int
}

// normalized 返回补齐默认值后的配置。
func (o Options) normalized() Options {
	if o.TTL <= 0 {
		o.TTL = defaultTTL
	}
	if o.WatchdogInterval <= 0 {
		o.WatchdogInterval = o.TTL / 3
	}
	if o.OpTimeout <= 0 {
		o.OpTimeout = defaultOpTimeout
	}
	return o
}

// Lock 表示一个已经成功获取的 Redis 分布式锁。
//
// WHY 使用对象而不是只返回 token：
//   - 续约 goroutine、停止信号、归属 token 和 Redis 客户端需要一起管理。
//   - 把这些生命周期状态收敛到一个对象里，调用方只需要 `Release()` 即可。
type Lock struct {
	client        *redis.Client
	lockKey       string
	token         string
	options       Options
	stopCh        chan struct{}
	doneCh        chan struct{}
	stopOnce      sync.Once
	renewErrCount int // 连续续约失败计数
}

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// TryAcquire 尝试获取一个带看门狗续租能力的 Redis 分布式锁。
//
// 返回值：
//   - *Lock: 抢锁成功时返回锁对象
//   - bool: 是否成功抢到锁
//   - error: Redis 操作失败时返回错误
//
// 语义说明：
//   - 这是一次性的 try-lock，不会在包内部自旋等待。
//   - 如果调用方需要“直到拿到锁或 ctx 结束”为止的语义，应使用 AcquireWithRetry。
func TryAcquire(ctx context.Context, client *redis.Client, lockKey string, options Options) (*Lock, bool, error) {
	opts := options.normalized()
	token := uuid.NewString()

	locked, err := client.SetNX(ctx, lockKey, token, opts.TTL).Result()
	if err != nil || !locked {
		return nil, locked, err
	}

	lock := &Lock{
		client:  client,
		lockKey: lockKey,
		token:   token,
		options: opts,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	go lock.watchdog()
	return lock, true, nil
}

// AcquireWithRetry 持续重试直到成功获取锁或 ctx 结束。
//
// 使用场景：
//   - 调用方确实需要阻塞等待锁，而不是一次 try-lock 后自己决定后续逻辑。
//   - 例如某些串行化写操作、后台任务抢占等场景。
//
// 注意：
//   - 这里仅对“锁被占用（locked=false）”做重试。
//   - 如果 Redis 自身返回错误，则直接返回给调用方，由上层决定是否降级或告警。
//   - retryInterval <= 0 时，默认使用 50ms。
func AcquireWithRetry(
	ctx context.Context,
	client *redis.Client,
	lockKey string,
	options Options,
	retryInterval time.Duration,
) (*Lock, error) {
	interval := retryInterval
	if interval <= 0 {
		interval = defaultRetryInterval
	}

	for {
		lock, locked, err := TryAcquire(ctx, client, lockKey, options)
		if err != nil {
			return nil, err
		}
		if locked {
			return lock, nil
		}
		if !sleepRetry(ctx, interval) {
			return nil, ctx.Err()
		}
	}
}

// Release 停止看门狗并在 token 匹配时安全释放锁。
func (l *Lock) Release() {
	if l == nil {
		return
	}

	l.stopWatchdog()

	releaseCtx, cancel := context.WithTimeout(context.Background(), l.options.OpTimeout)
	defer cancel()
	_, _ = releaseScript.Run(releaseCtx, l.client, []string{l.lockKey}, l.token).Result()
}

// watchdog 在业务仍持有锁期间周期性续租。
func (l *Lock) watchdog() {
	ticker := time.NewTicker(l.options.WatchdogInterval)
	defer ticker.Stop()
	defer close(l.doneCh)

	for {
		select {
		case <-l.stopCh:
			return
		case <-ticker.C:
			renewed, err := l.renew()
			if err != nil {
				// 连续续约失败计数
				l.renewErrCount++
				// 调用回调通知业务层
				if l.options.OnRenewFailed != nil {
					l.options.OnRenewFailed(err)
				}
				// 如果设置了最大失败次数且达到阈值，停止看门狗
				if l.options.MaxRenewErrors > 0 && l.renewErrCount >= l.options.MaxRenewErrors {
					return
				}
				// Redis 短暂抖动时继续下一轮续约，避免瞬时失败直接放弃租期。
				continue
			}
			// 续约成功，重置失败计数
			l.renewErrCount = 0
			if !renewed {
				// token 已不匹配，说明锁已过期或已被其他实例获取。
				return
			}
		}
	}
}

// renew 仅在 token 仍归当前持锁方时刷新 TTL。
func (l *Lock) renew() (bool, error) {
	renewCtx, cancel := context.WithTimeout(context.Background(), l.options.OpTimeout)
	defer cancel()

	result, err := renewScript.Run(
		renewCtx,
		l.client,
		[]string{l.lockKey},
		l.token,
		l.options.TTL.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}

	return result == 1, nil
}

// stopWatchdog 保证看门狗只停止一次。
func (l *Lock) stopWatchdog() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
		<-l.doneCh
	})
}

func sleepRetry(ctx context.Context, retryInterval time.Duration) bool {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
