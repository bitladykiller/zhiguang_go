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
	defaultTTL       = 5 * time.Second
	defaultOpTimeout = time.Second
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
	client   *redis.Client
	lockKey  string
	token    string
	options  Options
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
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

// Acquire 尝试获取一个带看门狗续租能力的 Redis 分布式锁。
//
// 返回值：
//   - *Lock: 抢锁成功时返回锁对象
//   - bool: 是否成功抢到锁
//   - error: Redis 操作失败时返回错误
func Acquire(ctx context.Context, client *redis.Client, lockKey string, options Options) (*Lock, bool, error) {
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
				// Redis 短暂抖动时继续下一轮续约，避免瞬时失败直接放弃租期。
				continue
			}
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
