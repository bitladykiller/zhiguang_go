package counter

import (
	"time"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

const (
	defaultRebuildLockTTL       = 10 * time.Second
	rebuildLockRetryInterval    = 50 * time.Millisecond
	defaultLockOperationTimeout = time.Second
)

// rebuildLockOptions 返回 SDS 重建场景使用的分布式锁配置。
//
// 配置解释：
//   - 当前 YAML 中同时存在 ttl_ms 和 watchdog_ms。
//   - 在手写看门狗模型里，真正决定“锁每次续租后还能活多久”的是 lease TTL。
//   - 为避免 watchdog_ms 大于 ttl_ms 时出现“首次加锁 5s、首次续约却要等 10s”这种提前过期，
//     这里统一取两者的较大值作为 lease TTL。
func rebuildLockOptions(cfg *config.CounterConfig) redislock.Options {
	leaseTTL := defaultRebuildLockTTL
	if cfg != nil {
		ttlMs := cfg.Rebuild.Lock.TTLMs
		watchdogMs := cfg.Rebuild.Lock.WatchdogMs
		if watchdogMs > ttlMs {
			ttlMs = watchdogMs
		}
		if ttlMs > 0 {
			leaseTTL = time.Duration(ttlMs) * time.Millisecond
		}
	}

	return redislock.Options{
		TTL:              leaseTTL,
		WatchdogInterval: leaseTTL / 3,
		OpTimeout:        defaultLockOperationTimeout,
	}
}
