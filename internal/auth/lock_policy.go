package auth

import (
	"fmt"
	"time"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

const (
	defaultAuthLockTTL           = 5 * time.Second
	defaultAuthLockWatchdog      = 15 * time.Second
	defaultAuthLockRetryInterval = 100 * time.Millisecond
)

// verificationFlowLockKey 返回验证码全链路使用的分布式锁键。
//
// WHY 以 scene + identifier 作为粒度：
//   - 我们要串行化的是“同一业务场景、同一手机号/邮箱”的验证码状态变更。
//   - SendCode 和 Verify 都会读写同一组 codeKey / intervalKey / attemptKey。
//   - 不同用户或不同场景之间不应互相阻塞。
func verificationFlowLockKey(scene VerificationScene, identifier string) string {
	return fmt.Sprintf("lock:auth:verification:send:%s:%s", scene, identifier)
}

// refreshSessionLockKey 返回用户 refresh token 会话操作使用的分布式锁键。
//
// WHY 以 userID 作为粒度：
//   - Refresh、ResetPassword.RevokeAll 本质上都在修改同一用户的 refresh token 白名单。
//   - 用 user 级别串行化，才能同时覆盖“并发 refresh”和“refresh 与全量吊销并发”两类竞态。
func refreshSessionLockKey(userID uint64) string {
	return fmt.Sprintf("lock:auth:refresh:user:%d", userID)
}

func verificationSendLockOptions(cfg *config.VerificationConfig) redislock.Options {
	if cfg == nil {
		return redislock.Options{
			TTL:              defaultAuthLockTTL,
			WatchdogInterval: defaultAuthLockWatchdog,
		}
	}
	return redislock.Options{
		TTL:              durationOrDefault(cfg.Lock.TTLMs, defaultAuthLockTTL),
		WatchdogInterval: durationOrDefault(cfg.Lock.WatchdogMs, defaultAuthLockWatchdog),
	}
}

func verificationSendRetryInterval(cfg *config.VerificationConfig) time.Duration {
	if cfg == nil {
		return defaultAuthLockRetryInterval
	}
	return durationOrDefault(cfg.Lock.RetryIntervalMs, defaultAuthLockRetryInterval)
}

func refreshSessionLockOptions(cfg *config.AuthConfig) redislock.Options {
	if cfg == nil {
		return redislock.Options{
			TTL:              defaultAuthLockTTL,
			WatchdogInterval: defaultAuthLockWatchdog,
		}
	}
	return redislock.Options{
		TTL:              durationOrDefault(cfg.Refresh.Lock.TTLMs, defaultAuthLockTTL),
		WatchdogInterval: durationOrDefault(cfg.Refresh.Lock.WatchdogMs, defaultAuthLockWatchdog),
	}
}

func refreshSessionRetryInterval(cfg *config.AuthConfig) time.Duration {
	if cfg == nil {
		return defaultAuthLockRetryInterval
	}
	return durationOrDefault(cfg.Refresh.Lock.RetryIntervalMs, defaultAuthLockRetryInterval)
}

func durationOrDefault(ms int, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}
