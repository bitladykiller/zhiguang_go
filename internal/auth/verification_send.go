package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// SendCode 生成密码学安全的随机数字验证码，写入 Redis，并返回基础元信息。
//
// 执行流程:
//  1. 先获取 scene + identifier 维度的分布式锁，串行化跨实例发送流程
//  2. 发送间隔检查：检查上次发送与当前是否已间隔 config.SendInterval，防止短信轰炸
//  3. 每日上限检查：检查当天已发送次数是否达到 config.DailyLimit，防止单个标识被大量发送
//  4. 生成验证码：通过 crypto/rand 生成安全随机数字串
//  5. Redis Pipeline 批量写入：验证码、间隔锁
//  6. 日计数通过 Lua 原子递增并设置过期
//  7. 重置尝试计数器：新验证码意味着新的尝试配额（防暴力枚举）
//  8. 打印验证码至标准输出（开发调试用；生产环境应替换为短信/邮件渠道下发）
//
// 为什么要先加分布式锁：
//   - 原实现先 EXISTS intervalKey 再 GET dailyKey，再写 codeKey/intervalKey。
//   - 多实例下两个请求可能同时通过检查并各自发送验证码，最终只剩最后一次写入的 codeKey 生效。
//   - 加锁后同一手机号/邮箱在同一场景下的发送链路被串行化，第二个请求会在锁内看到最新 intervalKey。
//
// 参数:
//   - ctx: 请求上下文，用于控制锁等待时长与 Redis 请求生命周期
//   - scene: 验证码场景（Registration/Login/PasswordReset 等）
//   - identifier: 用户标识（手机号或邮箱）
//
// 返回值:
//   - *SendCodeResult: 包含标识符、场景和过期秒数的结果对象
//   - error: 超过每日上限返回 fmt.Errorf("daily limit exceeded")，Redis 异常也返回 error
//
// 边界情况:
//   - 在发送间隔内调用时不会生成新验证码，但返回与成功相同的结果（避免暴露限流细节给调用方）
//   - 每日上限计数键（prefixDaily）按日期后缀组织，每天凌晨自动重置
func (s *VerificationService) SendCode(ctx context.Context, scene VerificationScene, identifier string) (*SendCodeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	lock, err := s.acquireFlowLock(ctx, scene, identifier)
	if err != nil {
		return nil, err
	}
	defer lock.Release()

	intervalKey := verificationIntervalKey(scene, identifier)
	exists, err := s.redis.Exists(ctx, intervalKey).Result()
	if err != nil {
		return nil, err
	}
	if exists > 0 {
		// 对调用方保持正常返回，避免暴露限流细节。
		return s.sendCodeResult(scene, identifier), nil
	}

	dailyKey := verificationDailyKey(scene, identifier, time.Now())
	dailyCount, err := s.redis.Get(ctx, dailyKey).Int()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if dailyCount >= s.config.DailyLimit {
		return nil, fmt.Errorf("daily limit exceeded")
	}

	code := generateCode(s.config.CodeLength)
	codeKey := verificationCodeKey(scene, identifier)

	pipe := s.redis.Pipeline()
	pipe.Set(ctx, codeKey, code, s.config.TTL)
	pipe.Set(ctx, intervalKey, "1", s.config.SendInterval)
	if _, err = pipe.Exec(ctx); err != nil {
		return nil, err
	}

	// 日计数使用 Lua 脚本原子递增 + 首次设置过期，避免 INCR + EXPIRE 竞态。
	if _, err = incrAndExpireScript.Run(ctx, s.redis, []string{dailyKey}, int(24*time.Hour/time.Second)).Result(); err != nil {
		return nil, err
	}

	// 新验证码意味着新的尝试配额，因此需要清掉旧尝试计数。
	s.redis.Del(ctx, verificationAttemptKey(scene, identifier))

	// 生产环境应走短信/邮件渠道；当前先输出到标准输出便于联调。
	fmt.Printf("[VERIFICATION] Scene=%s Identifier=%s Code=%s\n", scene, identifier, code)

	return s.sendCodeResult(scene, identifier), nil
}
