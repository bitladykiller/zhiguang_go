package auth

import "context"

// Verify 校验用户输入的验证码是否与 Redis 中保存的一致。
//
// 执行流程:
//  1. 先获取 scene + identifier 维度的分布式锁，串行化跨实例校验流程
//  2. 在 Lua 脚本中原子检查尝试次数是否达上限 + 递增尝试次数 + 获取验证码
//  3. 比对用户输入与存储值
//  4. 校验成功后删除验证码和尝试次数（一次性使用，用完即焚）
//
// 原子化原因：
//
//	原实现先 GET 尝试次数再条件 INCR，存在 TOCTOU 竞态条件——
//	两个并发请求可同时通过上限检查，导致暴力枚举超过最大尝试次数。
//	使用 Lua 脚本将检查、递增和 EXPIRE 合为原子操作，杜绝并发绕过。
//
// 为什么这里还要再加分布式锁：
//   - codeKey 的删除动作发生在 Lua 脚本之外，原实现会出现“两个实例几乎同时读到同一个正确验证码”的并发复用。
//   - 串行化后，同一验证码在跨实例环境下只能被一个请求成功消费。
//
// 参数:
//   - ctx: 请求上下文，用于控制锁等待时长与 Redis 请求生命周期
//   - scene: 验证码场景
//   - identifier: 用户标识（手机号或邮箱）
//   - code: 用户输入的验证码
//
// 返回值:
//   - *VerificationCheckResult: 包含成功标志和状态码
//   - Success=true, Status=StatusSuccess: 验证通过
//   - Success=false, Status=StatusNotFound: 验证码不存在或已过期
//   - Success=false, Status=StatusTooManyAttempts: 尝试次数超限
//   - Success=false, Status=StatusMismatch: 验证码不匹配
//
// 边界情况:
//   - 验证码过期后（Redis TTL 到期自动删除）自动返回 StatusNotFound
//   - 达到最大尝试次数后即使输入正确验证码也拒绝校验（防暴力破解）
//   - 每次校验无论结果都递增尝试计数，但成功后会立刻删除计数键
func (s *VerificationService) Verify(ctx context.Context, scene VerificationScene, identifier, code string) *VerificationCheckResult {
	if ctx == nil {
		ctx = context.Background()
	}

	lock, err := s.acquireFlowLock(ctx, scene, identifier)
	if err != nil {
		return fail(StatusNotFound)
	}
	defer lock.Release()

	attemptKey := verificationAttemptKey(scene, identifier)
	codeKey := verificationCodeKey(scene, identifier)

	result, err := verifyAndCountScript.Run(
		ctx,
		s.redis,
		[]string{attemptKey, codeKey},
		s.config.MaxAttempts,
		int(s.config.TTL.Seconds()),
	).StringSlice()
	if err != nil {
		return fail(StatusNotFound)
	}

	// result[0] = 尝试次数（递增后），"-1" 表示已超限。
	if len(result) < 1 || result[0] == "-1" {
		return fail(StatusTooManyAttempts)
	}

	// result[1] = 验证码，空字符串表示不存在或已过期。
	if len(result) < 2 || result[1] == "" {
		return fail(StatusNotFound)
	}
	if result[1] != code {
		return fail(StatusMismatch)
	}

	s.redis.Del(ctx, codeKey, attemptKey)
	return success()
}
