package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
	"go.uber.org/zap"
)

// 验证码相关 Redis 键前缀。
//
// 每种类型使用独立前缀，避免不同场景的验证码键冲突：
//   - code：存储实际验证码值
//   - interval：发送间隔锁，防止短时间内重复发送
//   - daily：每天发送次数计数
//   - attempts：验证尝试次数，用于暴力破解防护
const (
	prefixCode     = "vc:code:"
	prefixInterval = "vc:interval:"
	prefixDaily    = "vc:daily:"
	prefixAttempts = "vc:attempts:"
)

// VerificationService 负责管理验证码的完整生命周期。
//
// 功能特性：
//   - 生成具备密码学安全性的随机数字验证码（crypto/rand）
//   - Redis 存储 + TTL 自动过期
//   - 发送间隔限制：防止短信接口被频繁调用造成资损
//   - 每日发送上限：防止单个标识被大量发送验证码
//   - 验证尝试次数限制：防暴力枚举
type VerificationService struct {
	redis             *redis.Client
	config            *config.VerificationConfig
	sendLockOptions   redislock.Options
	sendLockRetryWait time.Duration
	operationTimeout  time.Duration
	logger            *zap.Logger
}

// NewVerificationService 创建验证码服务实例。
//
// 参数:
//   - redisClient: Redis 客户端连接，用于验证码的存储、过期和计数
//   - cfg: 验证码配置，包含有效期、发送间隔、每日上限、验证码长度、最大尝试次数
//
// 返回值:
//   - *VerificationService: 验证码服务实例
func NewVerificationService(redisClient *redis.Client, cfg *config.VerificationConfig, logger *zap.Logger) *VerificationService {
	if logger == nil {
		logger = zap.L()
	}
	return &VerificationService{
		redis:             redisClient,
		config:            cfg,
		sendLockOptions:   verificationSendLockOptions(cfg),
		sendLockRetryWait: verificationSendRetryInterval(cfg),
		operationTimeout:  verificationOperationTimeout(cfg),
		logger:            logger,
	}
}

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
	var cancel context.CancelFunc
	ctx, cancel = s.wrapWithTimeout(ctx)
	defer cancel()

	lock, err := redislock.AcquireWithRetry(
		ctx,
		s.redis,
		verificationFlowLockKey(scene, identifier),
		s.sendLockOptions,
		s.sendLockRetryWait,
		s.logger,
	)
	if err != nil {
		return nil, fmt.Errorf("send code: acquire lock: %w", err)
	}
	defer lock.Release()

	// 检查发送间隔，防止短信轰炸
	intervalKey := fmt.Sprintf("%s%s:%s", prefixInterval, scene, identifier)
	exists, err := s.redis.Exists(ctx, intervalKey).Result()
	if err != nil {
		return nil, fmt.Errorf("send code: check interval: %w", err)
	}
	if exists > 0 {
		// 对调用方保持正常返回，避免暴露限流细节
		return &SendCodeResult{Identifier: identifier, Scene: scene, ExpireSeconds: int(s.config.TTL.Seconds())}, nil
	}

	// 检查每日发送上限
	dailyKey := fmt.Sprintf("%s%s:%s:%s", prefixDaily, scene, identifier, time.Now().Format("20060102"))
	dailyCount, err := s.redis.Get(ctx, dailyKey).Int()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("send code: get daily count: %w", err)
	}
	if dailyCount >= s.config.DailyLimit {
		return nil, fmt.Errorf("daily limit exceeded")
	}

	// 生成验证码
	code := generateCode(s.config.CodeLength, s.logger)

	// 通过 pipeline 批量写入 Redis（验证码 + 间隔锁 + 日计数原子递增）
	codeKey := fmt.Sprintf("%s%s:%s", prefixCode, scene, identifier)
	pipe := s.redis.Pipeline()
	pipe.Set(ctx, codeKey, code, s.config.TTL)
	pipe.Set(ctx, intervalKey, "1", s.config.SendInterval)

	if _, err = pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("send code: pipeline exec: %w", err)
	}

	// 日计数使用 Lua 脚本原子递增 + 首次设置过期，避免 INCR + EXPIRE 竞态
	if _, err = incrAndExpireScript.Run(ctx, s.redis, []string{dailyKey}, int(24*time.Hour/time.Second)).Result(); err != nil {
		return nil, fmt.Errorf("send code: incr daily: %w", err)
	}

	// 重置尝试次数计数器（新验证码意味着新的尝试配额）
	attemptKey := fmt.Sprintf("%s%s:%s", prefixAttempts, scene, identifier)
	if err := s.redis.Del(ctx, attemptKey).Err(); err != nil {
		s.logger.Warn("failed to reset attempt counter", zap.String("attemptKey", attemptKey), zap.Error(err))
	}

	return &SendCodeResult{Identifier: identifier, Scene: scene, ExpireSeconds: int(s.config.TTL.Seconds())}, nil
}

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
	var cancel context.CancelFunc
	ctx, cancel = s.wrapWithTimeout(ctx)
	defer cancel()

	lock, err := redislock.AcquireWithRetry(
		ctx,
		s.redis,
		verificationFlowLockKey(scene, identifier),
		s.sendLockOptions,
		s.sendLockRetryWait,
		s.logger,
	)
	if err != nil {
		return fail(StatusNotFound)
	}
	defer lock.Release()

	attemptKey := fmt.Sprintf("%s%s:%s", prefixAttempts, scene, identifier)
	codeKey := fmt.Sprintf("%s%s:%s", prefixCode, scene, identifier)

	// 原子操作：检查尝试次数 + 递增 + 获取验证码
	luaResult, err := verifyAndCountScript.Run(
		ctx, s.redis,
		[]string{attemptKey, codeKey},
		s.config.MaxAttempts, int(s.config.TTL.Seconds()),
	).StringSlice()

	if err != nil {
		return fail(StatusNotFound)
	}

	// luaResult[0] = 尝试次数（递增后），"-1" 表示已超限
	if len(luaResult) < 1 || luaResult[0] == "-1" {
		return fail(StatusTooManyAttempts)
	}

	// luaResult[1] = 验证码，空字符串表示不存在或已过期
	if len(luaResult) < 2 || luaResult[1] == "" {
		return fail(StatusNotFound)
	}

	// 比较验证码
	if luaResult[1] != code {
		return fail(StatusMismatch)
	}

	// 成功后清理验证码和尝试次数
	if err := s.redis.Del(ctx, codeKey, attemptKey).Err(); err != nil {
		s.logger.Warn("failed to delete code/attempt keys after successful verification", zap.String("codeKey", codeKey), zap.String("attemptKey", attemptKey), zap.Error(err))
	}
	return success()
}

// generateCode 生成密码学安全的随机数字验证码。
//
// 使用 crypto/rand 而非 math/rand 生成随机数，确保不可预测性。
// 相比 math/rand（伪随机，可用种子预测），crypto/rand 读取操作系统熵池，适合安全关键场景。
// 具体实现：循环生成长度为 length 的随机数字（0-9），拼接成字符串返回。
//
// 参数:
//   - length: 验证码位数（通常为 4 或 6）
//
// 返回值:
//   - string: 长度为 length 的纯数字字符串
//
// 边界情况:
//   - length <= 0 时返回空字符串（调用方保证传入合法参数）
//   - crypto/rand 读取失败时 n 为 0，极端情况下可能影响安全性，但 rand.Int 错误已被忽略
//     （实际生产中 /dev/urandom 极少失败，忽略错误以简化代码）
func generateCode(length int, logger *zap.Logger) string {
	code := make([]byte, length)
	for i := range code {
		n, err := randomInt(10)
		if err != nil {
			logger.Warn("failed to generate secure random code digit", zap.Error(err))
			n = 0
		}
		code[i] = byte('0' + n)
	}
	return string(code)
}

// fail 构造一个失败状态的验证码检查结果。
//
// 参数:
//   - status: 失败原因的状态码（StatusNotFound / StatusMismatch / StatusTooManyAttempts）
//
// 返回值:
//   - *VerificationCheckResult: Success=false，携带指定状态码的结果对象
func fail(status VerificationCodeStatus) *VerificationCheckResult {
	return &VerificationCheckResult{Success: false, Status: status}
}

// success 构造一个成功状态的验证码检查结果。
//
// 返回值:
//   - *VerificationCheckResult: Success=true，Status=StatusSuccess 的结果对象
func success() *VerificationCheckResult {
	return &VerificationCheckResult{Success: true, Status: StatusSuccess}
}

// wrapWithTimeout 为 context 设置超时，如果 operationTimeout 为 0 则透传原 context。
//
// 参数：
//   - ctx: 父 context
//
// 返回值：
//   - context.Context: 带超时的 context 或原 context
//   - context.CancelFunc: 取消函数，无超时时为空操作
func (s *VerificationService) wrapWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.operationTimeout > 0 {
		return context.WithTimeout(ctx, s.operationTimeout)
	}
	return ctx, func() {}
}
