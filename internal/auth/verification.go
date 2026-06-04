package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
)

// 验证码相关 Redis 键前缀。
const (
	prefixCode     = "vc:code:"
	prefixInterval = "vc:interval:"
	prefixDaily    = "vc:daily:"
	prefixAttempts = "vc:attempts:"
)

// VerificationService 负责管理验证码的完整生命周期。
// 验证码存储在 Redis 中，依赖 TTL 自动过期。
// 同时会执行频率限制，包括发送间隔限制和每日发送上限。
type VerificationService struct {
	redis  *redis.Client
	config *config.VerificationConfig
}

// NewVerificationService 创建验证码服务。
func NewVerificationService(redisClient *redis.Client, cfg *config.VerificationConfig) *VerificationService {
	return &VerificationService{redis: redisClient, config: cfg}
}

// SendCode 生成随机数字验证码，写入 Redis，并返回基础元信息。
// 该过程会执行：
//   - 发送间隔限制：防止过于频繁发送（默认 60 秒）
//   - 每日上限限制：限制单个标识每天可发送的次数（默认 10 次）
func (s *VerificationService) SendCode(scene VerificationScene, identifier string) (*SendCodeResult, error) {
	ctx := context.Background()

	// 检查发送间隔，防止短信轰炸
	intervalKey := fmt.Sprintf("%s%s:%s", prefixInterval, scene, identifier)
	exists, err := s.redis.Exists(ctx, intervalKey).Result()
	if err != nil {
		return nil, err
	}
	if exists > 0 {
		// 对调用方保持正常返回，避免暴露限流细节
		return &SendCodeResult{Identifier: identifier, Scene: scene, ExpireSeconds: int(s.config.TTL.Seconds())}, nil
	}

	// 检查每日发送上限
	dailyKey := fmt.Sprintf("%s%s:%s:%s", prefixDaily, scene, identifier, time.Now().Format("20060102"))
	dailyCount, err := s.redis.Get(ctx, dailyKey).Int()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if dailyCount >= s.config.DailyLimit {
		return nil, fmt.Errorf("daily limit exceeded")
	}

	// 生成验证码
	code := generateCode(s.config.CodeLength)

	// 通过 pipeline 原子写入 Redis
	codeKey := fmt.Sprintf("%s%s:%s", prefixCode, scene, identifier)
	pipe := s.redis.Pipeline()
	pipe.Set(ctx, codeKey, code, s.config.TTL)
	pipe.Set(ctx, intervalKey, "1", s.config.SendInterval)
	pipe.Incr(ctx, dailyKey)
	pipe.Expire(ctx, dailyKey, 24*time.Hour)

	if _, err = pipe.Exec(ctx); err != nil {
		return nil, err
	}

	// 重置尝试次数计数器（新验证码意味着新的尝试配额）
	attemptKey := fmt.Sprintf("%s%s:%s", prefixAttempts, scene, identifier)
	s.redis.Del(ctx, attemptKey)

	// 生产环境应走短信/邮件渠道；当前先输出到标准输出便于联调。
	fmt.Printf("[VERIFICATION] Scene=%s Identifier=%s Code=%s\n", scene, identifier, code)

	return &SendCodeResult{Identifier: identifier, Scene: scene, ExpireSeconds: int(s.config.TTL.Seconds())}, nil
}

// Verify 使用 Redis 中保存的验证码校验用户输入。
// 它会统计尝试次数，并在达到 MaxAttempts 后直接锁定。
func (s *VerificationService) Verify(scene VerificationScene, identifier, code string) *VerificationCheckResult {
	ctx := context.Background()
	attemptKey := fmt.Sprintf("%s%s:%s", prefixAttempts, scene, identifier)
	codeKey := fmt.Sprintf("%s%s:%s", prefixCode, scene, identifier)

	// 检查尝试次数限制
	attempts, err := s.redis.Get(ctx, attemptKey).Int()
	if err != nil && err != redis.Nil {
		return fail(StatusNotFound)
	}
	if attempts >= s.config.MaxAttempts {
		return fail(StatusTooManyAttempts)
	}

	// 增加尝试次数。这里采用 fail-close 思路，即使后续校验失败也记录本次尝试。
	s.redis.Incr(ctx, attemptKey)
	s.redis.Expire(ctx, attemptKey, s.config.TTL)

	// 读取已保存的验证码
	storedCode, err := s.redis.Get(ctx, codeKey).Result()
	if err == redis.Nil {
		return fail(StatusNotFound)
	}
	if err != nil {
		return fail(StatusNotFound)
	}

	// 比较验证码
	if storedCode != code {
		return fail(StatusMismatch)
	}

	// 成功后清理验证码和尝试次数
	s.redis.Del(ctx, codeKey, attemptKey)
	return success()
}

// generateCode 生成具备密码学安全性的随机数字验证码。
func generateCode(length int) string {
	code := make([]byte, length)
	for i := range code {
		n, _ := rand.Int(rand.Reader, big.NewInt(10))
		code[i] = byte('0' + n.Int64())
	}
	return string(code)
}

func fail(status VerificationCodeStatus) *VerificationCheckResult {
	return &VerificationCheckResult{Success: false, Status: status}
}

func success() *VerificationCheckResult {
	return &VerificationCheckResult{Success: true, Status: StatusSuccess}
}
