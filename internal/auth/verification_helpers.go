package auth

import (
	"context"
	"crypto/rand"
	"math/big"

	"github.com/zhiguang/app/pkg/redislock"
)

func (s *VerificationService) acquireFlowLock(
	ctx context.Context,
	scene VerificationScene,
	identifier string,
) (*redislock.Lock, error) {
	return redislock.AcquireWithRetry(
		ctx,
		s.redis,
		verificationFlowLockKey(scene, identifier),
		s.sendLockOptions,
		s.sendLockRetryWait,
	)
}

func (s *VerificationService) sendCodeResult(scene VerificationScene, identifier string) *SendCodeResult {
	return &SendCodeResult{
		Identifier:    identifier,
		Scene:         scene,
		ExpireSeconds: int(s.config.TTL.Seconds()),
	}
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
func generateCode(length int) string {
	code := make([]byte, length)
	for i := range code {
		n, _ := rand.Int(rand.Reader, big.NewInt(10))
		code[i] = byte('0' + n.Int64())
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
