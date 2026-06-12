package auth

import (
	"fmt"
	"time"
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

func verificationCodeKey(scene VerificationScene, identifier string) string {
	return fmt.Sprintf("%s%s:%s", prefixCode, scene, identifier)
}

func verificationIntervalKey(scene VerificationScene, identifier string) string {
	return fmt.Sprintf("%s%s:%s", prefixInterval, scene, identifier)
}

func verificationAttemptKey(scene VerificationScene, identifier string) string {
	return fmt.Sprintf("%s%s:%s", prefixAttempts, scene, identifier)
}

func verificationDailyKey(scene VerificationScene, identifier string, now time.Time) string {
	return fmt.Sprintf("%s%s:%s:%s", prefixDaily, scene, identifier, now.Format("20060102"))
}
