package auth

import (
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

// VerificationService 负责管理验证码的完整生命周期。
//
// 为了避免发送流程、校验流程、Lua 脚本、Redis key 规则继续堆在同一文件里，
// 当前按职责拆分为：
//   - verification.go: 服务结构体与构造函数
//   - verification_keys.go: Redis key 规则
//   - verification_scripts.go: Lua 脚本
//   - verification_send.go: 发送流程
//   - verification_verify.go: 校验流程
//   - verification_helpers.go: 随机验证码与结果辅助函数
type VerificationService struct {
	redis             *redis.Client
	config            *config.VerificationConfig
	sendLockOptions   redislock.Options
	sendLockRetryWait time.Duration
}

// NewVerificationService 创建验证码服务实例。
//
// 参数:
//   - redisClient: Redis 客户端连接，用于验证码的存储、过期和计数
//   - cfg: 验证码配置，包含有效期、发送间隔、每日上限、验证码长度、最大尝试次数
//
// 返回值:
//   - *VerificationService: 验证码服务实例
func NewVerificationService(redisClient *redis.Client, cfg *config.VerificationConfig) *VerificationService {
	return &VerificationService{
		redis:             redisClient,
		config:            cfg,
		sendLockOptions:   verificationSendLockOptions(cfg),
		sendLockRetryWait: verificationSendRetryInterval(cfg),
	}
}
