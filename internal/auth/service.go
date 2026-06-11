package auth

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/redislock"
)

// AuthService 编排整个鉴权域的业务流程。
//
// 为了避免注册、登录、刷新、密码重置和纯辅助函数继续堆叠在同一文件里，
// 当前按职责拆分为：
//   - service_registration.go: 发送验证码、注册、重置密码
//   - service_session.go: 登录、刷新、登出、当前用户
//   - service_helpers.go: 分布式锁和纯函数辅助逻辑
type AuthService struct {
	repo                 *AuthRepository
	verifSvc             *VerificationService
	jwtSvc               *JWTService
	tokenStore           RefreshTokenStore
	redis                *redis.Client
	cfg                  *config.AuthConfig
	refreshLockOptions   redislock.Options
	refreshLockRetryWait time.Duration
}

// NewAuthService 使用完整依赖创建 AuthService。
func NewAuthService(
	repo *AuthRepository,
	verifSvc *VerificationService,
	jwtSvc *JWTService,
	tokenStore RefreshTokenStore,
	redisClient *redis.Client,
	cfg *config.AuthConfig,
) *AuthService {
	return &AuthService{
		repo:                 repo,
		verifSvc:             verifSvc,
		jwtSvc:               jwtSvc,
		tokenStore:           tokenStore,
		redis:                redisClient,
		cfg:                  cfg,
		refreshLockOptions:   refreshSessionLockOptions(cfg),
		refreshLockRetryWait: refreshSessionRetryInterval(cfg),
	}
}
