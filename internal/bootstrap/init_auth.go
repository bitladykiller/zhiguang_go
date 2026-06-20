package bootstrap

import (
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/auth"
	"github.com/zhiguang/app/pkg/config"
)

// initAuth 创建鉴权模块的完整服务栈。
//
// 创建顺序：
//   1. JWT 服务（加载 PEM 密钥对，RS256 非对称签名）
//   2. 验证码服务（Redis 存储 + TTL 管理）
//   3. 刷新令牌白名单存储（Redis SET）
//   4. 用户仓储（MySQL users / login_logs 表）
//   5. AuthService（编排以上依赖，对外提供统一 Facade）
//   6. AuthHandler（HTTP 请求适配）
//
// 返回：
//   - *auth.AuthHandler: HTTP handler，注册路由用
//   - *auth.JwtService: JWT 服务，供 middleware 和路由注册使用
//   - error: JWT 密钥加载失败时返回
func initAuth(
	db *sqlx.DB,
	redisClient *redis.Client,
	cfg *config.Config,
	logger *zap.Logger,
) (*auth.AuthHandler, *auth.JwtService, error) {
	jwtSvc, err := auth.NewJwtService(&cfg.Auth.Jwt)
	if err != nil {
		return nil, nil, err
	}

	verifSvc := auth.NewVerificationService(redisClient, &cfg.Auth.Verification, logger)
	tokenStore := auth.NewRedisRefreshTokenStore(redisClient, logger)
	authRepo := auth.NewAuthRepository(db, logger)
	authSvc := auth.NewAuthService(authRepo, verifSvc, jwtSvc, tokenStore, redisClient, &cfg.Auth, logger)
	authHandler := auth.NewAuthHandler(authSvc, jwtSvc)

	return authHandler, jwtSvc, nil
}
