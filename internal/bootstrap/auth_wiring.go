package bootstrap

import (
	"github.com/zhiguang/app/internal/auth"
	"github.com/zhiguang/app/internal/server"
	"github.com/zhiguang/app/pkg/middleware"
)

// AuthModule 汇总鉴权模块对外暴露的装配结果。
//
// Auth 除了 HTTP handler，还需要向路由鉴权中间件暴露 token 校验能力，
// 因此这里返回一个模块结构，而不是只返回单个 handler。
type AuthModule struct {
	Handler        server.RouteRegistrar
	TokenValidator middleware.TokenValidator
}

// BuildAuthModule 构建鉴权领域。
//
// 依赖方向保持单向：
//   - handler 依赖 use case 接口
//   - service 依赖 repository / redis / jwt 等基础设施
//   - bootstrap 负责最终装配
func BuildAuthModule(infra *InfraDeps) (AuthModule, error) {
	jwtSvc, err := auth.NewJWTService(&infra.Config.Auth.JWT)
	if err != nil {
		return AuthModule{}, err
	}

	verifSvc := auth.NewVerificationService(infra.Redis, &infra.Config.Auth.Verification)
	tokenStore := auth.NewRedisRefreshTokenStore(infra.Redis)
	authRepo := auth.NewAuthRepository(infra.DB)
	authSvc := auth.NewAuthService(authRepo, verifSvc, jwtSvc, tokenStore, infra.Redis, &infra.Config.Auth)

	return AuthModule{
		Handler:        auth.NewAuthHandler(authSvc, jwtSvc),
		TokenValidator: jwtSvc,
	}, nil
}
