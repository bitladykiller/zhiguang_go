package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
)

// AuthServicer 定义鉴权模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *AuthService，使得 handler 可以独立于
// service 实现进行单元测试，也支持在 bootstrap 中注入不同的实现
// （如 mock 用于测试、降级实现用于可选能力探测）。
type AuthServicer interface {
	SendCode(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, *errcode.AppError)
	Register(ctx context.Context, req *RegisterRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError)
	Login(ctx context.Context, req *LoginRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError)
	Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError)
	Logout(ctx context.Context, req *TokenRefreshRequest)
	ResetPassword(ctx context.Context, req *PasswordResetRequest) *errcode.AppError
	CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError)
}

// 编译期断言：*AuthService 实现了 AuthServicer。
var _ AuthServicer = (*AuthService)(nil)
