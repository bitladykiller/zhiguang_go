package auth

import (
	"context"
)

// AuthServiceInterface 定义鉴权模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *AuthService，使得 handler 可以独立于
// service 实现进行单元测试，也支持在 bootstrap 中注入不同的实现
// （如 mock 用于测试、降级实现用于可选能力探测）。
// 错误约定：与全仓一致返回 error。
// 服务内部仍以 errcode.ErrX(.WithMsg) 构造（其本身实现 error），
// handler 统一经 httputil.ToAppError 还原业务码——auth 曾是唯一返回
// *errcode.AppError 类型化指针的模块，双轨约定与 typed-nil 隐患由此消除。
type AuthServiceInterface interface {
	SendCode(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, error)
	Register(ctx context.Context, req *RegisterRequest, clientInfo ClientInfo) (AuthResponse, error)
	Login(ctx context.Context, req *LoginRequest, clientInfo ClientInfo) (AuthResponse, error)
	Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, error)
	Logout(ctx context.Context, req *TokenRefreshRequest)
	ResetPassword(ctx context.Context, req *PasswordResetRequest) error
	CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, error)
}

// 编译期断言：*AuthService 实现了 AuthServiceInterface。
var _ AuthServiceInterface = (*AuthService)(nil)
