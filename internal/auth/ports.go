package auth

import (
	"context"

	"github.com/zhiguang/app/pkg/errcode"
)

// AuthUseCase 定义 AuthHandler 所依赖的鉴权业务接口。
//
// Handler 只依赖这组语义稳定的接口，而不是直接依赖具体的 AuthService，
// 这样测试时可以只替换这一层，也能避免 HTTP 层被鉴权内部实现细节反向污染。
type AuthUseCase interface {
	SendCode(ctx context.Context, req *SendCodeRequest) (SendCodeResponse, *errcode.AppError)
	Register(ctx context.Context, req *RegisterRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError)
	Login(ctx context.Context, req *LoginRequest, clientInfo ClientInfo) (AuthResponse, *errcode.AppError)
	Refresh(ctx context.Context, req *TokenRefreshRequest) (AuthResponse, *errcode.AppError)
	Logout(ctx context.Context, req *TokenRefreshRequest)
	ResetPassword(ctx context.Context, req *PasswordResetRequest) *errcode.AppError
	CurrentUser(ctx context.Context, userID uint64) (AuthUserResponse, *errcode.AppError)
}
