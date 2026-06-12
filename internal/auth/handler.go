package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/middleware"
)

// AuthHandler 负责鉴权模块的 HTTP 路由注册与请求适配。
//
// 为了避免公开接口、受保护接口和 handler 级辅助逻辑继续堆在同一文件里，
// 当前按职责拆分为：
//   - handler.go: 结构体、构造函数、路由注册
//   - handler_public.go: 无需 JWT 的公开接口
//   - handler_protected.go: 需要已登录用户的受保护接口
//   - handler_helpers.go: 绑定请求、提取当前用户与客户端信息等辅助函数
type AuthHandler struct {
	svc    AuthUseCase
	jwtSvc middleware.TokenValidator
}

func NewAuthHandler(svc AuthUseCase, jwtSvc middleware.TokenValidator) *AuthHandler {
	return &AuthHandler{svc: svc, jwtSvc: jwtSvc}
}

// RegisterRoutes 注册鉴权模块的全部路由。
//
// 路由说明：
//   - `/send-code`、`/register`、`/login`、`/refresh`、`/logout`、`/reset-password`：
//     公开接口，不需要 JWT 鉴权。
//   - `/me`：受保护接口，通过 middleware.AuthMiddleware 要求合法的 access token。
//
// WHY：/me 需要单独加 AuthMiddleware，而不是在全局注入，
// 因为全局使用的是 OptionalAuthMiddleware（允许匿名访问）。
func (h *AuthHandler) RegisterRoutes(r *gin.RouterGroup) {
	auth := r.Group("/auth")
	{
		auth.POST("/send-code", h.SendCode)
		auth.POST("/register", h.Register)
		auth.POST("/login", h.Login)
		auth.POST("/refresh", h.Refresh)
		auth.POST("/logout", h.Logout)
		auth.POST("/reset-password", h.ResetPassword)
		auth.GET("/me", middleware.AuthMiddleware(h.jwtSvc), h.Me)
	}
}
