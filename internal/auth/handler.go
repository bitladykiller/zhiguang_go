package auth

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// AuthHandler 负责鉴权模块的 HTTP 路由注册与请求适配。
// 它将 HTTP 请求参数反序列化、验证后传递给 AuthService 处理，再组装响应。
type AuthHandler struct {
	svc    AuthServiceInterface
	jwtSvc *JwtService
}

// NewAuthHandler 创建 AuthHandler 实例。
//
// 参数:
//   - svc: AuthServiceInterface 实现，负责鉴权业务逻辑
//   - jwtSvc: JwtService 指针，用于 JWT 令牌解析路由保护规则
//
// 返回值:
//   - *AuthHandler: 已初始化的 Handler 实例
func NewAuthHandler(svc AuthServiceInterface, jwtSvc *JwtService) *AuthHandler {
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

// SendCode 发放验证码（POST /auth/send-code）。
func (h *AuthHandler) SendCode(c *gin.Context) {
	var req SendCodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.SendCode(c.Request.Context(), &req)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// Register 注册用户（POST /auth/register）。
func (h *AuthHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.Register(c.Request.Context(), &req, extractClientInfo(c))
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Created(c, data)
}

// Login 用户登录（POST /auth/login）。
// 校验密码后颁发 access token + refresh token，并记录审计日志。
func (h *AuthHandler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.Login(c.Request.Context(), &req, extractClientInfo(c))
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// Refresh 刷新令牌（POST /auth/refresh）。
// 校验 refresh token → 吊销旧 token → 颁发新 token 对（令牌轮换）。
func (h *AuthHandler) Refresh(c *gin.Context) {
	var req TokenRefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.Refresh(c.Request.Context(), &req)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// Logout 登出（POST /auth/logout）。
// 从 Redis 白名单移除 refresh token；access token 仍有效直到自然过期。
func (h *AuthHandler) Logout(c *gin.Context) {
	var req TokenRefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	h.svc.Logout(c.Request.Context(), &req)
	response.Success(c, gin.H{"message": "logged out"})
}

// ResetPassword 重置密码（POST /auth/reset-password）。
// 校验验证码 → bcrypt 哈希新密码 → 更新数据库 → 吊销该用户所有 refresh token。
func (h *AuthHandler) ResetPassword(c *gin.Context) {
	var req PasswordResetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	if appErr := h.svc.ResetPassword(c.Request.Context(), &req); appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, gin.H{"message": "password reset successful"})
}

// Me 获取当前用户信息（GET /auth/me）。
// 受 AuthMiddleware 保护，从 JWT 中提取用户 ID 后查询详情。
func (h *AuthHandler) Me(c *gin.Context) {
	userID, ok := middleware.GetUserID(c)
	if !ok {
		response.Error(c, errcode.ErrUnauthorized)
		return
	}

	data, appErr := h.svc.CurrentUser(c.Request.Context(), userID)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

// extractClientInfo 从 Gin 上下文中提取客户端 IP 和 User-Agent。
// IP 通过 c.ClientIP() 获取（自动处理 X-Forwarded-For），
// User-Agent 从请求头中提取。
func extractClientInfo(c *gin.Context) ClientInfo {
	return ClientInfo{
		IP:        c.ClientIP(),
		UserAgent: c.GetHeader("User-Agent"),
	}
}
