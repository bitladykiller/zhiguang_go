package auth

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/errcode"
	"github.com/zhiguang/app/pkg/middleware"
	"github.com/zhiguang/app/pkg/response"
)

// AuthHandler 负责鉴权模块的 HTTP 路由与请求适配。
type AuthHandler struct {
	svc    *AuthService
	jwtSvc *JwtService
}

func NewAuthHandler(svc *AuthService, jwtSvc *JwtService) *AuthHandler {
	return &AuthHandler{svc: svc, jwtSvc: jwtSvc}
}

// RegisterRoutes 注册鉴权模块的全部路由。
// 公开接口不要求鉴权中间件，`/me` 则要求合法的 access token。
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

func (h *AuthHandler) SendCode(c *gin.Context) {
	var req SendCodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.SendCode(c.Request.Context(), &req)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

func (h *AuthHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.Register(c.Request.Context(), &req, extractClientInfo(c))
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Created(c, data)
}

func (h *AuthHandler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.Login(c.Request.Context(), &req, extractClientInfo(c))
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

func (h *AuthHandler) Refresh(c *gin.Context) {
	var req TokenRefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	data, appErr := h.svc.Refresh(c.Request.Context(), &req)
	if appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, data)
}

func (h *AuthHandler) Logout(c *gin.Context) {
	var req TokenRefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	h.svc.Logout(c.Request.Context(), &req)
	response.Success(c, gin.H{"message": "logged out"})
}

func (h *AuthHandler) ResetPassword(c *gin.Context) {
	var req PasswordResetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, 400, "invalid request: "+err.Error())
		return
	}

	if appErr := h.svc.ResetPassword(c.Request.Context(), &req); appErr != nil {
		response.Error(c, appErr)
		return
	}
	response.Success(c, gin.H{"message": "password reset successful"})
}

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

func extractClientInfo(c *gin.Context) ClientInfo {
	return ClientInfo{
		IP:        c.ClientIP(),
		UserAgent: c.GetHeader("User-Agent"),
	}
}
