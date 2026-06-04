// Package middleware 提供一组 Gin 中间件组件：
//   - JWT 鉴权（提取并校验 Bearer Token）
//   - CORS（跨域资源共享）
//   - 请求日志（基于 zap 的结构化日志）
//
// 使用方式：
//
//	r.Use(middleware.LoggerMiddleware(logger))
//	r.Use(middleware.CorsMiddleware())
//	r.Use(middleware.OptionalAuthMiddleware(jwtSvc))
//
// GET /api/v1/me 这类要求登录的接口可以用 AuthMiddleware 单独包裹。
package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// TokenClaims 是鉴权中间件要求 JWT Claims 实现的最小接口。
// 这里用接口而不是具体类型，是为了避免 middleware 与 auth 包之间形成循环依赖。
type TokenClaims interface {
	UserID() uint64
	TokenType() string
}

// TokenValidator 是 JWT 服务需要实现的校验接口。
type TokenValidator interface {
	ValidateToken(tokenStr string) (TokenClaims, error)
}

// contextKey 是 Gin 上下文键使用的私有类型，用来避免键名冲突。
type contextKey string

const (
	ctxUserID    contextKey = "user_id"
	ctxTokenType contextKey = "token_type"
)

// AuthMiddleware 返回一个校验 JWT Bearer Token 的 Gin 中间件。
// 校验成功后会把用户 ID 和令牌类型写入 Gin 上下文。
// 校验失败时会直接中断请求并返回 401。
func AuthMiddleware(validator TokenValidator) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractBearerToken(c)
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "missing authorization header",
			})
			return
		}

		claims, err := validator.ValidateToken(token)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "invalid or expired token",
			})
			return
		}

		c.Set(string(ctxUserID), claims.UserID())
		c.Set(string(ctxTokenType), claims.TokenType())
		c.Next()
	}
}

// OptionalAuthMiddleware 与 AuthMiddleware 类似，但在缺少 token 时不会中断请求。
// 只有当请求携带了合法 token 时，它才会把用户 ID 写入上下文。
// 适用于既支持匿名访问又支持登录态增强的接口，例如公共 feed。
func OptionalAuthMiddleware(validator TokenValidator) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractBearerToken(c)
		if token == "" {
			c.Next()
			return
		}

		claims, err := validator.ValidateToken(token)
		if err != nil {
			c.Next()
			return
		}

		c.Set(string(ctxUserID), claims.UserID())
		c.Set(string(ctxTokenType), claims.TokenType())
		c.Next()
	}
}

// GetUserID 从 Gin 上下文中提取已认证用户的 ID。
// 如果上下文中没有 user_id，则返回 0 和 false。
func GetUserID(c *gin.Context) (uint64, bool) {
	val, exists := c.Get(string(ctxUserID))
	if !exists {
		return 0, false
	}

	// 兼容 JSON 数字被解码成 float64 的情况
	switch v := val.(type) {
	case uint64:
		return v, true
	case float64:
		return uint64(v), true
	case int64:
		return uint64(v), true
	case int:
		return uint64(v), true
	}
	return 0, false
}

// extractBearerToken 从 Authorization 请求头中提取 Token。
// 期望格式为：`Bearer <token>`。
func extractBearerToken(c *gin.Context) string {
	header := c.GetHeader("Authorization")
	if header == "" {
		return ""
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}

	return strings.TrimSpace(parts[1])
}
