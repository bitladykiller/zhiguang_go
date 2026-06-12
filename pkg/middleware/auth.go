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

import "github.com/gin-gonic/gin"

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

func setTokenClaims(c *gin.Context, claims TokenClaims) {
	c.Set(string(ctxUserID), claims.UserID())
	c.Set(string(ctxTokenType), claims.TokenType())
}
