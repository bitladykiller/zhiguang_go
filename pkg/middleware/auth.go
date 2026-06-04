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
//
// 功能：
//   强制要求请求携带有效的 JWT Bearer Token。
//   校验成功后把用户 ID 和令牌类型写入 Gin 上下文。
//   校验失败时直接中断请求并返回 401。
//
// 参数：
//   - validator: TokenValidator 接口实现（JWT 校验服务）
//
// 返回值：
//   - gin.HandlerFunc: Gin 中间件函数
//
// 处理流程：
//   Step 1: 从 Authorization 请求头提取 Bearer Token。
//   Step 2: 如果 token 为空，返回 401（缺少认证头）。
//   Step 3: 调用 validator.ValidateToken(token) 校验 token。
//   Step 4: 校验失败，返回 401（无效或过期的 token）。
//   Step 5: 校验成功，将 user_id 和 token_type 写入上下文，调用 c.Next()。
//
// 函数调用说明：
//   - c.AbortWithStatusJSON(code, body):
//     Gin 的方法，中断后续中间件和执行链，直接返回指定状态码和 JSON 响应体。
//   - c.Set(key, value):
//     Gin 的上下文存储方法，将值写入当前请求的上下文。
//     后续的处理器通过 c.Get(key) 读取。
//   - c.Next():
//     Gin 的方法，将控制权交给下一个中间件或最终处理器。
//
// 设计决策：
//   使用接口（TokenValidator）而非具体类型，避免 middleware 包与 auth 服务包之间
//   形成循环依赖。auth 包会导入 middleware 类型定义，而 middleware 如果不抽象出接口，
//   就无法引用 auth 包的类型（否则循环依赖）。
//
// 使用场景：
//   用于需要强制登录验证的路由组：
//     r.Group("/api/v1/user").Use(middleware.AuthMiddleware(jwtSvc))
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
//
// 功能：
//   提供"尽力而为"的鉴权。只有当请求携带了合法 JWT Token 时，
//   才会把用户 ID 写入上下文；否则不做任何处理，直接放行。
//
// 参数：
//   - validator: TokenValidator 接口实现
//
// 返回值：
//   - gin.HandlerFunc: Gin 中间件函数
//
// 处理流程：
//   Step 1: 从 Authorization 请求头提取 Bearer Token。
//   Step 2: 如果 token 为空，直接调用 c.Next() 放行（匿名访问）。
//   Step 3: 尝试校验 token，如果校验失败，也直接放行（匿名访问）。
//   Step 4: 校验成功，将 user_id 写入上下文后继续。
//
// 使用场景：
//   用于同时支持匿名访问和登录态增强的接口：
//   - 公共 feed：未登录用户看到通用内容，已登录用户看到个性化内容
//   - 知文详情：未登录只能看，已登录可看到已点赞状态
//   - 搜索：未登录可以搜索，已登录搜到的结果可显示个人状态
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
//
// 功能：
//   读取 Gin 上下文中的 user_id 值，并尝试将其转换为 uint64 类型。
//   支持多种数值类型（uint64、float64、int64、int），兼容 JSON 自动解析
//   和 JWT 服务返回的不同数值类型。
//
// 参数：
//   - c: Gin 上下文
//
// 返回值：
//   - uint64: 用户 ID（如果上下文中存在且类型可转换）
//   - bool:   true=成功获取用户 ID；false=上下文中没有用户 ID
//
// 边界情况：
//   - 上下文中不存在 user_id → 返回 0, false
//   - JSON 数字在多次序列化/反序列化后可能变为 float64，
//     因此需要显式支持 float64 → uint64 转换。
//   - 不支持的类型（如 string）→ 返回 0, false
//
// 兼容性说明：
//   Gin 的 Set/Get 是 interface{} 存取，不保留原始类型。
//   如果 user_id 设置时是 uint64，直接断言成功；
//   如果经过 JSON 编解码（如中间层 serialization），
//   可能变成 float64，需要额外处理。
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
//
// 功能：
//   解析 HTTP 请求头 "Authorization: Bearer <token>"，
//   返回 token 部分（去掉 "Bearer " 前缀）。
//
// 参数：
//   - c: Gin 上下文
//
// 返回值：
//   - string: 裸 token 字符串。如果请求头缺失或格式不合法则返回空字符串。
//
// 函数调用说明：
//   - c.GetHeader("Authorization"):
//     Gin 的方法，从 HTTP 请求中获取指定头部字段的值。
//     不区分大小写（Gin/Go 的 HTTP 库标准化头部名称）。
//   - strings.SplitN(header, " ", 2):
//     标准库字符串分割函数。按空格分割为最多 2 部分：
//     parts[0] 是类型（"Bearer"），parts[1] 是 token 值。
//   - strings.EqualFold(a, b):
//     不区分大小写的字符串比较。确保 "bearer"、"Bearer"、"BEARER" 都匹配。
//   - strings.TrimSpace(s):
//     去除字符串首尾空白字符。
//
// 边界情况：
//   - Authorization 头缺失 → 返回 ""
//   - Authorization 头只有 "Bearer" 没有 token → 返回 ""
//   - 非 Bearer 类型（如 "Basic xxx"）→ 返回 ""
//   - Token 值两侧有空格 → TrimSpace 处理
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
