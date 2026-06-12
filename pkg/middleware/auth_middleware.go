package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware 返回一个校验 JWT Bearer Token 的 Gin 中间件。
//
// 功能：
//
//	强制要求请求携带有效的 JWT Bearer Token。
//	校验成功后把用户 ID 和令牌类型写入 Gin 上下文。
//	校验失败时直接中断请求并返回 401。
//
// 参数：
//   - validator: TokenValidator 接口实现（JWT 校验服务）
//
// 返回值：
//   - gin.HandlerFunc: Gin 中间件函数
//
// 处理流程：
//
//	Step 1: 从 Authorization 请求头提取 Bearer Token。
//	Step 2: 如果 token 为空，返回 401（缺少认证头）。
//	Step 3: 调用 validator.ValidateToken(token) 校验 token。
//	Step 4: 校验失败，返回 401（无效或过期的 token）。
//	Step 5: 校验成功，将 user_id 和 token_type 写入上下文，调用 c.Next()。
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
//
//	使用接口（TokenValidator）而非具体类型，避免 middleware 包与 auth 服务包之间
//	形成循环依赖。auth 包会导入 middleware 类型定义，而 middleware 如果不抽象出接口，
//	就无法引用 auth 包的类型（否则循环依赖）。
//
// 使用场景：
//
//	用于需要强制登录验证的路由组：
//	  r.Group("/api/v1/user").Use(middleware.AuthMiddleware(jwtSvc))
func AuthMiddleware(validator TokenValidator) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractBearerToken(c)
		if token == "" {
			abortUnauthorized(c, "missing authorization header")
			return
		}

		claims, err := validator.ValidateToken(token)
		if err != nil {
			abortUnauthorized(c, "invalid or expired token")
			return
		}

		setTokenClaims(c, claims)
		c.Next()
	}
}

// OptionalAuthMiddleware 与 AuthMiddleware 类似，但在缺少 token 时不会中断请求。
//
// 功能：
//
//	提供"尽力而为"的鉴权。只有当请求携带了合法 JWT Token 时，
//	才会把用户 ID 写入上下文；否则不做任何处理，直接放行。
//
// 参数：
//   - validator: TokenValidator 接口实现
//
// 返回值：
//   - gin.HandlerFunc: Gin 中间件函数
//
// 处理流程：
//
//	Step 1: 从 Authorization 请求头提取 Bearer Token。
//	Step 2: 如果 token 为空，直接调用 c.Next() 放行（匿名访问）。
//	Step 3: 尝试校验 token，如果校验失败，也直接放行（匿名访问）。
//	Step 4: 校验成功，将 user_id 写入上下文后继续。
//
// 使用场景：
//
//	用于同时支持匿名访问和登录态增强的接口：
//	- 公共 feed：未登录用户看到通用内容，已登录用户看到个性化内容
//	- 知文详情：未登录只能看，已登录可看到已点赞状态
//	- 搜索：未登录可以搜索，已登录搜到的结果可显示个人状态
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

		setTokenClaims(c, claims)
		c.Next()
	}
}

func abortUnauthorized(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"code":    401,
		"message": message,
	})
}
