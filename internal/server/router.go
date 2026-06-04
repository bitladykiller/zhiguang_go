package server

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/middleware"
	"go.uber.org/zap"
)

// HandlerSet 汇总所有需要注册路由的 HTTP 处理器。
//
// 每个处理器字段的类型都是 RouteRegistrar 接口，而不是具体的 handler 类型。
// 这样设计的原因：
//   - 处理器在配置不完整时可能为 nil（如 Search 或 LLM）时，
//     不会因为 nil 接口调用而 panic。
//   - 装配阶段只需在 RegisterRoutes 调用前做 nil 检查即可。
type HandlerSet struct {
	Auth     RouteRegistrar
	KnowPost RouteRegistrar
	Counter  RouteRegistrar
	Relation RouteRegistrar
	Search   RouteRegistrar
	LLM      RouteRegistrar
	Storage  RouteRegistrar
	Profile  RouteRegistrar
}

// RouteRegistrar 表示任何能够注册一组 HTTP 路由的组件。
// 每个业务 handler 都应实现此接口，在内部注册自己的全部路由。
type RouteRegistrar interface {
	RegisterRoutes(r *gin.RouterGroup)
}

// NewRouter 创建带全局中间件和全部 API 路由的 Gin 引擎。
//
// 全局中间件链（按执行顺序）：
//  1. LoggerMiddleware：记录请求日志
//  2. CorsMiddleware：处理跨域
//  3. Recovery：捕获 panic 并返回 500
//  4. OptionalAuthMiddleware：尝试解析 JWT，但不拒绝未登录的请求
//
// WHY 使用 OptionalAuthMiddleware 而非 AuthMiddleware：
// 全局挂载可选鉴权中间件后，那些同时支持匿名访问和登录态增强的接口
//（如公共 feed、知文详情、搜索）也能拿到当前用户的登录身份，
// 可在响应中补充用户维度的状态（如是否已点赞）。
// 而真正受保护的接口依然会在各自处理器内部显式做鉴权判断。
func NewRouter(handlers *HandlerSet, logger *zap.Logger, tokenValidator middleware.TokenValidator) *gin.Engine {
	r := gin.New()

	// --- 全局中间件 ---
	r.Use(middleware.LoggerMiddleware(logger))
	r.Use(middleware.CorsMiddleware())
	r.Use(gin.Recovery())
	if tokenValidator != nil {
		r.Use(middleware.OptionalAuthMiddleware(tokenValidator))
	}

	// --- 健康检查 ---
	// K8s 或 Docker 的健康探针使用此接口判断服务存活状态
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// --- API v1 路由 ---
	// 按模块注册路由，每个处理器可选（可能因配置不完整而返回 nil）
	v1 := r.Group("/api/v1")
	{
		if handlers.Auth != nil {
			handlers.Auth.RegisterRoutes(v1)
		}
		if handlers.KnowPost != nil {
			handlers.KnowPost.RegisterRoutes(v1)
		}
		if handlers.Counter != nil {
			handlers.Counter.RegisterRoutes(v1)
		}
		if handlers.Relation != nil {
			handlers.Relation.RegisterRoutes(v1)
		}
		if handlers.Search != nil {
			handlers.Search.RegisterRoutes(v1)
		}
		if handlers.LLM != nil {
			handlers.LLM.RegisterRoutes(v1)
		}
		if handlers.Storage != nil {
			handlers.Storage.RegisterRoutes(v1)
		}
		if handlers.Profile != nil {
			handlers.Profile.RegisterRoutes(v1)
		}
	}

	return r
}
