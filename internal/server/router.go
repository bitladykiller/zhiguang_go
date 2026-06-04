package server

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/middleware"
	"go.uber.org/zap"
)

// HandlerSet 汇总所有需要注册路由的 HTTP 处理器。
// 每个处理器都应实现 `RegisterRoutes(*gin.RouterGroup)` 方法。
// 启动装配阶段会把处理器实例汇总到这里。
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

// RouteRegistrar 表示任何能够注册 HTTP 路由的组件。
type RouteRegistrar interface {
	RegisterRoutes(r *gin.RouterGroup)
}

// NewRouter 创建带全局中间件和全部 API 路由的 Gin 引擎。
// WHY：全局挂载 OptionalAuthMiddleware 后，
// 那些同时支持匿名访问和登录访问的接口也能拿到登录身份；
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
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// --- API v1 路由 ---
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
