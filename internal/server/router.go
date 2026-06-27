package server

import (
	"net/http/pprof"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zhiguang/app/pkg/config"
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
	Auth          RouteRegistrar
	KnowPost      RouteRegistrar
	Counter       RouteRegistrar
	Relation      RouteRegistrar
	Search        RouteRegistrar
	LLM           RouteRegistrar
	Storage       RouteRegistrar
	Profile       RouteRegistrar
	RateLimiter   *middleware.RateLimiter
}

// RouteRegistrar 表示任何能够注册一组 HTTP 路由的组件。
// 每个业务 handler 都应实现此接口，在内部注册自己的全部路由。
type RouteRegistrar interface {
	RegisterRoutes(r *gin.RouterGroup)
}

// NewRouter 创建带全局中间件和全部 API 路由的 Gin 引擎。
//
// 功能：
//   创建 Gin 引擎实例，按顺序注册全局中间件，挂载健康检查端点，
//   并遍历 HandlerSet 中的每个处理器，将非 nil 的处理器路由注册到 /api/v1 组下。
//
// 参数：
//   - handlers: 汇总所有业务模块的 HTTP 处理器。每个处理器可能为 nil
//     （由外部初始化时根据配置决定是否创建）。
//   - logger:   zap 结构化日志器，用于 LoggerMiddleware
//   - tokenValidator: JWT Token 校验器，用于 OptionalAuthMiddleware。可能为 nil。
//   - healthChecker: 健康检查器，用于注册 /health 和 /ready 端点。可能为 nil。
//
// 返回值：
//   - *gin.Engine: 配置完成的 Gin 引擎，可直接用于 Run()
//
// 全局中间件链（按执行顺序）：
//  1. TraceMiddleware：注入 trace id + 请求超时
//  2. LoggerMiddleware：记录请求日志（zap 结构化）
//  3. ErrorLogMiddleware：记录 handler 层的错误
//  4. CorsMiddleware：处理跨域请求
//  5. gin.Recovery：捕获 panic 并返回 500，防止服务崩溃
//  6. OptionalAuthMiddleware：尝试解析 JWT Token，但不拒绝未登录的请求
//
// WHY 使用 OptionalAuthMiddleware 而非 AuthMiddleware：
//   全局挂载可选鉴权中间件后，那些同时支持匿名访问和登录态增强的接口
//  （如公共 feed、知文详情、搜索）也能拿到当前用户的登录身份，
//   可在响应中补充用户维度的状态（如是否已点赞）。
//   而真正受保护的接口依然会在各自处理器内部显式做鉴权判断。
//
// 函数调用说明：
//   - gin.New():
//     创建纯 Gin 引擎（不含默认中间件 Logger 和 Recovery）。
//     因为要用自定义的 LoggerMiddleware 和 CorsMiddleware。
//   - r.Use(middleware):
//     向 Gin 引擎注册全局中间件。中间件按注册顺序执行。
//   - gin.Recovery():
//     Gin 内置的 Recovery 中间件。从 panic 中恢复并返回 500 响应。
//
// 边界情况：
//   - handlers 中的某个处理器为 nil → 跳过注册，该模块路由不可访问
//     （不会 panic 或报错）
//   - tokenValidator 为 nil → 不挂载 OptionalAuthMiddleware
//     （所有接口均匿名访问）
//   - healthChecker 为 nil → 使用默认的简单健康检查端点
func NewRouter(handlers *HandlerSet, logger *zap.Logger, tokenValidator middleware.TokenValidator, healthChecker *HealthChecker, cfg *config.Config) *gin.Engine {
	r := gin.New()

	// --- 全局中间件 ---
	timeout := 30 * time.Second
	if cfg != nil {
		timeout = cfg.Server.HTTPRequestTimeout()
	}
	r.Use(middleware.TraceMiddleware(timeout))
	r.Use(middleware.LoggerMiddleware(logger))
	r.Use(middleware.ErrorLogMiddleware(logger))
	if cfg != nil && cfg.Prometheus.Enabled {
		r.Use(middleware.MetricsMiddleware())
	}
	if cfg != nil && handlers.RateLimiter != nil {
		r.Use(handlers.RateLimiter.Middleware())
	}
	r.Use(middleware.CorsMiddleware(cfg.Server.CorsAllowedOrigins))
	r.Use(gin.Recovery())
	if tokenValidator != nil {
		r.Use(middleware.OptionalAuthMiddleware(tokenValidator))
	}

	// --- 健康检查 ---
	// K8s 或 Docker 的健康探针使用此接口判断服务存活状态
	if healthChecker != nil {
		healthChecker.RegisterRoutes(&r.RouterGroup)
	} else {
		// 兜底：如果没有提供 HealthChecker，使用简单的存活探针
		r.GET("/health", func(c *gin.Context) {
			c.JSON(200, gin.H{"status": "ok"})
		})
	}

	if cfg != nil && cfg.Server.Mode == "debug" {
		dbg := r.Group("/debug/pprof")
		{
			dbg.GET("/", gin.WrapF(pprof.Index))
			dbg.GET("/cmdline", gin.WrapF(pprof.Cmdline))
			dbg.GET("/profile", gin.WrapF(pprof.Profile))
			dbg.POST("/symbol", gin.WrapF(pprof.Symbol))
			dbg.GET("/symbol", gin.WrapF(pprof.Symbol))
			dbg.GET("/trace", gin.WrapF(pprof.Trace))
			dbg.GET("/allocs", gin.WrapH(pprof.Handler("allocs")))
			dbg.GET("/block", gin.WrapH(pprof.Handler("block")))
			dbg.GET("/goroutine", gin.WrapH(pprof.Handler("goroutine")))
			dbg.GET("/heap", gin.WrapH(pprof.Handler("heap")))
			dbg.GET("/mutex", gin.WrapH(pprof.Handler("mutex")))
			dbg.GET("/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
		}
	}

	// --- Prometheus metrics ---
	if cfg != nil && cfg.Prometheus.Enabled {
		r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

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
