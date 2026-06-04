// server 包是 HTTP 应用的顶层容器与启动入口。
// 它持有 Gin 路由引擎、全局配置和后台 Runner 集合，
// 通过统一的 Run() 方法启动服务和所有后台 goroutine。
package server

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

// BackgroundRunner 表示会伴随 HTTP 服务一起启动的长生命周期后台任务。
//
// 接口设计：
//   - Start(ctx) 会以 goroutine 方式并发启动，不阻塞 HTTP 服务主 goroutine。
//   - 当 ctx 被取消时（服务关闭），Runner 应自行清理并退出。
//   - 当前的后台 Runner 包括：
//     + canal.Bridge：Canal binlog → Kafka 的桥接器
//     + search.OutboxConsumer：搜索索引异步同步消费者
//     + relation.OutboxConsumer：关系事件异步同步消费者
type BackgroundRunner interface {
	Start(ctx context.Context)
}

// App 是顶层应用容器。
// 它持有 Gin 路由、全局配置以及统一的 Run() 启动入口。
type App struct {
	router     *gin.Engine
	config     *config.Config
	logger     *zap.Logger
	background []BackgroundRunner
}

// NewApp 创建一个新的应用实例。
//
// 功能：
//   将路由引擎、配置、日志器和后台 Runner 组装为一个 App 实例。
//   采用函数选项模式（varargs）传递后台 Runner，使得 Runner 列表可扩展。
//
// 参数：
//   - router:    Gin 路由引擎（已配置好所有路由和中间件）
//   - cfg:       全局应用配置
//   - logger:    zap 结构化日志器
//   - background: 零个或多个后台 Runner（可选，可能为 nil）
//
// 返回值：
//   - *App: 组装完成的应用实例
//
// 设计决策：
//   使用变长参数（...BackgroundRunner）而非切片参数，
//   使得调用时可以传入零个、一个或多个 Runner，API 更简洁：
//     NewApp(router, cfg, logger)
//     NewApp(router, cfg, logger, bridge, consumer1, consumer2)
func NewApp(router *gin.Engine, cfg *config.Config, logger *zap.Logger, background ...BackgroundRunner) *App {
	return &App{
		router:     router,
		config:     cfg,
		logger:     logger,
		background: background,
	}
}

// Run 启动 HTTP 服务，并一直阻塞到服务退出。
//
// 功能（启动流程）：
//   Step 1: 遍历所有后台 Runner，对非 nil 的 Runner 以 goroutine 方式启动。
//   Step 2: 在 Gin 引擎上监听 a.config.Server.Port 端口，直到收到退出信号。
//   Step 3: Run() 在 a.router.Run() 返回后退出（通常是在接收到 SIGINT/SIGTERM 时）。
//
// 参数：
//   - 无（使用 App 实例持有的配置）
//
// 返回值：
//   - error: Gin 路由启动失败时返回（如端口被占用）
//
// 函数调用说明：
//   - a.router.Run(addr):
//     Gin 引擎的 Run 方法启动 HTTP 服务并阻塞当前 goroutine。
//     addr 格式为 ":port"（如 ":8080"）。
//     内部调用 http.ListenAndServe，当服务被关闭时返回 error。
//
// 设计决策：
//   后台 Runner 以 goroutine 方式启动，不阻塞主 goroutine。
//   所有 Runner 共享同一个上下文（context.Background()），
//   当 Gin 退出时，Runner 仍然运行。
//   未来如果需要优雅关闭，应使用 context.WithCancel 并在 Shutdown 时取消。
//
// 边界情况：
//   - Runner 为 nil：跳过（不启动），避免 nil goroutine 导致 panic
//   - 端口被占用：router.Run() 返回错误，程序退出
func (a *App) Run() error {
	ctx := context.Background()
	for _, runner := range a.background {
		if runner == nil {
			continue
		}
		go runner.Start(ctx)
	}

	addr := fmt.Sprintf(":%d", a.config.Server.Port)
	a.logger.Info("starting zhiguang server",
		zap.String("addr", addr),
		zap.String("mode", a.config.Server.Mode),
	)
	return a.router.Run(addr)
}
