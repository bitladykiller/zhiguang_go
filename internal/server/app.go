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
// 启动流程：
//  1. 如果配置了后台 Runner，以 goroutine 方式并发启动。
//  2. 在 Gin 引擎上监听 Server.Port 端口。
//  3. 当任意后台 Runner 失败或 Gin 退出时，Run() 返回。
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
