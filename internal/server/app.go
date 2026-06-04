// server 包负责 HTTP 应用的装配与启动。
package server

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

// BackgroundRunner 表示会伴随 HTTP 服务一起启动的长生命周期后台任务。
// 例如：搜索 outbox 同步、缓存预热器、异步轮询器等。
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
