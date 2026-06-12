// server 包是 HTTP 应用的顶层容器与启动入口。
package server

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

// BackgroundRunner 表示会伴随 HTTP 服务一起启动的长生命周期后台任务。
type BackgroundRunner interface {
	Start(ctx context.Context)
}

// CleanupFunc 定义应用退出时需要执行的资源清理函数。
type CleanupFunc func(ctx context.Context) error

// App 是顶层应用容器。
//
// 当前按职责拆分为：
//   - app.go: 容器结构、构造函数、基础注册
//   - app_run.go: Run 主流程与 HTTP/runner 生命周期编排
//   - app_shutdown.go: runner 等待与 cleanup 收敛
//
// 这样做的目的是把“容器定义”和“运行时状态机”拆开，
// 避免生命周期控制代码重新长成单文件。
type App struct {
	router     *gin.Engine
	config     *config.Config
	logger     *zap.Logger
	background []BackgroundRunner
	cleanup    []CleanupFunc
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

// AddCleanup 注册应用关闭阶段需要执行的资源清理函数。
func (a *App) AddCleanup(cleanup ...CleanupFunc) {
	a.cleanup = append(a.cleanup, cleanup...)
}
