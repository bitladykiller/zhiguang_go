// server 包是 HTTP 应用的顶层容器与启动入口。
// 它持有 Gin 路由引擎、全局配置和后台 Runner 集合，
// 通过统一的 Run() 方法启动服务和所有后台 goroutine。
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
//   - canal.Bridge：Canal binlog → Kafka 的桥接器
//   - search.OutboxConsumer：搜索索引异步同步消费者
//   - relation.OutboxConsumer：关系事件异步同步消费者
type BackgroundRunner interface {
	Start(ctx context.Context)
}

// CleanupFunc 定义应用退出时需要执行的资源清理函数。
//
// 约定：
//   - cleanup 在所有后台 runner 接收到 cancel 并开始退出后执行。
//   - cleanup 不应 panic；若返回错误，会被收集并作为 Run 的返回值之一。
type CleanupFunc func(ctx context.Context) error

// App 是顶层应用容器。
// 它持有 Gin 路由、全局配置以及统一的 Run() 启动入口。
type App struct {
	router     *gin.Engine
	config     *config.Config
	logger     *zap.Logger
	background []BackgroundRunner
	cleanup    []CleanupFunc
}

// NewApp 创建一个新的应用实例。
//
// 功能：
//
//	将路由引擎、配置、日志器和后台 Runner 组装为一个 App 实例。
//	采用函数选项模式（varargs）传递后台 Runner，使得 Runner 列表可扩展。
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
//
//	使用变长参数（...BackgroundRunner）而非切片参数，
//	使得调用时可以传入零个、一个或多个 Runner，API 更简洁：
//	  NewApp(router, cfg, logger)
//	  NewApp(router, cfg, logger, bridge, consumer1, consumer2)
func NewApp(router *gin.Engine, cfg *config.Config, logger *zap.Logger, background ...BackgroundRunner) *App {
	return &App{
		router:     router,
		config:     cfg,
		logger:     logger,
		background: background,
	}
}

// AddCleanup 注册应用关闭阶段需要执行的资源清理函数。
//
// 常见场景包括：
//   - 关闭 DB 连接池
//   - 关闭 Redis 客户端
//   - Flush/Close Kafka writer
func (a *App) AddCleanup(cleanup ...CleanupFunc) {
	a.cleanup = append(a.cleanup, cleanup...)
}

// Run 启动 HTTP 服务，并一直阻塞到服务退出。
//
// 功能（启动流程）：
//
//	Step 1: 监听 SIGINT / SIGTERM，并创建贯穿整个应用生命周期的根上下文。
//	Step 2: 启动所有后台 Runner，与 HTTP 服务共享同一个 root context。
//	Step 3: 启动 http.Server。
//	Step 4: 收到退出信号后先 cancel root context，再对 HTTP 服务执行优雅关闭。
//	Step 5: 等待后台 Runner 退出，并执行资源清理函数。
//
// 参数：
//   - 无（使用 App 实例持有的配置）
//
// 返回值：
//   - error: Gin 路由启动失败时返回（如端口被占用）
//
// 设计决策：
//
//	使用同一个 root context 驱动 HTTP 和所有后台任务，保证关闭顺序可控。
//	关闭时先 cancel runner，再 Shutdown HTTP，可以让消费者和桥接器尽快停止拉取新任务。
//
// 边界情况：
//   - Runner 为 nil：跳过（不启动），避免 nil goroutine 导致 panic
//   - 端口被占用：ListenAndServe 返回错误，程序退出
func (a *App) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return a.run(ctx)
}

const (
	shutdownTimeoutHTTP    = 5 * time.Second
	shutdownTimeoutRunners = 8 * time.Second
	shutdownTimeoutCleanup = 2 * time.Second
	shutdownTotalBuffer    = 500 * time.Millisecond
)

// run 在给定的生命周期上下文中启动 HTTP 服务和后台任务。
//
// 该函数拆出来是为了让单元测试可以直接传入可控 context，而不依赖真实操作系统信号。
func (a *App) run(parent context.Context) error {
	rootCtx, cancel := context.WithCancel(parent)
	defer cancel()

	addr := fmt.Sprintf(":%d", a.config.Server.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: a.router,
	}

	var runnerWG sync.WaitGroup
	for _, runner := range a.background {
		if runner == nil {
			continue
		}
		runnerWG.Add(1)
		go func(r BackgroundRunner) {
			defer runnerWG.Done()
			defer func() {
				if rec := recover(); rec != nil {
					a.logger.Error("background runner panicked",
						zap.Any("panic", rec),
						zap.Stack("stack"),
					)
				}
			}()
			r.Start(rootCtx)
		}(runner)
	}

	serverErrCh := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
			return
		}
		serverErrCh <- nil
	}()

	a.logger.Info("starting zhiguang server",
		zap.String("addr", addr),
		zap.String("mode", a.config.Server.Mode),
	)

	var listenErr error
	serverErrConsumed := false

	select {
	case <-rootCtx.Done():
	case listenErr = <-serverErrCh:
		serverErrConsumed = true
		if listenErr != nil {
			cancel()
		}
	}

	cancel()

	// Phase 1: Stop accepting new HTTP requests (graceful shutdown)
	shutdownHTTPCtx, shutdownHTTPCancel := context.WithTimeout(parent, shutdownTimeoutHTTP)
	shutdownErr := httpServer.Shutdown(shutdownHTTPCtx)
	shutdownHTTPCancel()
	if shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
		a.logger.Warn("http server shutdown failed", zap.Error(shutdownErr))
	}

	if !serverErrConsumed {
		listenErr = <-serverErrCh
	}

	// Phase 2: Wait for background runners to finish processing backlog
	runnersCtx, runnersCancel := context.WithTimeout(parent, shutdownTimeoutRunners)
	a.waitBackgroundRunners(runnersCtx, &runnerWG)
	runnersCancel()

	// Phase 3: Run cleanup functions
	cleanupCtx, cleanupCancel := context.WithTimeout(parent, shutdownTimeoutCleanup)
	cleanupErr := a.runCleanup(cleanupCtx)
	cleanupCancel()

	return a.aggregateErrors(listenErr, cleanupErr)
}

func (a *App) waitBackgroundRunners(ctx context.Context, wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
	case <-ctx.Done():
		a.logger.Warn("background runners did not exit before shutdown timeout")
	}
}

func (a *App) runCleanup(ctx context.Context) error {
	var errs []error
	for _, cleanup := range a.cleanup {
		if cleanup == nil {
			continue
		}
		if err := cleanup(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *App) aggregateErrors(listenErr error, cleanupErr error) error {
	var combined []error
	if listenErr != nil {
		combined = append(combined, listenErr)
	}
	if cleanupErr != nil {
		combined = append(combined, cleanupErr)
	}
	return errors.Join(combined...)
}
