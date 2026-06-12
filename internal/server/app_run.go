package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"
)

// Run 启动 HTTP 服务，并一直阻塞到服务退出。
func (a *App) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return a.run(ctx)
}

// run 在给定的生命周期上下文中启动 HTTP 服务和后台任务。
//
// 拆成独立方法是为了让测试可以直接传入可控 context，
// 不依赖真实操作系统信号。
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

	listenErr, consumed := waitServerExit(rootCtx, serverErrCh, cancel)
	shutdownCtx, shutdownCancel := newShutdownContext()
	defer shutdownCancel()

	shutdownErr := a.shutdownHTTP(httpServer, shutdownCtx)
	if shutdownErr != nil {
		a.logger.Warn("http server shutdown failed", zap.Error(shutdownErr))
	}

	if !consumed {
		listenErr = <-serverErrCh
	}

	a.waitBackgroundRunners(shutdownCtx, &runnerWG)
	cleanupErr := a.runCleanup(shutdownCtx)

	if listenErr != nil && cleanupErr != nil {
		return errors.Join(listenErr, cleanupErr)
	}
	if listenErr != nil {
		return listenErr
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	return nil
}

func waitServerExit(rootCtx context.Context, serverErrCh <-chan error, cancel context.CancelFunc) (error, bool) {
	var listenErr error
	var consumed bool

	select {
	case <-rootCtx.Done():
	case listenErr = <-serverErrCh:
		consumed = true
		if listenErr != nil {
			cancel()
		}
	}

	cancel()
	return listenErr, consumed
}
