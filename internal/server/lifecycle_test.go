package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

func TestAppRunCancelsBackgroundRunnerAndExecutesCleanup(t *testing.T) {
	runner := newTestRunner()
	cleanupCalled := make(chan struct{}, 1)

	app := newTestApp(t, runner)
	app.AddCleanup(func(ctx context.Context) error {
		select {
		case cleanupCalled <- struct{}{}:
		default:
		}
		return nil
	})

	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.run(parentCtx)
	}()

	waitForSignal(t, runner.started, "runner started")
	cancel()

	if err := waitForResult(t, errCh); err != nil {
		t.Fatalf("expected app run to exit cleanly, got %v", err)
	}
	waitForSignal(t, runner.stopped, "runner stopped")
	waitForSignal(t, cleanupCalled, "cleanup called")
}

func TestAppRunReturnsCleanupError(t *testing.T) {
	runner := newTestRunner()
	cleanupErr := errors.New("cleanup failed")

	app := newTestApp(t, runner)
	app.AddCleanup(func(ctx context.Context) error {
		return cleanupErr
	})

	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.run(parentCtx)
	}()

	waitForSignal(t, runner.started, "runner started")
	cancel()

	err := waitForResult(t, errCh)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("expected cleanup error %v, got %v", cleanupErr, err)
	}
	waitForSignal(t, runner.stopped, "runner stopped")
}

type testRunner struct {
	started chan struct{}
	stopped chan struct{}
}

func newTestRunner() *testRunner {
	return &testRunner{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (r *testRunner) Start(ctx context.Context) {
	close(r.started)
	<-ctx.Done()
	close(r.stopped)
}

func newTestApp(t *testing.T, runner BackgroundRunner) *App {
	t.Helper()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/health", func(c *gin.Context) {
		c.Status(200)
	})

	cfg := &config.Config{
		Server: config.ServerConfig{
			Port: 0,
			Mode: "test",
		},
	}

	return NewApp(router, cfg, zap.NewNop(), runner)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitForResult(t *testing.T, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for app run to finish")
		return nil
	}
}
