package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

type testRunner struct {
	started chan struct{}
	stopped chan struct{}
}

func (r *testRunner) Start(ctx context.Context) {
	close(r.started)
	<-ctx.Done()
	close(r.stopped)
}

func TestAppRunCancelsRunnersAndExecutesCleanup(t *testing.T) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	runner := &testRunner{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}

	app := NewApp(router, &config.Config{
		Server: config.ServerConfig{
			Port: 0,
			Mode: "test",
		},
	}, zap.NewNop(), runner)

	cleanupCalled := make(chan struct{})
	app.AddCleanup(func(context.Context) error {
		close(cleanupCalled)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- app.run(ctx)
	}()

	select {
	case <-runner.started:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not start")
	}

	cancel()

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("app run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("app run did not exit after cancel")
	}

	select {
	case <-runner.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not observe shutdown cancellation")
	}

	select {
	case <-cleanupCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup callback was not executed")
	}
}
