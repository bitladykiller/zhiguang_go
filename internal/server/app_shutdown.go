package server

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const appShutdownTimeout = 15 * time.Second

func newShutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), appShutdownTimeout)
}

func (a *App) shutdownHTTP(httpServer *http.Server, ctx context.Context) error {
	shutdownErr := httpServer.Shutdown(ctx)
	if shutdownErr != nil &&
		!errors.Is(shutdownErr, http.ErrServerClosed) &&
		!errors.Is(shutdownErr, context.Canceled) {
		return shutdownErr
	}
	return nil
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
