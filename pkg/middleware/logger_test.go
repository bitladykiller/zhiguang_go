package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func TestLoggerMiddleware_Success(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(LoggerMiddleware(logger))
	r.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestLoggerMiddleware_ClientError(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(LoggerMiddleware(logger))
	r.GET("/bad", func(c *gin.Context) {
		c.Status(http.StatusBadRequest)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bad", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestLoggerMiddleware_ServerError(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(LoggerMiddleware(logger))
	r.GET("/panic", func(c *gin.Context) {
		c.Status(http.StatusInternalServerError)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestLoggerMiddleware_SkipsHealth(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(LoggerMiddleware(logger))
	r.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestLoggerMiddleware_SkipsMetrics(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(LoggerMiddleware(logger))
	r.GET("/metrics", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestLoggerMiddleware_RecordsLatency(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	r := setupTest()
	r.Use(LoggerMiddleware(logger))
	r.GET("/slow", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/slow", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestLoggerMiddleware_WithNopLogger(t *testing.T) {
	r := setupTest()
	r.Use(LoggerMiddleware(zap.NewNop()))
	r.GET("/nop", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nop", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}