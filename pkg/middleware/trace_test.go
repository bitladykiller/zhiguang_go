package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestTraceMiddleware_GeneratesTraceID(t *testing.T) {
	r := setupTest()
	r.Use(TraceMiddleware(0))
	r.GET("/test", func(c *gin.Context) {
		traceID := GetTraceID(c)
		if traceID == "" {
			t.Error("expected non-empty trace ID")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	traceID := w.Header().Get(TraceIDHeader)
	if traceID == "" {
		t.Error("expected X-Trace-ID response header")
	}
}

func TestTraceMiddleware_PropagatesTraceID(t *testing.T) {
	r := setupTest()
	r.Use(TraceMiddleware(0))
	r.GET("/propagate", func(c *gin.Context) {
		traceID := GetTraceID(c)
		c.String(http.StatusOK, traceID)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/propagate", nil)
	req.Header.Set(TraceIDHeader, "custom-trace-123")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "custom-trace-123" {
		t.Errorf("body = %q, want custom-trace-123", w.Body.String())
	}
	if w.Header().Get(TraceIDHeader) != "custom-trace-123" {
		t.Errorf("response X-Trace-ID = %q, want custom-trace-123", w.Header().Get(TraceIDHeader))
	}
}

func TestTraceMiddleware_WithTimeout(t *testing.T) {
	r := setupTest()
	r.Use(TraceMiddleware(100 * time.Millisecond))
	r.GET("/timeout", func(c *gin.Context) {
		deadline, ok := c.Request.Context().Deadline()
		if !ok {
			t.Error("expected deadline with timeout")
		}
		if time.Until(deadline) > 100*time.Millisecond {
			t.Error("deadline too far in the future")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/timeout", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestTraceMiddleware_ZeroTimeout(t *testing.T) {
	r := setupTest()
	r.Use(TraceMiddleware(0))
	r.GET("/no-timeout", func(c *gin.Context) {
		_, ok := c.Request.Context().Deadline()
		if ok {
			t.Error("expected no deadline with zero timeout")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/no-timeout", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestTraceMiddleware_NegativeTimeout(t *testing.T) {
	r := setupTest()
	r.Use(TraceMiddleware(-1))
	r.GET("/neg-timeout", func(c *gin.Context) {
		_, ok := c.Request.Context().Deadline()
		if ok {
			t.Error("expected no deadline with negative timeout")
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/neg-timeout", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestGetTraceID_NotSet(t *testing.T) {
	c := &gin.Context{}
	traceID := GetTraceID(c)
	if traceID != "" {
		t.Errorf("expected empty, got %q", traceID)
	}
}

func TestGetTraceID_WrongType(t *testing.T) {
	c := &gin.Context{}
	c.Set(contextKeyTraceID, 12345)
	traceID := GetTraceID(c)
	if traceID != "" {
		t.Errorf("expected empty for non-string type, got %q", traceID)
	}
}
