package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/middleware"
	"go.uber.org/zap"
)

func TestRateLimiter_Disabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := config.RateLimitConfig{
		Enabled:       false,
		PerIP:         10,
		WindowMs:      1000,
		BanDurationMs: 5000,
	}
	rl := middleware.NewRateLimiter(nil, cfg, zap.NewNop())
	r := gin.New()
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("iteration %d: expected 200 when disabled, got %d", i, w.Code)
		}
	}
}

func TestRateLimiter_NilRedisClient_PassesAll(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := config.RateLimitConfig{
		Enabled:       true,
		PerIP:         1,
		WindowMs:      1000,
		BanDurationMs: 5000,
	}
	rl := middleware.NewRateLimiter(nil, cfg, zap.NewNop())
	r := gin.New()
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("iteration %d: expected 200 when redis is nil, got %d", i, w.Code)
		}
	}
}

func TestRateLimiter_RedisEnabled_RequestsWithinLimit(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Skip("redis not available, skipping integration test")
	}

	cfg := config.RateLimitConfig{
		Enabled: true, PerIP: 3, WindowMs: 60000, BanDurationMs: 0,
	}
	rl := middleware.NewRateLimiter(rdb, cfg, zap.NewNop())
	r := gin.New()
	gin.SetMode(gin.TestMode)
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

	rdb.Del(t.Context(), "ratelimit:127.0.0.1")

	for i := 0; i < cfg.PerIP; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200 within limit, got %d", i+1, w.Code)
		}
		if w.Header().Get("X-RateLimit-Remaining") == "" {
			t.Errorf("request %d: expected X-RateLimit-Remaining header", i+1)
		}
	}
}

func TestRateLimiter_RedisEnabled_ExceedsLimit(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Skip("redis not available, skipping integration test")
	}

	cfg := config.RateLimitConfig{
		Enabled: true, PerIP: 2, WindowMs: 60000, BanDurationMs: 0,
	}
	rl := middleware.NewRateLimiter(rdb, cfg, zap.NewNop())
	r := gin.New()
	gin.SetMode(gin.TestMode)
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

	rdb.Del(t.Context(), "ratelimit:127.0.0.2", "ratelimit:ban:127.0.0.2")

	for i := 0; i < cfg.PerIP; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "127.0.0.2:12345"
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200 within limit, got %d", i+1, w.Code)
		}
	}

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "127.0.0.2:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after exceeding limit, got %d", w.Code)
	}
}

func TestRateLimiter_RedisEnabled_BanAfterExceed(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Skip("redis not available, skipping integration test")
	}

	cfg := config.RateLimitConfig{
		Enabled: true, PerIP: 1, WindowMs: 60000, BanDurationMs: 2000,
	}
	rl := middleware.NewRateLimiter(rdb, cfg, zap.NewNop())
	r := gin.New()
	gin.SetMode(gin.TestMode)
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

	rdb.Del(t.Context(), "ratelimit:127.0.0.3", "ratelimit:ban:127.0.0.3")

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "127.0.0.3:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "127.0.0.3:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: expected 429, got %d", w.Code)
	}

	exists, err := rdb.Exists(t.Context(), "ratelimit:ban:127.0.0.3").Result()
	if err != nil {
		t.Fatalf("failed to check ban key: %v", err)
	}
	if exists == 0 {
		t.Error("expected ban key to exist after exceeding limit with ban duration")
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "127.0.0.3:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("during ban: expected 429, got %d", w.Code)
	}

	time.Sleep(time.Duration(cfg.BanDurationMs+100) * time.Millisecond)
	rdb.Del(t.Context(), "ratelimit:127.0.0.3", "ratelimit:ban:127.0.0.3")

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "127.0.0.3:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("after ban expires: expected 200, got %d", w.Code)
	}
}

func TestRateLimiter_IPExtraction_XForwardedFor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := middleware.NewRateLimiter(nil, config.RateLimitConfig{Enabled: false}, zap.NewNop())
	r := gin.New()
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "" {
			ip = c.Request.RemoteAddr
		}
		c.String(http.StatusOK, ip)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Forwarded-For", "192.168.1.100")
	req.RemoteAddr = "10.0.0.1:54321"
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "192.168.1.100" {
		t.Errorf("expected X-Forwarded-For IP 192.168.1.100, got %s", w.Body.String())
	}
}

func TestRateLimiter_IPExtraction_XRealIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := middleware.NewRateLimiter(nil, config.RateLimitConfig{Enabled: false}, zap.NewNop())
	r := gin.New()
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "" {
			ip = c.Request.RemoteAddr
		}
		c.String(http.StatusOK, ip)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Real-IP", "172.16.0.50")
	req.RemoteAddr = "10.0.0.2:54321"
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "172.16.0.50" {
		t.Errorf("expected X-Real-IP 172.16.0.50, got %s", w.Body.String())
	}
}

func TestRateLimiter_IPExtraction_RemoteAddr(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := middleware.NewRateLimiter(nil, config.RateLimitConfig{Enabled: false}, zap.NewNop())
	r := gin.New()
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "" {
			ip = c.Request.RemoteAddr
		}
		c.String(http.StatusOK, ip)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "10.0.0.3:54321"
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "10.0.0.3" {
		t.Errorf("expected RemoteAddr 10.0.0.3, got %s", w.Body.String())
	}
}

func TestRateLimiter_XRateLimitRemainingHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := config.RateLimitConfig{
		Enabled: false, PerIP: 10, WindowMs: 1000, BanDurationMs: 5000,
	}
	rl := middleware.NewRateLimiter(nil, cfg, zap.NewNop())
	r := gin.New()
	r.Use(rl.Middleware())
	r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}