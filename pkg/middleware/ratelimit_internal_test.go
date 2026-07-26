package middleware

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestClientIP_FallbacksParseIPv6 覆盖 ClientIP() 为空时的兜底解析。
//
// 历史实现用 strings.Split(RemoteAddr, ":")[0] 取地址，
// 而 IPv6 地址本身含冒号——"[::1]:8080" 会被截成 "["，
// 于是所有 IPv6 客户端共用同一个限流桶，彼此互相挤占配额。
// 这里改用 net.SplitHostPort，本用例锁住该行为。
func TestClientIP_FallbacksParseIPv6(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := &RateLimiter{}

	tests := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{"ipv4 with port", "192.0.2.10:54321", "192.0.2.10"},
		{"ipv6 loopback with port", "[::1]:8080", "::1"},
		{"ipv6 full with port", "[2001:db8::1]:443", "2001:db8::1"},
		{"no port at all", "192.0.2.77", "192.0.2.77"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("GET", "/", nil)
			c.Request.RemoteAddr = tt.remoteAddr
			// 清空可信来源头，并把 RemoteAddr 置为不可解析的形式之外的值，
			// 使 gin 的 ClientIP() 走到我们期望的兜底分支。
			c.Request.Header.Del("X-Forwarded-For")
			c.Request.Header.Del("X-Real-IP")

			if got := rl.clientIP(c); got != tt.want {
				t.Errorf("clientIP(%q) = %q, want %q", tt.remoteAddr, got, tt.want)
			}
		})
	}
}

// TestClientIP_EmptyGinResultUsesRemoteAddr 直接验证兜底分支本身。
func TestClientIP_EmptyGinResultUsesRemoteAddr(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := &RateLimiter{}

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/", nil)
	// 非法端口使 gin 的 ClientIP() 返回空串，从而进入兜底路径。
	c.Request.RemoteAddr = "not-an-address"

	if got := rl.clientIP(c); got != "not-an-address" {
		t.Errorf("clientIP = %q, want the raw RemoteAddr when it cannot be split", got)
	}
}

// TestNextRateLimitMember_Unique 验证成员标识逐次唯一。
//
// 这是滑动窗口能正确计数的前提：ZADD 对相同 member 只更新 score 而不新增元素，
// 一旦成员重复，同窗口内的多次请求会被折叠成一次，限流即失效。
func TestNextRateLimitMember_Unique(t *testing.T) {
	const n = 1000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		m := nextRateLimitMember()
		if _, dup := seen[m]; dup {
			t.Fatalf("duplicate rate limit member %q at iteration %d", m, i)
		}
		seen[m] = struct{}{}
	}
	if !strings.HasPrefix(nextRateLimitMember(), rateLimitMemberPrefix) {
		t.Error("member should carry the process-unique prefix")
	}
}
