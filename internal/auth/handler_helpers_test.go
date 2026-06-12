package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBindJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type payload struct {
		Name string `json:"name" binding:"required"`
	}

	t.Run("valid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"alice"}`))
		ctx.Request.Header.Set("Content-Type", "application/json")

		var req payload
		if ok := bindJSON(ctx, &req); !ok {
			t.Fatal("bindJSON(valid) should succeed")
		}
		if req.Name != "alice" {
			t.Fatalf("req.Name = %q, want alice", req.Name)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		ctx.Request.Header.Set("Content-Type", "application/json")

		var req payload
		if ok := bindJSON(ctx, &req); ok {
			t.Fatal("bindJSON(invalid) should fail")
		}
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}

func TestCurrentUserID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("missing", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)

		if _, ok := currentUserID(ctx); ok {
			t.Fatal("currentUserID() should fail when context has no user_id")
		}
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
		}
	})

	t.Run("present", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Set("user_id", uint64(7))

		userID, ok := currentUserID(ctx)
		if !ok {
			t.Fatal("currentUserID() should succeed when context has user_id")
		}
		if userID != 7 {
			t.Fatalf("userID = %d, want 7", userID)
		}
	})
}

func TestExtractClientInfo(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.10:12345"
	req.Header.Set("User-Agent", "unit-test-agent")
	ctx.Request = req

	info := extractClientInfo(ctx)
	if info.IP != "203.0.113.10" {
		t.Fatalf("IP = %q, want %q", info.IP, "203.0.113.10")
	}
	if info.UserAgent != "unit-test-agent" {
		t.Fatalf("UserAgent = %q, want %q", info.UserAgent, "unit-test-agent")
	}
}
