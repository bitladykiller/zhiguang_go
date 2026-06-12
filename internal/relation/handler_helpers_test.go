package relation

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBindFollowRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"to_user_id":123}`))
		ctx.Request.Header.Set("Content-Type", "application/json")

		req, ok := bindFollowRequest(ctx)
		if !ok {
			t.Fatal("bindFollowRequest(valid) should succeed")
		}
		if req.ToUserID != 123 {
			t.Fatalf("ToUserID = %d, want 123", req.ToUserID)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		ctx.Request.Header.Set("Content-Type", "application/json")

		if _, ok := bindFollowRequest(ctx); ok {
			t.Fatal("bindFollowRequest(invalid) should fail")
		}
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}

func TestOtherIDQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/?other_id=42", nil)

		otherID, ok := otherIDQuery(ctx)
		if !ok {
			t.Fatal("otherIDQuery(valid) should succeed")
		}
		if otherID != 42 {
			t.Fatalf("otherID = %d, want 42", otherID)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/?other_id=bad", nil)

		if _, ok := otherIDQuery(ctx); ok {
			t.Fatal("otherIDQuery(invalid) should fail")
		}
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}

func TestRelationQueryHelpers(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/?limit=30&offset=bad&cursor=1001&user_id=77", nil)

	if got := queryInt(ctx, "limit", 20); got != 30 {
		t.Fatalf("queryInt(limit) = %d, want 30", got)
	}
	if got := queryInt(ctx, "offset", 5); got != 5 {
		t.Fatalf("queryInt(offset) = %d, want 5", got)
	}
	if got := queryInt64(ctx, "cursor"); got != 1001 {
		t.Fatalf("queryInt64(cursor) = %d, want 1001", got)
	}
	if got := queryUint64(ctx, "user_id"); got != 77 {
		t.Fatalf("queryUint64(user_id) = %d, want 77", got)
	}
}
