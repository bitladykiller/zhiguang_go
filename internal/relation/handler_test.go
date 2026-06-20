package relation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// stubService implements RelationServiceInterface for handler testing.
type stubService struct {
	followFn           func(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	unfollowFn         func(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	followingFn        func(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	followersFn        func(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	followingCursorFn  func(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	followersCursorFn  func(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	relationStatusFn   func(ctx context.Context, fromUserID, toUserID uint64) (string, error)
	isFollowingFn      func(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
}

func (s *stubService) Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	return s.followFn(ctx, fromUserID, toUserID)
}

func (s *stubService) Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	return s.unfollowFn(ctx, fromUserID, toUserID)
}

func (s *stubService) Following(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.followingFn(ctx, userID, limit, offset)
}

func (s *stubService) Followers(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error) {
	return s.followersFn(ctx, userID, limit, offset)
}

func (s *stubService) FollowingCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.followingCursorFn(ctx, userID, limit, cursor)
}

func (s *stubService) FollowersCursor(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error) {
	return s.followersCursorFn(ctx, userID, limit, cursor)
}

func (s *stubService) RelationStatus(ctx context.Context, fromUserID, toUserID uint64) (string, error) {
	return s.relationStatusFn(ctx, fromUserID, toUserID)
}

func (s *stubService) IsFollowing(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	return s.isFollowingFn(ctx, fromUserID, toUserID)
}

func setupHandlerTest(stub *stubService, userID uint64) (*RelationHandler, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	handler := NewRelationHandler(stub)
	r := gin.New()

	if userID > 0 {
		r.Use(func(c *gin.Context) {
			c.Set("user_id", userID)
		})
	}

	rel := r.Group("/relations")
	{
		rel.POST("/follow", handler.Follow)
		rel.POST("/unfollow", handler.Unfollow)
		rel.GET("/status", handler.Status)
		rel.GET("/following", handler.Following)
		rel.GET("/followers", handler.Followers)
		rel.GET("/following/cursor", handler.FollowingCursor)
		rel.GET("/followers/cursor", handler.FollowersCursor)
	}
	return handler, r
}

func perform(r *gin.Engine, method, path string, body []byte) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

// ============================================================================
// Follow handler tests
// ============================================================================

func TestHandler_Follow_Success(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/follow", body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Follow_Unauthorized(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 0)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/follow", body)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandler_Follow_Self(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 1001})
	w := perform(r, "POST", "/relations/follow", body)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_Follow_RateLimited(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return false, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/follow", body)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", w.Code)
	}
}

func TestHandler_Follow_InternalError(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return false, errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/follow", body)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandler_Follow_InvalidBody(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "POST", "/relations/follow", []byte(`{invalid}`))

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_Follow_EmptyBody(t *testing.T) {
	stub := &stubService{
		followFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "POST", "/relations/follow", nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// ============================================================================
// Unfollow handler tests
// ============================================================================

func TestHandler_Unfollow_Success(t *testing.T) {
	stub := &stubService{
		unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/unfollow", body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Unfollow_AlreadyUnfollowed(t *testing.T) {
	stub := &stubService{
		unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return false, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/unfollow", body)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Unfollow_Unauthorized(t *testing.T) {
	stub := &stubService{
		unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 0)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/unfollow", body)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandler_Unfollow_InternalError(t *testing.T) {
	stub := &stubService{
		unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return false, errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	body, _ := json.Marshal(FollowRequest{ToUserID: 2})
	w := perform(r, "POST", "/relations/unfollow", body)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandler_Unfollow_InvalidBody(t *testing.T) {
	stub := &stubService{
		unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) {
			return true, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "POST", "/relations/unfollow", []byte(`not json`))

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// ============================================================================
// Status handler tests
// ============================================================================

func TestHandler_Status_Success(t *testing.T) {
	tests := []struct {
		name   string
		status string
	}{
		{"mutual", "mutual"},
		{"following", "following"},
		{"followed", "followed"},
		{"none", "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) {
					return tt.status, nil
				},
			}
			_, r := setupHandlerTest(stub, 1001)

			w := perform(r, "GET", "/relations/status?other_id=2", nil)

			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}
		})
	}
}

func TestHandler_Status_Unauthorized(t *testing.T) {
	stub := &stubService{
		relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) {
			return "none", nil
		},
	}
	_, r := setupHandlerTest(stub, 0)

	w := perform(r, "GET", "/relations/status?other_id=2", nil)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandler_Status_InvalidOtherID(t *testing.T) {
	stub := &stubService{
		relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) {
			return "none", nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/status?other_id=abc", nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_Status_MissingOtherID(t *testing.T) {
	stub := &stubService{
		relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) {
			return "none", nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/status", nil)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_Status_InternalError(t *testing.T) {
	stub := &stubService{
		relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) {
			return "", errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/status?other_id=2", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// ============================================================================
// Following handler tests (offset-based)
// ============================================================================

func TestHandler_Following_Success(t *testing.T) {
	stub := &stubService{
		followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return []uint64{10, 20, 30}, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following?user_id=1&limit=10&offset=0", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Following_DefaultParams(t *testing.T) {
	stub := &stubService{
		followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return []uint64{}, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following?user_id=1", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Following_Empty(t *testing.T) {
	stub := &stubService{
		followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return []uint64{}, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following?user_id=1", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Following_InternalError(t *testing.T) {
	stub := &stubService{
		followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return nil, errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following?user_id=1", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// ============================================================================
// Followers handler tests (offset-based)
// ============================================================================

func TestHandler_Followers_Success(t *testing.T) {
	stub := &stubService{
		followersFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return []uint64{100, 200}, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/followers?user_id=1", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_Followers_InternalError(t *testing.T) {
	stub := &stubService{
		followersFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return nil, errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/followers?user_id=1", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// ============================================================================
// FollowingCursor handler tests
// ============================================================================

func TestHandler_FollowingCursor_Success(t *testing.T) {
	stub := &stubService{
		followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return []uint64{30, 20}, 1000, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following/cursor?user_id=1&limit=2&cursor=0", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_FollowingCursor_LastPage(t *testing.T) {
	stub := &stubService{
		followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return []uint64{10}, 0, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following/cursor?user_id=1&limit=10&cursor=500", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_FollowingCursor_Empty(t *testing.T) {
	stub := &stubService{
		followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return []uint64{}, 0, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following/cursor?user_id=1", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_FollowingCursor_InternalError(t *testing.T) {
	stub := &stubService{
		followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return nil, 0, errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following/cursor?user_id=1", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// ============================================================================
// FollowersCursor handler tests
// ============================================================================

func TestHandler_FollowersCursor_Success(t *testing.T) {
	stub := &stubService{
		followersCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return []uint64{50, 40}, 2000, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/followers/cursor?user_id=1&limit=2", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_FollowersCursor_InternalError(t *testing.T) {
	stub := &stubService{
		followersCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return nil, 0, errors.New("db error")
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/followers/cursor?user_id=1", nil)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// ============================================================================
// Helper function tests
// ============================================================================

func TestQueryUint64(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test?user_id=12345", nil)

	id := queryUint64(c, "user_id")
	if id != 12345 {
		t.Fatalf("expected 12345, got %d", id)
	}
}

func TestQueryUint64_Missing(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test", nil)

	id := queryUint64(c, "user_id")
	if id != 0 {
		t.Fatalf("expected 0, got %d", id)
	}
}

func TestQueryUint64_Invalid(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test?user_id=abc", nil)

	id := queryUint64(c, "user_id")
	if id != 0 {
		t.Fatalf("expected 0, got %d", id)
	}
}

func TestQueryInt64(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test?cursor=98765", nil)

	v := queryInt64(c, "cursor")
	if v != 98765 {
		t.Fatalf("expected 98765, got %d", v)
	}
}

func TestQueryInt64_Missing(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test", nil)

	v := queryInt64(c, "cursor")
	if v != 0 {
		t.Fatalf("expected 0, got %d", v)
	}
}

func TestQueryInt64_Invalid(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test?cursor=abc", nil)

	v := queryInt64(c, "cursor")
	if v != 0 {
		t.Fatalf("expected 0, got %d", v)
	}
}

func TestHandler_Following_MissingUserID(t *testing.T) {
	stub := &stubService{
		followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
			return []uint64{}, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/following", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandler_FollowersCursor_DefaultCursor(t *testing.T) {
	stub := &stubService{
		followersCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
			return []uint64{}, 0, nil
		},
	}
	_, r := setupHandlerTest(stub, 1001)

	w := perform(r, "GET", "/relations/followers/cursor?user_id=1", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNewRelationHandler(t *testing.T) {
	stub := &stubService{}
	h := NewRelationHandler(stub)
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}
