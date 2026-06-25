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
	followFn          func(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	unfollowFn        func(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
	followingFn       func(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	followersFn       func(ctx context.Context, userID uint64, limit, offset int) ([]uint64, error)
	followingCursorFn func(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	followersCursorFn func(ctx context.Context, userID uint64, limit int, cursor int64) ([]uint64, int64, error)
	relationStatusFn  func(ctx context.Context, fromUserID, toUserID uint64) (string, error)
	isFollowingFn     func(ctx context.Context, fromUserID, toUserID uint64) (bool, error)
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

// mustMarshal is a test helper that JSON-encodes v or panics.
func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ============================================================================
// Follow handler tests (table-driven)
// ============================================================================

// followTestCase 定义 Follow / Unfollow 测试的通用结构。
type followTestCase struct {
	name       string
	stub       *stubService
	userID     uint64
	reqBody    []byte
	wantStatus int
}

func TestHandler_Follow(t *testing.T) {
	tests := []followTestCase{
		{
			name: "success",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusOK,
		},
		{
			name: "unauthorized",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     0,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "self",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 1001}),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "rate_limited",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return false, nil },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusTooManyRequests,
		},
		{
			name: "internal_error",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return false, errors.New("db error") },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "invalid_body",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     1001,
			reqBody:    []byte(`{invalid}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "empty_body",
			stub: &stubService{
				followFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     1001,
			reqBody:    nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "POST", "/relations/follow", tt.reqBody)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

// ============================================================================
// Unfollow handler tests (table-driven)
// ============================================================================

func TestHandler_Unfollow(t *testing.T) {
	tests := []followTestCase{
		{
			name: "success",
			stub: &stubService{
				unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusOK,
		},
		{
			name: "already_unfollowed",
			stub: &stubService{
				unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) { return false, nil },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusOK,
		},
		{
			name: "unauthorized",
			stub: &stubService{
				unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     0,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "internal_error",
			stub: &stubService{
				unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) { return false, errors.New("db error") },
			},
			userID:     1001,
			reqBody:    mustMarshal(FollowRequest{ToUserID: 2}),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "invalid_body",
			stub: &stubService{
				unfollowFn: func(_ context.Context, _, _ uint64) (bool, error) { return true, nil },
			},
			userID:     1001,
			reqBody:    []byte(`not json`),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "POST", "/relations/unfollow", tt.reqBody)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

// ============================================================================
// Status handler tests (table-driven)
// ============================================================================

// statusTestCase 定义 Status 测试的结构。
type statusTestCase struct {
	name       string
	stub       *stubService
	userID     uint64
	query      string
	wantStatus int
}

func TestHandler_Status(t *testing.T) {
	tests := []statusTestCase{
		{
			name: "mutual",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "mutual", nil },
			},
			userID:     1001,
			query:      "?other_id=2",
			wantStatus: http.StatusOK,
		},
		{
			name: "following",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "following", nil },
			},
			userID:     1001,
			query:      "?other_id=2",
			wantStatus: http.StatusOK,
		},
		{
			name: "followed",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "followed", nil },
			},
			userID:     1001,
			query:      "?other_id=2",
			wantStatus: http.StatusOK,
		},
		{
			name: "none",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "none", nil },
			},
			userID:     1001,
			query:      "?other_id=2",
			wantStatus: http.StatusOK,
		},
		{
			name: "unauthorized",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "none", nil },
			},
			userID:     0,
			query:      "?other_id=2",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid_other_id",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "none", nil },
			},
			userID:     1001,
			query:      "?other_id=abc",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_other_id",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "none", nil },
			},
			userID:     1001,
			query:      "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "internal_error",
			stub: &stubService{
				relationStatusFn: func(_ context.Context, _, _ uint64) (string, error) { return "", errors.New("db error") },
			},
			userID:     1001,
			query:      "?other_id=2",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "GET", "/relations/status"+tt.query, nil)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

// ============================================================================
// Following handler tests (offset-based, table-driven)
// ============================================================================

// listTestCase 定义 Following / Followers / FollowingCursor / FollowersCursor 测试的通用结构。
type listTestCase struct {
	name       string
	stub       *stubService
	userID     uint64
	path       string
	wantStatus int
}

func TestHandler_Following(t *testing.T) {
	tests := []listTestCase{
		{
			name: "success",
			stub: &stubService{
				followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return []uint64{10, 20, 30}, nil
				},
			},
			userID:     1001,
			path:       "/relations/following?user_id=1&limit=10&offset=0",
			wantStatus: http.StatusOK,
		},
		{
			name: "default_params",
			stub: &stubService{
				followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return []uint64{}, nil
				},
			},
			userID:     1001,
			path:       "/relations/following?user_id=1",
			wantStatus: http.StatusOK,
		},
		{
			name: "empty",
			stub: &stubService{
				followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return []uint64{}, nil
				},
			},
			userID:     1001,
			path:       "/relations/following?user_id=1",
			wantStatus: http.StatusOK,
		},
		{
			name: "internal_error",
			stub: &stubService{
				followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return nil, errors.New("db error")
				},
			},
			userID:     1001,
			path:       "/relations/following?user_id=1",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "missing_user_id",
			stub: &stubService{
				followingFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return []uint64{}, nil
				},
			},
			userID:     1001,
			path:       "/relations/following",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "GET", tt.path, nil)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

// ============================================================================
// Followers handler tests (offset-based, table-driven)
// ============================================================================

func TestHandler_Followers(t *testing.T) {
	tests := []listTestCase{
		{
			name: "success",
			stub: &stubService{
				followersFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return []uint64{100, 200}, nil
				},
			},
			userID:     1001,
			path:       "/relations/followers?user_id=1",
			wantStatus: http.StatusOK,
		},
		{
			name: "internal_error",
			stub: &stubService{
				followersFn: func(_ context.Context, _ uint64, limit, offset int) ([]uint64, error) {
					return nil, errors.New("db error")
				},
			},
			userID:     1001,
			path:       "/relations/followers?user_id=1",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "GET", tt.path, nil)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

// ============================================================================
// FollowingCursor handler tests (table-driven)
// ============================================================================

func TestHandler_FollowingCursor(t *testing.T) {
	tests := []listTestCase{
		{
			name: "success",
			stub: &stubService{
				followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return []uint64{30, 20}, 1000, nil
				},
			},
			userID:     1001,
			path:       "/relations/following/cursor?user_id=1&limit=2&cursor=0",
			wantStatus: http.StatusOK,
		},
		{
			name: "last_page",
			stub: &stubService{
				followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return []uint64{10}, 0, nil
				},
			},
			userID:     1001,
			path:       "/relations/following/cursor?user_id=1&limit=10&cursor=500",
			wantStatus: http.StatusOK,
		},
		{
			name: "empty",
			stub: &stubService{
				followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return []uint64{}, 0, nil
				},
			},
			userID:     1001,
			path:       "/relations/following/cursor?user_id=1",
			wantStatus: http.StatusOK,
		},
		{
			name: "internal_error",
			stub: &stubService{
				followingCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return nil, 0, errors.New("db error")
				},
			},
			userID:     1001,
			path:       "/relations/following/cursor?user_id=1",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "GET", tt.path, nil)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

// ============================================================================
// FollowersCursor handler tests (table-driven)
// ============================================================================

func TestHandler_FollowersCursor(t *testing.T) {
	tests := []listTestCase{
		{
			name: "success",
			stub: &stubService{
				followersCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return []uint64{50, 40}, 2000, nil
				},
			},
			userID:     1001,
			path:       "/relations/followers/cursor?user_id=1&limit=2",
			wantStatus: http.StatusOK,
		},
		{
			name: "internal_error",
			stub: &stubService{
				followersCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return nil, 0, errors.New("db error")
				},
			},
			userID:     1001,
			path:       "/relations/followers/cursor?user_id=1",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "default_cursor",
			stub: &stubService{
				followersCursorFn: func(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
					return []uint64{}, 0, nil
				},
			},
			userID:     1001,
			path:       "/relations/followers/cursor?user_id=1",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, r := setupHandlerTest(tt.stub, tt.userID)
			w := perform(r, "GET", tt.path, nil)
			if w.Code != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, w.Code)
			}
		})
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

func TestNewRelationHandler(t *testing.T) {
	stub := &stubService{}
	h := NewRelationHandler(stub)
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}
