package counter

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

// ============================================================================
// Stub handler dependencies
// ============================================================================

type stubHandlerCounter struct {
	likeFn      func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	unlikeFn    func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	favFn       func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	unfavFn     func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	getCountsFn func(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	isLikedFn   func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	isFavedFn   func(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
}

func (s *stubHandlerCounter) Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	if s.likeFn == nil {
		return false, nil
	}
	return s.likeFn(ctx, userID, entityType, entityID)
}
func (s *stubHandlerCounter) Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	if s.unlikeFn == nil {
		return false, nil
	}
	return s.unlikeFn(ctx, userID, entityType, entityID)
}
func (s *stubHandlerCounter) Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	if s.favFn == nil {
		return false, nil
	}
	return s.favFn(ctx, userID, entityType, entityID)
}
func (s *stubHandlerCounter) Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	if s.unfavFn == nil {
		return false, nil
	}
	return s.unfavFn(ctx, userID, entityType, entityID)
}
func (s *stubHandlerCounter) GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error) {
	if s.getCountsFn == nil {
		return nil, nil
	}
	return s.getCountsFn(ctx, entityType, entityID, metrics)
}
func (s *stubHandlerCounter) IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	if s.isLikedFn == nil {
		return false, nil
	}
	return s.isLikedFn(ctx, userID, entityType, entityID)
}
func (s *stubHandlerCounter) IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	if s.isFavedFn == nil {
		return false, nil
	}
	return s.isFavedFn(ctx, userID, entityType, entityID)
}

// ============================================================================
// Helpers
// ============================================================================

func setupHandlerTest(method, target string, body any, setUserID bool) (*httptest.ResponseRecorder, *gin.Context) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	var req *http.Request
	if body != nil {
		b, _ := json.Marshal(body)
		req = httptest.NewRequest(method, target, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	c.Request = req

	if setUserID {
		c.Set("user_id", uint64(42))
	}
	return w, c
}

func readSuccessData(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp struct {
		Code    int            `json:"code"`
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp.Data
}

func readFailCode(t *testing.T, w *httptest.ResponseRecorder) (int, string) {
	t.Helper()
	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp.Code, resp.Message
}

// ============================================================================
// Toggle tests (Like / Unlike / Fav / Unfav)
// ============================================================================

func TestHandlerLike_Success(t *testing.T) {
	svc := &stubHandlerCounter{
		likeFn: func(_ context.Context, userID uint64, entityType, entityID string) (bool, error) {
			if userID != 42 || entityType != "post" || entityID != "1" {
				t.Errorf("unexpected args: userID=%d type=%s id=%s", userID, entityType, entityID)
			}
			return true, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("POST", "/counter/like", ToggleRequest{EntityType: "post", EntityID: "1"}, true)
	handler.Like(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
	data := readSuccessData(t, w)
	if data["success"] != true {
		t.Error("success != true")
	}
	if data["changed"] != true {
		t.Error("changed != true")
	}
}

func TestHandlerLike_Unauthenticated(t *testing.T) {
	handler := NewCounterHandler(&stubHandlerCounter{})
	w, c := setupHandlerTest("POST", "/counter/like", ToggleRequest{EntityType: "post", EntityID: "1"}, false)
	handler.Like(c)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want=401", w.Code)
	}
}

func TestHandlerLike_InvalidBody(t *testing.T) {
	handler := NewCounterHandler(&stubHandlerCounter{})
	w, c := setupHandlerTest("POST", "/counter/like", map[string]string{"wrong": "field"}, true)
	handler.Like(c)

	code, msg := readFailCode(t, w)
	if code != 400 || msg != "invalid request" {
		t.Fatalf("got code=%d msg=%s want 400+invalid request", code, msg)
	}
}

func TestHandlerLike_ServiceError(t *testing.T) {
	svc := &stubHandlerCounter{
		likeFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) {
			return false, errors.New("redis down")
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("POST", "/counter/like", ToggleRequest{EntityType: "post", EntityID: "1"}, true)
	handler.Like(c)

	code, msg := readFailCode(t, w)
	if code != 500 {
		t.Fatalf("got code=%d msg=%s want 500", code, msg)
	}
}

func TestHandlerUnlike_Success(t *testing.T) {
	svc := &stubHandlerCounter{
		unlikeFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) {
			return true, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("POST", "/counter/unlike", ToggleRequest{EntityType: "post", EntityID: "1"}, true)
	handler.Unlike(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
}

func TestHandlerUnlike_NoChange(t *testing.T) {
	svc := &stubHandlerCounter{
		unlikeFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) {
			return false, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("POST", "/counter/unlike", ToggleRequest{EntityType: "post", EntityID: "1"}, true)
	handler.Unlike(c)

	data := readSuccessData(t, w)
	if data["changed"] != false {
		t.Error("expected changed=false for no-op unlike")
	}
}

func TestHandlerFav_Success(t *testing.T) {
	svc := &stubHandlerCounter{
		favFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) {
			return true, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("POST", "/counter/fav", ToggleRequest{EntityType: "post", EntityID: "1"}, true)
	handler.Fav(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
}

func TestHandlerUnfav_Success(t *testing.T) {
	svc := &stubHandlerCounter{
		unfavFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) {
			return true, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("POST", "/counter/unfav", ToggleRequest{EntityType: "post", EntityID: "1"}, true)
	handler.Unfav(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
}

// ============================================================================
// GetCounts tests
// ============================================================================

func TestGetCounts_Success(t *testing.T) {
	svc := &stubHandlerCounter{
		getCountsFn: func(_ context.Context, entityType, entityID string, metrics []string) (map[string]int32, error) {
			return map[string]int32{"like": 10, "fav": 5}, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("GET", "/counter/counts?entity_type=post&entity_id=1&metrics=like,fav", nil, false)
	handler.GetCounts(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
	data := readSuccessData(t, w)
	inner := data["data"].(map[string]any)
	if inner["like"].(float64) != 10 || inner["fav"].(float64) != 5 {
		t.Fatalf("unexpected counts: %+v", inner)
	}
}

func TestGetCounts_DefaultMetrics(t *testing.T) {
	svc := &stubHandlerCounter{
		getCountsFn: func(_ context.Context, _, _ string, metrics []string) (map[string]int32, error) {
			if len(metrics) != 2 || metrics[0] != "like" || metrics[1] != "fav" {
				t.Fatalf("unexpected metrics: %v", metrics)
			}
			return map[string]int32{"like": 3, "fav": 1}, nil
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("GET", "/counter/counts?entity_type=post&entity_id=2", nil, false)
	handler.GetCounts(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
}

func TestGetCounts_MissingParams(t *testing.T) {
	handler := NewCounterHandler(&stubHandlerCounter{})

	tests := []struct {
		name   string
		query  string
	}{
		{"no entity_type", "entity_id=1"},
		{"no entity_id", "entity_type=post"},
		{"both empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, c := setupHandlerTest("GET", "/counter/counts?"+tt.query, nil, false)
			handler.GetCounts(c)

			code, msg := readFailCode(t, w)
			if code != 400 || msg != "entity_type and entity_id are required" {
				t.Fatalf("got code=%d msg=%s", code, msg)
			}
		})
	}
}

func TestGetCounts_ServiceError(t *testing.T) {
	svc := &stubHandlerCounter{
		getCountsFn: func(_ context.Context, _, _ string, _ []string) (map[string]int32, error) {
			return nil, errors.New("redis error")
		},
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("GET", "/counter/counts?entity_type=post&entity_id=1", nil, false)
	handler.GetCounts(c)

	code, msg := readFailCode(t, w)
	if code != 500 {
		t.Fatalf("got code=%d msg=%s want 500", code, msg)
	}
}

// ============================================================================
// Status tests
// ============================================================================

func TestStatus_Success(t *testing.T) {
	svc := &stubHandlerCounter{
		isLikedFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) { return true, nil },
		isFavedFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) { return false, nil },
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("GET", "/counter/status?entity_type=post&entity_id=1", nil, true)
	handler.Status(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", w.Code)
	}
	data := readSuccessData(t, w)
	if data["is_liked"] != true {
		t.Error("is_liked != true")
	}
	if data["is_faved"] != false {
		t.Error("is_faved != false")
	}
}

func TestStatus_Unauthenticated(t *testing.T) {
	handler := NewCounterHandler(&stubHandlerCounter{})
	w, c := setupHandlerTest("GET", "/counter/status?entity_type=post&entity_id=1", nil, false)
	handler.Status(c)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want=401", w.Code)
	}
}

func TestStatus_MissingParams(t *testing.T) {
	handler := NewCounterHandler(&stubHandlerCounter{})
	w, c := setupHandlerTest("GET", "/counter/status", nil, true)
	handler.Status(c)

	code, msg := readFailCode(t, w)
	if code != 400 || msg != "entity_type and entity_id are required" {
		t.Fatalf("got code=%d msg=%s", code, msg)
	}
}

func TestStatus_RedisErrorDegradesGracefully(t *testing.T) {
	svc := &stubHandlerCounter{
		isLikedFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) { return false, errors.New("redis error") },
		isFavedFn: func(_ context.Context, _ uint64, _, _ string) (bool, error) { return false, errors.New("redis error") },
	}
	handler := NewCounterHandler(svc)
	w, c := setupHandlerTest("GET", "/counter/status?entity_type=post&entity_id=1", nil, true)
	handler.Status(c)

	data := readSuccessData(t, w)
	if data["is_liked"] != false || data["is_faved"] != false {
		t.Fatalf("expected both false on redis error, got %+v", data)
	}
}