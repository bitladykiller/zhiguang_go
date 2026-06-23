package search

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhiguang/app/internal/knowpost"
)

// ---------------------------------------------------------------------------
// mock SearchServiceInterface
// ---------------------------------------------------------------------------

type mockSearchService struct {
	searchFunc   func(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	suggestFunc  func(ctx context.Context, prefix string, size int) ([]string, error)
}

func (m *mockSearchService) Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error) {
	return m.searchFunc(ctx, keyword, size, tagsCSV, after, currentUserID)
}

func (m *mockSearchService) Suggest(ctx context.Context, prefix string, size int) ([]string, error) {
	return m.suggestFunc(ctx, prefix, size)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func setupRouter(h *SearchHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	rg := r.Group("/api/v1")
	h.RegisterRoutes(rg)
	return r
}

func performRequest(r http.Handler, method, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	r.ServeHTTP(w, req)
	return w
}

// ---------------------------------------------------------------------------
// Search handler
// ---------------------------------------------------------------------------

func TestSearchHandler_NilService(t *testing.T) {
	h := NewSearchHandler(nil)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search?q=golang")
	if w.Code != 503 {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestSearchHandler_MissingQ(t *testing.T) {
	svc := &mockSearchService{}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search")
	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestSearchHandler_ServiceError(t *testing.T) {
	svc := &mockSearchService{
		searchFunc: func(_ context.Context, _ string, _ int, _, _ string, _ *uint64) (*SearchResponse, error) {
			return nil, errors.New("es down")
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search?q=golang")
	if w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestSearchHandler_Success(t *testing.T) {
	svc := &mockSearchService{
		searchFunc: func(_ context.Context, keyword string, size int, tagsCSV, after string, _ *uint64) (*SearchResponse, error) {
			if keyword != "golang" {
				t.Errorf("keyword = %q, want 'golang'", keyword)
			}
			if size != 20 {
				t.Errorf("size = %d, want 20", size)
			}
			return &SearchResponse{
				Items: []knowpost.FeedItemResponse{
					{ID: "1", Title: strPtr("Go入门"), AuthorNickname: "Alice"},
				},
				HasMore: false,
			}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search?q=golang")
	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var body struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Data    *SearchResponse  `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal error = %v", err)
	}
	if body.Code != 0 {
		t.Errorf("code = %d, want 0", body.Code)
	}
	if body.Data == nil {
		t.Fatal("data is nil")
	}
	if len(body.Data.Items) != 1 {
		t.Fatalf("len(items) = %d", len(body.Data.Items))
	}
	if body.Data.Items[0].ID != "1" {
		t.Errorf("item.ID = %q", body.Data.Items[0].ID)
	}
}

func TestSearchHandler_WithAllParams(t *testing.T) {
	var capturedKeyword, capturedTags, capturedAfter string
	var capturedSize int

	svc := &mockSearchService{
		searchFunc: func(_ context.Context, keyword string, size int, tagsCSV, after string, _ *uint64) (*SearchResponse, error) {
			capturedKeyword = keyword
			capturedSize = size
			capturedTags = tagsCSV
			capturedAfter = after
			return &SearchResponse{}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search?q=go&size=5&tags=go,redis&after=abc123")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	if capturedKeyword != "go" {
		t.Errorf("keyword = %q", capturedKeyword)
	}
	if capturedSize != 5 {
		t.Errorf("size = %d", capturedSize)
	}
	if capturedTags != "go,redis" {
		t.Errorf("tags = %q", capturedTags)
	}
	if capturedAfter != "abc123" {
		t.Errorf("after = %q", capturedAfter)
	}
}

func TestSearchHandler_InvalidSize(t *testing.T) {
	svc := &mockSearchService{
		searchFunc: func(_ context.Context, _ string, size int, _, _ string, _ *uint64) (*SearchResponse, error) {
			if size != 20 {
				t.Errorf("size = %d, want default 20 when invalid", size)
			}
			return &SearchResponse{}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search?q=go&size=-1")
	if w.Code != 200 {
		t.Errorf("status = %d", w.Code)
	}
}

func TestSearchHandler_ZeroSize(t *testing.T) {
	svc := &mockSearchService{
		searchFunc: func(_ context.Context, _ string, size int, _, _ string, _ *uint64) (*SearchResponse, error) {
			if size != 20 {
				t.Errorf("size = %d, want default 20", size)
			}
			return &SearchResponse{}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search?q=go&size=0")
	if w.Code != 200 {
		t.Errorf("status = %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Suggest handler
// ---------------------------------------------------------------------------

func TestSuggestHandler_NilService(t *testing.T) {
	h := NewSearchHandler(nil)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search/suggest?prefix=go")
	if w.Code != 503 {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestSuggestHandler_Success(t *testing.T) {
	svc := &mockSearchService{
		suggestFunc: func(_ context.Context, prefix string, size int) ([]string, error) {
			if prefix != "go" {
				t.Errorf("prefix = %q, want 'go'", prefix)
			}
			if size != 10 {
				t.Errorf("size = %d, want 10", size)
			}
			return []string{"Go并发编程", "Go语言入门"}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search/suggest?prefix=go")
	if w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Items []string `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal error = %v", err)
	}
	if body.Code != 0 {
		t.Errorf("code = %d, want 0", body.Code)
	}
	if len(body.Data.Items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(body.Data.Items))
	}
	if body.Data.Items[0] != "Go并发编程" {
		t.Errorf("items[0] = %q", body.Data.Items[0])
	}
}

func TestSuggestHandler_WithSize(t *testing.T) {
	var capturedSize int
	svc := &mockSearchService{
		suggestFunc: func(_ context.Context, _ string, size int) ([]string, error) {
			capturedSize = size
			return []string{"a", "b", "c"}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search/suggest?prefix=g&size=3")
	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	if capturedSize != 3 {
		t.Errorf("size = %d, want 3", capturedSize)
	}
}

func TestSuggestHandler_EmptyPrefix(t *testing.T) {
	svc := &mockSearchService{
		suggestFunc: func(_ context.Context, prefix string, _ int) ([]string, error) {
			// In production the handler will just pass empty prefix to the service.
			// The service will call ES which returns an error.
			return nil, errors.New("completion suggester requires a prefix")
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search/suggest?prefix=")
	if w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestSuggestHandler_ServiceError(t *testing.T) {
	svc := &mockSearchService{
		suggestFunc: func(_ context.Context, _ string, _ int) ([]string, error) {
			return nil, errors.New("es error")
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search/suggest?prefix=go")
	if w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestSuggestHandler_NoPrefixParam(t *testing.T) {
	// When prefix is not provided, c.Query("prefix") returns "".
	svc := &mockSearchService{
		suggestFunc: func(_ context.Context, prefix string, _ int) ([]string, error) {
			if prefix != "" {
				t.Errorf("prefix = %q, want empty", prefix)
			}
			// The handler doesn't check for empty prefix, just passes it through.
			return nil, errors.New("prefix is required")
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)
	w := performRequest(r, "GET", "/api/v1/search/suggest")
	if w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Benchmark Handler
// ---------------------------------------------------------------------------

func BenchmarkSearchHandler(b *testing.B) {
	svc := &mockSearchService{
		searchFunc: func(_ context.Context, _ string, _ int, _, _ string, _ *uint64) (*SearchResponse, error) {
			return &SearchResponse{
				Items: []knowpost.FeedItemResponse{
					{ID: "1", Title: strPtr("Go"), AuthorNickname: "Alice"},
				},
			}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/search?q=go", nil)
		r.ServeHTTP(w, req)
	}
}

func BenchmarkSuggestHandler(b *testing.B) {
	svc := &mockSearchService{
		suggestFunc: func(_ context.Context, _ string, _ int) ([]string, error) {
			return []string{"Go并发", "Go入门", "Golang"}, nil
		},
	}
	h := NewSearchHandler(svc)
	r := setupRouter(h)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/search/suggest?prefix=g", nil)
		r.ServeHTTP(w, req)
	}
}