package search

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// mock SearchServiceInterface
// ---------------------------------------------------------------------------

type mockSearchService struct {
	searchFunc  func(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	suggestFunc func(ctx context.Context, prefix string, size int) ([]string, error)
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
// Search handler test (table-driven)
// ---------------------------------------------------------------------------

// searchTestCase 定义 Search handler 测试的通用结构。
type searchTestCase struct {
	name       string
	svc        SearchServiceInterface // 使用接口类型以支持 nil
	path       string
	wantStatus int
}

func TestSearchHandler(t *testing.T) {
	tests := []searchTestCase{
		{
			name:       "nil_service",
			svc:        nil,
			path:       "/api/v1/search?q=golang",
			wantStatus: 503,
		},
		{
			name: "missing_q",
			svc: &mockSearchService{
				searchFunc: func(_ context.Context, _ string, _ int, _, _ string, _ *uint64) (*SearchResponse, error) {
					return &SearchResponse{}, nil
				},
			},
			path:       "/api/v1/search",
			wantStatus: 400,
		},
		{
			name: "service_error",
			svc: &mockSearchService{
				searchFunc: func(_ context.Context, _ string, _ int, _, _ string, _ *uint64) (*SearchResponse, error) {
					return nil, errors.New("es down")
				},
			},
			path:       "/api/v1/search?q=golang",
			wantStatus: 500,
		},
		{
			name: "success",
			svc: &mockSearchService{
				searchFunc: func(_ context.Context, keyword string, size int, tagsCSV, after string, _ *uint64) (*SearchResponse, error) {
					if keyword != "golang" {
						t.Errorf("keyword = %q, want 'golang'", keyword)
					}
					if size != 20 {
						t.Errorf("size = %d, want 20", size)
					}
					return &SearchResponse{
						Items: []SearchItem{
							{ID: "1", Title: strPtr("Go入门"), AuthorNickname: "Alice"},
						},
						HasMore: false,
					}, nil
				},
			},
			path:       "/api/v1/search?q=golang",
			wantStatus: 200,
		},
		{
			name: "with_all_params",
			svc: &mockSearchService{
				searchFunc: func(_ context.Context, keyword string, size int, tagsCSV, after string, _ *uint64) (*SearchResponse, error) {
					if keyword != "go" {
						t.Errorf("keyword = %q", keyword)
					}
					if size != 5 {
						t.Errorf("size = %d", size)
					}
					if tagsCSV != "go,redis" {
						t.Errorf("tags = %q", tagsCSV)
					}
					if after != "abc123" {
						t.Errorf("after = %q", after)
					}
					return &SearchResponse{}, nil
				},
			},
			path:       "/api/v1/search?q=go&size=5&tags=go,redis&after=abc123",
			wantStatus: 200,
		},
		{
			name: "invalid_size",
			svc: &mockSearchService{
				searchFunc: func(_ context.Context, _ string, size int, _, _ string, _ *uint64) (*SearchResponse, error) {
					if size != 20 {
						t.Errorf("size = %d, want default 20 when invalid", size)
					}
					return &SearchResponse{}, nil
				},
			},
			path:       "/api/v1/search?q=go&size=-1",
			wantStatus: 200,
		},
		{
			name: "zero_size",
			svc: &mockSearchService{
				searchFunc: func(_ context.Context, _ string, size int, _, _ string, _ *uint64) (*SearchResponse, error) {
					if size != 20 {
						t.Errorf("size = %d, want default 20", size)
					}
					return &SearchResponse{}, nil
				},
			},
			path:       "/api/v1/search?q=go&size=0",
			wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewSearchHandler(tt.svc)
			r := setupRouter(h)
			w := performRequest(r, "GET", tt.path)
			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Suggest handler test (table-driven)
// ---------------------------------------------------------------------------

// suggestTestCase 定义 Suggest handler 测试的通用结构。
type suggestTestCase struct {
	name       string
	svc        SearchServiceInterface // 使用接口类型以支持 nil
	path       string
	wantStatus int
}

func TestSuggestHandler(t *testing.T) {
	tests := []suggestTestCase{
		{
			name:       "nil_service",
			svc:        nil,
			path:       "/api/v1/search/suggest?prefix=go",
			wantStatus: 503,
		},
		{
			name: "success",
			svc: &mockSearchService{
				suggestFunc: func(_ context.Context, prefix string, size int) ([]string, error) {
					if prefix != "go" {
						t.Errorf("prefix = %q, want 'go'", prefix)
					}
					if size != 10 {
						t.Errorf("size = %d, want 10", size)
					}
					return []string{"Go并发编程", "Go语言入门"}, nil
				},
			},
			path:       "/api/v1/search/suggest?prefix=go",
			wantStatus: 200,
		},
		{
			name: "with_size",
			svc: &mockSearchService{
				suggestFunc: func(_ context.Context, _ string, size int) ([]string, error) {
					if size != 3 {
						t.Errorf("size = %d, want 3", size)
					}
					return []string{"a", "b", "c"}, nil
				},
			},
			path:       "/api/v1/search/suggest?prefix=g&size=3",
			wantStatus: 200,
		},
		{
			name: "empty_prefix",
			svc: &mockSearchService{
				suggestFunc: func(_ context.Context, prefix string, _ int) ([]string, error) {
					// In production the handler just passes empty prefix to the service.
					return nil, errors.New("completion suggester requires a prefix")
				},
			},
			path:       "/api/v1/search/suggest?prefix=",
			wantStatus: 500,
		},
		{
			name: "service_error",
			svc: &mockSearchService{
				suggestFunc: func(_ context.Context, _ string, _ int) ([]string, error) {
					return nil, errors.New("es error")
				},
			},
			path:       "/api/v1/search/suggest?prefix=go",
			wantStatus: 500,
		},
		{
			name: "no_prefix_param",
			svc: &mockSearchService{
				suggestFunc: func(_ context.Context, prefix string, _ int) ([]string, error) {
					if prefix != "" {
						t.Errorf("prefix = %q, want empty", prefix)
					}
					return nil, errors.New("prefix is required")
				},
			},
			path:       "/api/v1/search/suggest",
			wantStatus: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewSearchHandler(tt.svc)
			r := setupRouter(h)
			w := performRequest(r, "GET", tt.path)
			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tt.wantStatus)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmark Handler
// ---------------------------------------------------------------------------

func BenchmarkSearchHandler(b *testing.B) {
	svc := &mockSearchService{
		searchFunc: func(_ context.Context, _ string, _ int, _, _ string, _ *uint64) (*SearchResponse, error) {
			return &SearchResponse{
				Items: []SearchItem{
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
