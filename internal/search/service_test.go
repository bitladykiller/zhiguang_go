package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/zhiguang/app/internal/model"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// mock ES client
// ---------------------------------------------------------------------------

type mockTransport struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

// mockESResponse 构建一个 *http.Response，body 为 respBody，用于 mock ES 返回。
// 注意：ES v8 客户端会验证响应头 X-Elastic-Product 必须为 "Elasticsearch"。
func mockESResponse(statusCode int, respBody string) *http.Response {
	header := make(http.Header)
	header.Set("X-Elastic-Product", "Elasticsearch")
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(respBody)),
		Header:     header,
	}
}

// newMockClient 用 mockTransport 创建一个 *elasticsearch.Client。
func newMockClient(fn func(req *http.Request) (*http.Response, error)) *elasticsearch.Client {
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Transport:                 &mockTransport{roundTrip: fn},
		EnableCompatibilityMode:   true,
		DisableMetaHeader:         true,
	})
	if err != nil {
		panic(fmt.Sprintf("newMockClient: %v", err))
	}
	return c
}

// ---------------------------------------------------------------------------
// helper: stub counter
// ---------------------------------------------------------------------------

type stubSearchCounter struct {
	likedMap map[string]bool
	favedMap map[string]bool
	err      error
}

func (s *stubSearchCounter) IsLiked(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, s.err
}

func (s *stubSearchCounter) IsFaved(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, s.err
}

func (s *stubSearchCounter) BatchIsLiked(_ context.Context, _ uint64, _ string, ids []string) (map[string]bool, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.likedMap != nil {
		return s.likedMap, nil
	}
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m, nil
}

func (s *stubSearchCounter) BatchIsFaved(_ context.Context, _ uint64, _ string, ids []string) (map[string]bool, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.favedMap != nil {
		return s.favedMap, nil
	}
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = false
	}
	return m, nil
}

func (s *stubSearchCounter) Like(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, s.err
}

func (s *stubSearchCounter) Unlike(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, s.err
}

func (s *stubSearchCounter) Fav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, s.err
}

func (s *stubSearchCounter) Unfav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, s.err
}

func (s *stubSearchCounter) GetCounts(_ context.Context, _, _ string, _ []string) (map[string]int32, error) {
	return nil, s.err
}

func (s *stubSearchCounter) GetCountsBatch(_ context.Context, _ string, _, _ []string) (map[string]map[string]int32, error) {
	return nil, s.err
}

// ---------------------------------------------------------------------------
// parseCSV
// ---------------------------------------------------------------------------

func TestParseCSV(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty string", "", nil},
		{"whitespace only", "  ", nil},
		{"single value", "go", []string{"go"}},
		{"multiple values", "go,redis,mysql", []string{"go", "redis", "mysql"}},
		{"with surrounding spaces", " go , redis ", []string{"go", "redis"}},
		{"empty segment skipped", "go,,redis", []string{"go", "redis"}},
		{"all empty segments", ",,", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCSV(tt.input)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("parseCSV(%q) = %v, want %v", tt.input, got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseCSV(%q) = %v, want %v", tt.input, got, tt.want)
					return
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseAfter / encodeAfter
// ---------------------------------------------------------------------------

func TestParseAfter_Empty(t *testing.T) {
	if got := parseAfter(""); got != nil {
		t.Errorf("parseAfter('') = %v, want nil", got)
	}
	if got := parseAfter("  "); got != nil {
		t.Errorf("parseAfter('  ') = %v, want nil", got)
	}
}

func TestParseAfter_InvalidBase64(t *testing.T) {
	got := parseAfter("!!!not-base64!!!")
	if got != nil {
		t.Errorf("parseAfter(invalid) = %v, want nil", got)
	}
}

func TestParseAfter_Valid(t *testing.T) {
	// encodeAfter: float64(3.14) + int64(12345) => "3.14,12345" => base64
	encoded := encodeAfter([]interface{}{float64(3.14), int64(12345)})
	got := parseAfter(encoded)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	v0, ok := got[0].(float64)
	if !ok || v0 != 3.14 {
		t.Errorf("got[0] = %v (type %T), want 3.14", got[0], got[0])
	}
	v1, ok := got[1].(int64)
	if !ok || v1 != 12345 {
		t.Errorf("got[1] = %v (type %T), want 12345", got[1], got[1])
	}
}

func TestEncodeAfter_RoundTrip(t *testing.T) {
	input := []interface{}{float64(1.0), int64(99), "hello"}
	encoded := encodeAfter(input)
	decoded := parseAfter(encoded)
	if len(decoded) != 3 {
		t.Fatalf("len = %d, want 3", len(decoded))
	}
	if v, ok := decoded[0].(float64); !ok || v != 1.0 {
		t.Errorf("decoded[0] = %v (%T), want 1.0", decoded[0], decoded[0])
	}
	if v, ok := decoded[1].(int64); !ok || v != 99 {
		t.Errorf("decoded[1] = %v (%T), want 99", decoded[1], decoded[1])
	}
	if v, ok := decoded[2].(string); !ok || v != "hello" {
		t.Errorf("decoded[2] = %v (%T), want 'hello'", decoded[2], decoded[2])
	}
}

func TestEncodeAfter_StringFallback(t *testing.T) {
	// unknown type should fallback to fmt.Sprint
	input := []interface{}{uint(42)}
	encoded := encodeAfter(input)
	decoded := parseAfter(encoded)
	if len(decoded) != 1 {
		t.Fatalf("len = %d, want 1", len(decoded))
	}
	// parseAfter at index 0 tries ParseFloat first; "42" -> float64(42)
	if v, ok := decoded[0].(float64); !ok || v != 42 {
		t.Errorf("decoded[0] = %v (%T), want float64(42)", decoded[0], decoded[0])
	}
}

// ---------------------------------------------------------------------------
// buildSnippet
// ---------------------------------------------------------------------------

func TestBuildSnippet_Nil(t *testing.T) {
	if got := buildSnippet(nil); got != "" {
		t.Errorf("buildSnippet(nil) = %q, want ''", got)
	}
}

func TestBuildSnippet_EmptyMap(t *testing.T) {
	if got := buildSnippet(map[string][]string{}); got != "" {
		t.Errorf("buildSnippet({}) = %q, want ''", got)
	}
}

func TestBuildSnippet_TitleOnly(t *testing.T) {
	hl := map[string][]string{"title": {"<em>go</em>语言入门"}}
	got := buildSnippet(hl)
	want := "<em>go</em>语言入门"
	if got != want {
		t.Errorf("buildSnippet = %q, want %q", got, want)
	}
}

func TestBuildSnippet_BodyOnly(t *testing.T) {
	hl := map[string][]string{"body": {"学习<em>并发</em>编程"}}
	got := buildSnippet(hl)
	want := "学习<em>并发</em>编程"
	if got != want {
		t.Errorf("buildSnippet = %q, want %q", got, want)
	}
}

func TestBuildSnippet_Both(t *testing.T) {
	hl := map[string][]string{
		"title": {"<em>Go</em>并发"},
		"body":  {"深入<em>goroutine</em>"},
	}
	got := buildSnippet(hl)
	want := "<em>Go</em>并发 深入<em>goroutine</em>"
	if got != want {
		t.Errorf("buildSnippet = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// boolPtr
// ---------------------------------------------------------------------------

func TestBoolPtr(t *testing.T) {
	if got := boolPtr(true); got == nil || !*got {
		t.Error("boolPtr(true) should return pointer to true")
	}
	if got := boolPtr(false); got == nil || *got {
		t.Error("boolPtr(false) should return pointer to false")
	}
}

// ---------------------------------------------------------------------------
// SearchService.Search
// ---------------------------------------------------------------------------

func TestSearch_DefaultSize(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, `{"hits":{"hits":[]}}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	resp, err := svc.Search(context.Background(), "golang", 0, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if resp == nil {
		t.Fatal("resp is nil")
	}
	if len(resp.Items) != 0 {
		t.Errorf("len(items) = %d, want 0", len(resp.Items))
	}
	if resp.HasMore {
		t.Error("HasMore should be false for empty result")
	}
	if resp.NextAfter != nil {
		t.Error("NextAfter should be nil for empty result")
	}
}

func TestSearch_VerifyRequestBody(t *testing.T) {
	var capturedBody string
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		capturedBody = string(b)
		return mockESResponse(200, `{"hits":{"hits":[]}}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Search(context.Background(), "golang", 20, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if !strings.Contains(capturedBody, `"size":20`) {
		t.Errorf("body missing size:20, got = %s", capturedBody)
	}
	if !strings.Contains(capturedBody, `"status"`) {
		t.Errorf("body missing status filter, got = %s", capturedBody)
	}
}

func TestSearch_Success(t *testing.T) {
	esResp := `{
	  "hits": {
	    "hits": [
	      {
	        "_source": {
	          "id": "101",
	          "title": "Go并发",
	          "description": "精通goroutine",
	          "tags": ["go","concurrency"],
	          "author_id": "42",
	          "author_name": "Alice",
	          "like_count": 10,
	          "favorite_count": 3,
	          "view_count": 100,
	          "status": "published",
	          "visible": "public",
	          "img_urls": ["https://example.com/cover.jpg"],
	          "is_top": false
	        },
	        "_score": 2.5,
	        "sort": [2.5, 1700000000000, 10, 100, "101"]
	      }
	    ]
	  }
	}`

	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	counter := &stubSearchCounter{}
	svc := &SearchService{client: client, indexName: "test-index", counter: counter}
	uid := uint64(1)
	resp, err := svc.Search(context.Background(), "Go", 10, "", "", &uid)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(resp.Items))
	}
	item := resp.Items[0]
	if item.ID != "101" {
		t.Errorf("ID = %q, want '101'", item.ID)
	}
	if *item.Title != "Go并发" {
		t.Errorf("Title = %q, want 'Go并发'", *item.Title)
	}
	if *item.Description != "精通goroutine" {
		t.Errorf("Description = %q, want '精通goroutine'", *item.Description)
	}
	if item.LikeCount != 10 {
		t.Errorf("LikeCount = %d, want 10", item.LikeCount)
	}
	if len(item.Tags) != 2 || item.Tags[0] != "go" {
		t.Errorf("Tags = %v, want [go concurrency]", item.Tags)
	}
	if item.CoverImage == nil || *item.CoverImage != "https://example.com/cover.jpg" {
		t.Errorf("CoverImage = %v, want 'https://example.com/cover.jpg'", item.CoverImage)
	}
	if resp.NextAfter == nil {
		t.Error("NextAfter should not be nil when hits exist")
	}
	if resp.HasMore {
		t.Error("HasMore should be false when hits(1) < size(10)")
	}
	// user state
	if item.Liked == nil || !*item.Liked {
		t.Error("Liked should be true from stub")
	}
	if item.Faved == nil || *item.Faved {
		t.Error("Faved should be false from stub")
	}
}

func TestSearch_HasMore(t *testing.T) {
	// Build a response with exactly `size` hits — hasMore should be true.
	size := 2
	hit := `{
	  "_source": {"id":"1","title":"t","author_id":"1","author_name":"n","status":"published","visible":"public"},
	  "_score": 1.0,
	  "sort": [1.0,"1"]
	}`
	esResp := `{"hits":{"hits":[` + hit + `,` + hit + `]}}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	resp, err := svc.Search(context.Background(), "go", size, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if !resp.HasMore {
		t.Errorf("HasMore should be true when hits(%d) >= size(%d)", size, size)
	}
	if resp.NextAfter == nil {
		t.Error("NextAfter should not be nil when hits exist")
	}
}

func TestSearch_NoHasMore(t *testing.T) {
	// Only 1 hit, size=10 — hasMore should be false.
	esResp := `{"hits":{"hits":[{"_source":{"id":"1","title":"t","author_id":"1","author_name":"n","status":"published","visible":"public"},"_score":1.0,"sort":[1.0,"1"]}]}}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	resp, err := svc.Search(context.Background(), "go", 10, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if resp.HasMore {
		t.Error("HasMore should be false when hits(1) < size(10)")
	}
}

func TestSearch_WithTags(t *testing.T) {
	var capturedBody string
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		capturedBody = string(b)
		return mockESResponse(200, `{"hits":{"hits":[]}}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Search(context.Background(), "go", 10, "go,redis", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if !strings.Contains(capturedBody, `"tags"`) {
		t.Errorf("filter with tags missing, body = %s", capturedBody)
	}
	if !strings.Contains(capturedBody, `"go"`) || !strings.Contains(capturedBody, `"redis"`) {
		t.Errorf("tags not in filter, body = %s", capturedBody)
	}
}

func TestSearch_WithAfter(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, `{
		  "hits": {
		    "hits": [
		      {
		        "_source": {"id":"1","title":"t","author_id":"1","author_name":"n","status":"published","visible":"public"},
		        "_score": 1.0,
		        "sort": [1.0, 0, 0, 0, "1"]
		      }
		    ]
		  }
		}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	// encode a valid after cursor
	after := encodeAfter([]interface{}{float64(1.0), int64(0), int64(0), int64(0), "prevID"})
	resp, err := svc.Search(context.Background(), "go", 10, "", after, nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(resp.Items))
	}
}

func TestSearch_WithSnippetHighlight(t *testing.T) {
	esResp := `{
	  "hits": {
	    "hits": [
	      {
	        "_source": {
	          "id":"1","title":"Go并发编程","description":"原始描述","author_id":"1","author_name":"n","status":"published","visible":"public"
	        },
	        "_score": 1.0,
	        "sort": [1.0,"1"],
	        "highlight": {
	          "title": ["<em>Go</em>并发编程"],
	          "body": ["深入<em>goroutine</em>"]
	        }
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	resp, err := svc.Search(context.Background(), "Go", 10, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("len(items) = %d", len(resp.Items))
	}
	// description should be replaced by snippet
	want := "<em>Go</em>并发编程 深入<em>goroutine</em>"
	if *resp.Items[0].Description != want {
		t.Errorf("Description = %q, want %q", *resp.Items[0].Description, want)
	}
}

func TestSearch_ESError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(400, `{"error":"bad request"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Search(context.Background(), "go", 10, "", "", nil)
	if err == nil {
		t.Fatal("expected error for ES 400 response")
	}
	if !strings.Contains(err.Error(), "search failed") {
		t.Errorf("error = %v, want 'search failed'", err)
	}
}

func TestSearch_ESNetworkError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Search(context.Background(), "go", 10, "", "", nil)
	if err == nil {
		t.Fatal("expected network error")
	}
}

func TestSearch_NoHits(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, `{"hits":{"hits":[]}}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	resp, err := svc.Search(context.Background(), "nonexistent", 10, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 0 {
		t.Errorf("len(items) = %d, want 0", len(resp.Items))
	}
	if resp.HasMore {
		t.Error("HasMore should be false")
	}
	if resp.NextAfter != nil {
		t.Error("NextAfter should be nil")
	}
}

func TestSearch_AnonymousUser(t *testing.T) {
	esResp := `{
	  "hits": {
	    "hits": [
	      {
	        "_source": {"id":"1","title":"t","author_id":"1","author_name":"n","status":"published","visible":"public"},
	        "_score": 1.0,
	        "sort": [1.0,"1"]
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index", counter: &stubSearchCounter{}}
	resp, err := svc.Search(context.Background(), "go", 10, "", "", nil)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if resp.Items[0].Liked != nil {
		t.Error("Liked should be nil for anonymous user")
	}
	if resp.Items[0].Faved != nil {
		t.Error("Faved should be nil for anonymous user")
	}
}

func TestSearch_CounterError(t *testing.T) {
	esResp := `{
	  "hits": {
	    "hits": [
	      {
	        "_source": {"id":"1","title":"t","author_id":"1","author_name":"n","status":"published","visible":"public"},
	        "_score": 1.0,
	        "sort": [1.0,"1"]
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	// counter returns an error — Search should still return items without liked/faved
	counter := &stubSearchCounter{err: errors.New("counter down")}
	svc := &SearchService{client: client, indexName: "test-index", counter: counter, logger: zap.NewNop()}
	uid := uint64(1)
	resp, err := svc.Search(context.Background(), "go", 10, "", "", &uid)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("len(items) = %d", len(resp.Items))
	}
	// Liked/Faved should be nil because counter error means likedMap/favedMap remain nil
	if resp.Items[0].Liked != nil {
		t.Error("Liked should be nil when counter returns error")
	}
}

// ---------------------------------------------------------------------------
// SearchService.Suggest
// ---------------------------------------------------------------------------

func TestSuggest_DefaultSize(t *testing.T) {
	var capturedBody string
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		capturedBody = string(b)
		return mockESResponse(200, `{"suggest":{}}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	got, err := svc.Suggest(context.Background(), "go", 0)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(suggestions) = %d, want 0", len(got))
	}
	if !strings.Contains(capturedBody, `"size":10`) {
		t.Errorf("default size not 10, body = %s", capturedBody)
	}
}

func TestSuggest_Success(t *testing.T) {
	esResp := `{
	  "suggest": {
	    "title-suggest": [
	      {
	        "options": [
	          {"text": "Go并发编程"},
	          {"text": "Go语言入门"}
	        ]
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	got, err := svc.Suggest(context.Background(), "Go", 5)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0] != "Go并发编程" {
		t.Errorf("got[0] = %q, want 'Go并发编程'", got[0])
	}
	if got[1] != "Go语言入门" {
		t.Errorf("got[1] = %q, want 'Go语言入门'", got[1])
	}
}

func TestSuggest_Deduplicate(t *testing.T) {
	esResp := `{
	  "suggest": {
	    "title-suggest": [
	      {
	        "options": [
	          {"text": "Go"},
	          {"text": "Go"},
	          {"text": "Go语言"}
	        ]
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	got, err := svc.Suggest(context.Background(), "Go", 10)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (deduplicated)", len(got))
	}
}

func TestSuggest_EmptyPrefix(t *testing.T) {
	// Suggest with empty prefix: ES returns error, but we should propagate it.
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(400, `{"error":"completion suggester requires a prefix"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Suggest(context.Background(), "", 5)
	if err == nil {
		t.Fatal("expected error for empty prefix")
	}
}

func TestSuggest_CappedBySize(t *testing.T) {
	esResp := `{
	  "suggest": {
	    "title-suggest": [
	      {
	        "options": [
	          {"text": "A"},{"text": "B"},{"text": "C"},{"text": "D"}
	        ]
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	got, err := svc.Suggest(context.Background(), "x", 2)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2 (capped by size)", len(got))
	}
}

func TestSuggest_ESError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(500, `{"error":"internal"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Suggest(context.Background(), "go", 5)
	if err == nil {
		t.Fatal("expected ES error")
	}
}

func TestSuggest_NetworkError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("timeout")
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	_, err := svc.Suggest(context.Background(), "go", 5)
	if err == nil {
		t.Fatal("expected network error")
	}
}

func TestSuggest_SkipEmptyText(t *testing.T) {
	esResp := `{
	  "suggest": {
	    "title-suggest": [
	      {
	        "options": [
	          {"text": ""},
	          {"text": "Go"}
	        ]
	      }
	    ]
	  }
	}`
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, esResp), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	got, err := svc.Suggest(context.Background(), "G", 10)
	if err != nil {
		t.Fatalf("Suggest() error = %v", err)
	}
	if len(got) != 1 || got[0] != "Go" {
		t.Errorf("got = %v, want ['Go']", got)
	}
}

// ---------------------------------------------------------------------------
// SearchService.IndexDocument
// ---------------------------------------------------------------------------

func TestIndexDocument_Success(t *testing.T) {
	var capturedBody string
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		capturedBody = string(b)
		return mockESResponse(201, `{"result":"created"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	doc := &SearchIndexDoc{
		ID:     "1",
		Title:  "Test",
		Status: "published",
	}
	err := svc.IndexDocument(context.Background(), doc)
	if err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}
	if !strings.Contains(capturedBody, `"id":"1"`) {
		t.Errorf("body missing id, got = %s", capturedBody)
	}
}

func TestIndexDocument_ESError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(500, `{"error":"cluster error"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	err := svc.IndexDocument(context.Background(), &SearchIndexDoc{ID: "1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIndexDocument_NetworkError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("connection lost")
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	err := svc.IndexDocument(context.Background(), &SearchIndexDoc{ID: "1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// SearchService.DeleteDocument
// ---------------------------------------------------------------------------

func TestDeleteDocument_Success(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(200, `{"result":"deleted"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	err := svc.DeleteDocument(context.Background(), "1")
	if err != nil {
		t.Fatalf("DeleteDocument() error = %v", err)
	}
}

func TestDeleteDocument_NotFound(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(404, `{"found":false}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	err := svc.DeleteDocument(context.Background(), "999")
	if err != nil {
		t.Fatalf("DeleteDocument() for 404 should return nil, got: %v", err)
	}
}

func TestDeleteDocument_ESError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return mockESResponse(500, `{"error":"internal"}`), nil
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	err := svc.DeleteDocument(context.Background(), "1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteDocument_NetworkError(t *testing.T) {
	client := newMockClient(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("connection timeout")
	})
	svc := &SearchService{client: client, indexName: "test-index"}
	err := svc.DeleteDocument(context.Background(), "1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// FeedItemResponse — ensure response JSON serialisation round-trips cleanly
// ---------------------------------------------------------------------------

func TestSearchResponse_JSONRoundTrip(t *testing.T) {
	resp := &SearchResponse{
		Items: []model.FeedItem{
			{
				ID:             "1",
				Title:          strPtr("t"),
				Description:    strPtr("d"),
				Tags:           []string{"a"},
				AuthorNickname: "n",
				LikeCount:      5,
			},
		},
		HasMore: true,
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("json.Marshal error = %v", err)
	}
	var decoded SearchResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal error = %v", err)
	}
	if len(decoded.Items) != 1 {
		t.Fatalf("len = %d", len(decoded.Items))
	}
	if decoded.Items[0].ID != "1" {
		t.Errorf("ID = %q", decoded.Items[0].ID)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkParseCSV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseCSV("go,redis,mysql,elasticsearch,kafka")
	}
}

func BenchmarkBuildSnippet(b *testing.B) {
	hl := map[string][]string{
		"title": {"<em>Go</em>并发编程"},
		"body":  {"深入<em>goroutine</em>与<em>channel</em>"},
	}
	for i := 0; i < b.N; i++ {
		buildSnippet(hl)
	}
}

func BenchmarkEncodeDecodeAfter(b *testing.B) {
	sortVals := []interface{}{float64(3.14), int64(12345), int64(67890)}
	for i := 0; i < b.N; i++ {
		encoded := encodeAfter(sortVals)
		parseAfter(encoded)
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }