package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// ============================================================================
// Mock 服务
// ============================================================================

type mockDescSvc struct {
	desc string
	err  error
}

func (m *mockDescSvc) SuggestDescription(_ context.Context, _, _ string) (string, error) {
	return m.desc, m.err
}

type mockRagSvc struct {
	tokens []string
	err    error
	// 记录收到的参数
	lastPostID uint64
	lastQ      string
	mu         sync.Mutex
	// 控制写入 channel 前是否检查 ctx.Done
	checkCtx bool
}

func (m *mockRagSvc) Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error {
	m.mu.Lock()
	m.lastPostID = postID
	m.lastQ = question
	m.mu.Unlock()

	defer close(streamChan)

	if m.err != nil {
		return m.err
	}

	for _, t := range m.tokens {
		if m.checkCtx {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case streamChan <- t:
		}
	}
	return nil
}

// ============================================================================
// Helpers
// ============================================================================

func setupHandler(descSvc DescriptionServiceInterface, ragSvc RagQueryServiceInterface) *LlmHandler {
	gin.SetMode(gin.TestMode)
	return NewLlmHandler(descSvc, ragSvc)
}

const ctxUserID = "user_id"

// 注入 user_id 到上下文，模拟已登录
func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(ctxUserID, uint64(42))
		c.Next()
	}
}

func performRequest(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(w, req)
	return w
}

// ============================================================================
// SuggestDescription 测试
// ============================================================================

func TestSuggestDescription_Success(t *testing.T) {
	handler := setupHandler(&mockDescSvc{desc: "AI 生成的摘要"}, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{"title":"标题","content":"内容"}`)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			Description string `json:"description"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	if resp.Data.Description != "AI 生成的摘要" {
		t.Errorf("description = %q, want %q", resp.Data.Description, "AI 生成的摘要")
	}
}

func TestSuggestDescription_Unauthorized(t *testing.T) {
	handler := setupHandler(&mockDescSvc{desc: "xxx"}, nil)

	r := gin.New()
	handler.RegisterRoutes(r.Group("/api/v1"))
	// 不添加 authMiddleware，模拟未登录

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{"title":"t","content":"c"}`)

	if w.Code != 401 {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestSuggestDescription_ServiceUnavailable(t *testing.T) {
	handler := setupHandler(nil, nil) // descSvc is nil

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{"title":"t","content":"c"}`)

	if w.Code != 503 {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestSuggestDescription_InvalidJSON(t *testing.T) {
	handler := setupHandler(&mockDescSvc{}, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{invalid`)

	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestSuggestDescription_MissingTitle(t *testing.T) {
	handler := setupHandler(&mockDescSvc{}, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{"content":"c"}`)

	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestSuggestDescription_ServiceError(t *testing.T) {
	handler := setupHandler(&mockDescSvc{err: errors.New("api error")}, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{"title":"t","content":"c"}`)

	if w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// ============================================================================
// RagQuery SSE 测试
// ============================================================================

func TestRagQuery_SSE_Stream(t *testing.T) {
	tokens := []string{
		"data: {\"token\": \"你好\"}\n\n",
		"data: {\"token\": \"世界\"}\n\n",
		"data: [DONE]\n\n",
	}
	mock := &mockRagSvc{tokens: tokens}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"你好吗"}`)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want %q", ct, "text/event-stream")
	}

	body := w.Body.String()
	for _, tok := range tokens {
		if !strings.Contains(body, tok) {
			t.Errorf("body should contain %q", tok)
		}
	}

	if mock.lastPostID != 1 {
		t.Errorf("postID = %d, want 1", mock.lastPostID)
	}
	if mock.lastQ != "你好吗" {
		t.Errorf("question = %q, want %q", mock.lastQ, "你好吗")
	}
}

func TestRagQuery_SSE_EmptyTokens(t *testing.T) {
	mock := &mockRagSvc{tokens: []string{}}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}

	body := w.Body.String()
	if body != "" {
		t.Errorf("expected empty body for no tokens, got %q", body)
	}
}

func TestRagQuery_Unauthorized(t *testing.T) {
	handler := setupHandler(nil, &mockRagSvc{})

	r := gin.New()
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)

	if w.Code != 401 {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestRagQuery_ServiceUnavailable(t *testing.T) {
	handler := setupHandler(nil, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)

	if w.Code != 503 {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestRagQuery_InvalidPostID(t *testing.T) {
	handler := setupHandler(nil, &mockRagSvc{})

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/abc/rag/query", `{"question":"q"}`)

	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRagQuery_MissingQuestion(t *testing.T) {
	handler := setupHandler(nil, &mockRagSvc{})

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{}`)

	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRagQuery_InvalidJSON(t *testing.T) {
	handler := setupHandler(nil, &mockRagSvc{})

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{invalid`)

	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// ============================================================================
// 客户端断开连接测试
// ============================================================================

func TestRagQuery_ClientDisconnects(t *testing.T) {
	// 验证：客户端断开后，handler 正常返回（streamChan 被关闭，goroutine 退出）
	tokens := []string{
		"data: token1\n\n",
		"data: token2\n\n",
	}
	mock := &mockRagSvc{tokens: tokens}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxUserID, uint64(42))
		c.Next()
	})
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/knowposts/1/rag/query",
		strings.NewReader(`{"question":"q"}`))
	req.Header.Set("Content-Type", "application/json")

	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)

	// 在 goroutine 中处理，然后立即取消
	done := make(chan struct{})
	go func() {
		r.ServeHTTP(w, req)
		close(done)
	}()
	cancel()
	<-done

	// 只要 handler 不 panic，测试通过
	if w.Code != 200 {
		t.Logf("after cancel status = %d (expected 200 or incomplete)", w.Code)
	}
}

func TestRagQuery_RecoverFromPanicInGoroutine(t *testing.T) {
	ragSvc := &RagQueryServiceInterfaceMock{
		QueryFunc: func(_ context.Context, _ uint64, _ string, _ chan<- string) error {
			// Don't close the channel — handler's recover will handle it
			panic("unexpected error in rag query")
		},
	}

	handler := setupHandler(nil, ragSvc)

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxUserID, uint64(42))
		c.Next()
	})
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200 (SSE should still return with error)", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "error") || !strings.Contains(body, "[DONE]") {
		t.Errorf("SSE body should contain error and [DONE] after panic, got: %s", body)
	}
}

// ============================================================================
// RagQueryServiceInterface mock with panic support
// ============================================================================

type RagQueryServiceInterfaceMock struct {
	QueryFunc func(ctx context.Context, postID uint64, question string, streamChan chan<- string) error
}

func (m *RagQueryServiceInterfaceMock) Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error {
	return m.QueryFunc(ctx, postID, question, streamChan)
}

// ============================================================================
// 边缘情况与边界测试
// ============================================================================

func TestRagQuery_LargeQuestion(t *testing.T) {
	largeQ := strings.Repeat("你好", 10000)
	mock := &mockRagSvc{tokens: []string{"data: ok\n\n", "data: [DONE]\n\n"}}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	body := fmt.Sprintf(`{"question":"%s"}`, largeQ)
	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", body)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestSuggestDescription_EmptyTitle(t *testing.T) {
	handler := setupHandler(&mockDescSvc{}, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/123/description/suggest", `{"title":"","content":"c"}`)

	if w.Code != 400 {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRagQuery_Timeout(t *testing.T) {
	mock := &mockRagSvc{
		tokens:   []string{"data: slow\n\n"},
		checkCtx: true,
	}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxUserID, uint64(42))
		c.Next()
	})
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/knowposts/1/rag/query",
		strings.NewReader(`{"question":"q"}`))
	req.Header.Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(req.Context(), 1*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	r.ServeHTTP(w, req)

	// timeout may return 200 with partial content or incomplete,
	// but should not panic
	if w.Code == 0 {
		w.Code = 200
	}
}

// ============================================================================
// SSE 格式校验
// ============================================================================

func TestRagQuery_SSE_Format(t *testing.T) {
	mock := &mockRagSvc{
		tokens: []string{"data: {\"answer\":\"hello\"}\n\n", "data: [DONE]\n\n"},
	}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"hi"}`)

	body := w.Body.String()
	if !strings.HasPrefix(body, "data: ") {
		t.Errorf("SSE body should start with 'data: ', got: %s", body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Errorf("SSE body should end with 'data: [DONE]\\n\\n', got: %s", body)
	}
}

func TestRagQuery_ConcurrentRequests(t *testing.T) {
	mock := &mockRagSvc{tokens: []string{"data: concurrent\n\n", "data: [DONE]\n\n"}}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)
			if w.Code != 200 {
				t.Errorf("concurrent: status = %d", w.Code)
			}
		}()
	}
	wg.Wait()
}

// ============================================================================
// Zero value handler 测试
// ============================================================================

func TestNewLlmHandler_ZeroValues(t *testing.T) {
	h := NewLlmHandler(nil, nil)
	if h.descSvc != nil {
		t.Error("descSvc should be nil")
	}
	if h.ragSvc != nil {
		t.Error("ragSvc should be nil")
	}
}

// ============================================================================
// Benchmark
// ============================================================================

func BenchmarkSuggestDescription(b *testing.B) {
	handler := setupHandler(&mockDescSvc{desc: "摘要"}, nil)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	body := `{"title":"标题","content":"内容"}`
	for i := 0; i < b.N; i++ {
		w := performRequest(r, "POST", "/api/v1/knowposts/1/description/suggest", body)
		if w.Code != 200 {
			b.Fatalf("status = %d", w.Code)
		}
	}
}

func BenchmarkRagQuery(b *testing.B) {
	mock := &mockRagSvc{
		tokens: []string{
			"data: {\"token\": \"a\"}\n\n",
			"data: {\"token\": \"b\"}\n\n",
			"data: [DONE]\n\n",
		},
	}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	for i := 0; i < b.N; i++ {
		w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)
		if w.Code != 200 {
			b.Fatalf("status = %d", w.Code)
		}
	}
}

// ensure mock implements interfaces
var (
	_ DescriptionServiceInterface = (*mockDescSvc)(nil)
	_ RagQueryServiceInterface    = (*mockRagSvc)(nil)
	_ RagQueryServiceInterface    = (*RagQueryServiceInterfaceMock)(nil)
)

// 流式读取 helper
type sseLineReader struct {
	body string
	pos  int
}

func (r *sseLineReader) ReadLine() (string, bool) {
	if r.pos >= len(r.body) {
		return "", false
	}
	idx := strings.Index(r.body[r.pos:], "\n")
	if idx == -1 {
		line := r.body[r.pos:]
		r.pos = len(r.body)
		return line, false
	}
	line := r.body[r.pos : r.pos+idx]
	r.pos += idx + 1
	return line, true
}

func TestSSELineReader(t *testing.T) {
	sse := "data: hello\n\ndata: [DONE]\n\n"
	r := &sseLineReader{body: sse}
	lines := []string{}
	for {
		line, ok := r.ReadLine()
		if !ok {
			break
		}
		lines = append(lines, line)
	}
	if len(lines) != 4 {
		t.Errorf("expected 4 lines, got %d: %v", len(lines), lines)
	}
}

func TestRagQuery_ReadSSELines(t *testing.T) {
	mock := &mockRagSvc{
		tokens: []string{"data: line1\n\n", "data: line2\n\n", "data: [DONE]\n\n"},
	}
	handler := setupHandler(nil, mock)

	r := gin.New()
	r.Use(authMiddleware())
	handler.RegisterRoutes(r.Group("/api/v1"))

	w := performRequest(r, "POST", "/api/v1/knowposts/1/rag/query", `{"question":"q"}`)

	sse := &sseLineReader{body: w.Body.String()}
	var events []string
	for {
		line, ok := sse.ReadLine()
		if !ok {
			break
		}
		if strings.HasPrefix(line, "data: ") {
			events = append(events, strings.TrimPrefix(line, "data: "))
		}
	}
	if len(events) != 3 {
		t.Errorf("expected 3 SSE events, got %d: %v", len(events), events)
	}
	if events[2] != "[DONE]" {
		t.Errorf("last event should be [DONE], got %q", events[2])
	}
}
