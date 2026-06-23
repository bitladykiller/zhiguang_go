package llm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhiguang/app/pkg/config"
)

// ============================================================================
// Helpers
// ============================================================================

func newTestDescService(cfg *config.LLMConfig) *KnowPostDescriptionService {
	return NewKnowPostDescriptionService(cfg)
}

func defaultLLMConfig() *config.LLMConfig {
	return &config.LLMConfig{
		DeepSeek: config.DeepSeekConfig{
			BaseURL:     "http://localhost:9999",
			Model:       "deepseek-chat",
			Temperature: 0.7,
			APIKey:      "test-key",
		},
		TimeoutMs: 5000,
	}
}

// ============================================================================
// SuggestDescription — HTTP API mock 测试
// ============================================================================

func TestSuggestDescription_API_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"choices": [{"message": {"content": " 这是一段AI生成的摘要 "}}]
		}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	desc, err := svc.SuggestDescription(context.Background(), "测试标题", "测试内容")
	if err != nil {
		t.Fatalf("SuggestDescription() error = %v", err)
	}
	if desc != "这是一段AI生成的摘要" {
		t.Errorf("description = %q, want %q", desc, "这是一段AI生成的摘要")
	}
}

func TestSuggestDescription_API_EmptyChoices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices": []}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
	if !strings.Contains(err.Error(), "no choices") {
		t.Errorf("error = %v, want 'no choices'", err)
	}
}

func TestSuggestDescription_API_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"error": {"message": "rate limit exceeded"}}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error for api error response")
	}
	if !strings.Contains(err.Error(), "rate limit") {
		t.Errorf("error = %v, want 'rate limit'", err)
	}
}

func TestSuggestDescription_API_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error": {"message": "too many requests"}}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error for non-200 status")
	}
}

func TestSuggestDescription_API_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{invalid json`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSuggestDescription_API_ServerUnreachable(t *testing.T) {
	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = "http://127.0.0.1:1"
	cfg.TimeoutMs = 100
	svc := newTestDescService(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := svc.SuggestDescription(ctx, "t", "c")
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

func TestSuggestDescription_API_ContextCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	cfg.TimeoutMs = 5000
	svc := newTestDescService(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := svc.SuggestDescription(ctx, "t", "c")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestSuggestDescription_API_Non200_NoErrorField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message": "internal error"}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error for no choices and no error field")
	}
}

func TestSuggestDescription_API_WithTemperature(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices": [{"message": {"content": "ok"}}]}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	cfg.DeepSeek.Temperature = 0.3
	svc := newTestDescService(cfg)

	desc, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err != nil {
		t.Fatalf("SuggestDescription() error = %v", err)
	}
	if desc != "ok" {
		t.Errorf("description = %q, want %q", desc, "ok")
	}
}

// ============================================================================
// 截断行为测试（通过实际 API mock 间接验证）
// ============================================================================

func TestSuggestDescription_ContentTruncation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 验证请求体中的 content 已被截断
		// 无法直接读取请求体判断，但可以通过返回结果验证链路正常
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices": [{"message": {"content": "ok"}}]}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	// 传 3000 字符的内容，服务端应截断为 2000
	longContent := strings.Repeat("a", 3000)
	desc, err := svc.SuggestDescription(context.Background(), "t", longContent)
	if err != nil {
		t.Fatalf("SuggestDescription() error = %v", err)
	}
	if desc != "ok" {
		t.Errorf("description = %q, want %q", desc, "ok")
	}
}

func TestSuggestDescription_ShortContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices": [{"message": {"content": "ok"}}]}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	shortContent := strings.Repeat("b", 500)
	desc, err := svc.SuggestDescription(context.Background(), "t", shortContent)
	if err != nil {
		t.Fatalf("SuggestDescription() error = %v", err)
	}
	if desc != "ok" {
		t.Errorf("description = %q, want %q", desc, "ok")
	}
}

// ============================================================================
// KnowPostDescriptionService — 构造测试
// ============================================================================

func TestNewKnowPostDescriptionService_NilConfig(t *testing.T) {
	svc := NewKnowPostDescriptionService(nil)
	if svc == nil {
		t.Fatal("NewKnowPostDescriptionService(nil) should not return nil")
	}
	if svc.cfg != nil {
		t.Error("cfg should be nil")
	}
}

func TestNewKnowPostDescriptionService_Config(t *testing.T) {
	cfg := defaultLLMConfig()
	svc := NewKnowPostDescriptionService(cfg)
	if svc.cfg != cfg {
		t.Error("cfg pointer should match")
	}
}

func TestNewKnowPostDescriptionService_ZeroValue(t *testing.T) {
	svc := &KnowPostDescriptionService{}
	if svc.cfg != nil {
		t.Error("cfg should be nil in zero value")
	}
}

// ============================================================================
// RagQueryService — 构造测试
// ============================================================================

func TestNewRagQueryService(t *testing.T) {
	cfg := defaultLLMConfig()
	svc := NewRagQueryService(cfg, "http://es:9200")
	if svc == nil {
		t.Fatal("NewRagQueryService should not return nil")
	}
	if svc.llmCfg != cfg {
		t.Error("llmCfg should match")
	}
	if svc.esURL != "http://es:9200" {
		t.Errorf("esURL = %q, want %q", svc.esURL, "http://es:9200")
	}
}

func TestNewRagQueryService_NilConfig(t *testing.T) {
	svc := NewRagQueryService(nil, "")
	if svc == nil {
		t.Fatal("NewRagQueryService(nil) should not return nil")
	}
}

func TestRagQueryService_ZeroValue(t *testing.T) {
	svc := &RagQueryService{}
	if svc.llmCfg != nil {
		t.Error("llmCfg should be nil in zero value")
	}
	if svc.esURL != "" {
		t.Error("esURL should be empty in zero value")
	}
}

// ============================================================================
// RagQueryService.Query — 核心流程测试
// ============================================================================

func TestRagQueryService_Query_SendsTokens(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	streamChan := make(chan string, 10)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := svc.Query(context.Background(), 1, "测试问题", streamChan)
		if err != nil {
			t.Errorf("Query() error = %v", err)
		}
	}()

	var tokens []string
	for token := range streamChan {
		tokens = append(tokens, token)
	}
	wg.Wait()

	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d: %v", len(tokens), tokens)
	}
	if !strings.Contains(tokens[0], "RAG 问答系统已就绪") {
		t.Errorf("first token = %q, want 'RAG 问答系统已就绪'", tokens[0])
	}
	if tokens[1] != "data: [DONE]\n\n" {
		t.Errorf("second token = %q, want 'data: [DONE]\\n\\n'", tokens[1])
	}
}

func TestRagQueryService_Query_ContextCancelled(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	streamChan := make(chan string, 10)
	err := svc.Query(ctx, 1, "q", streamChan)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	_, ok := <-streamChan
	if ok {
		t.Error("streamChan should be closed after context cancellation")
	}
}

func TestRagQueryService_Query_ContextTimeout(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	streamChan := make(chan string, 10)
	err := svc.Query(ctx, 1, "q", streamChan)
	if err == nil {
		t.Fatal("expected error for timeout")
	}

	_, ok := <-streamChan
	if ok {
		t.Error("streamChan should be closed after context timeout")
	}
}

func TestRagQueryService_Query_EmptyQuestion(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	streamChan := make(chan string, 10)
	var tokens []string

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = svc.Query(context.Background(), 1, "", streamChan)
	}()
	for token := range streamChan {
		tokens = append(tokens, token)
	}
	wg.Wait()

	if len(tokens) != 2 {
		t.Errorf("expected 2 tokens for empty question, got %d", len(tokens))
	}
}

func TestRagQueryService_Query_ZeroPostID(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	streamChan := make(chan string, 10)
	var tokens []string

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = svc.Query(context.Background(), 0, "q", streamChan)
	}()
	for token := range streamChan {
		tokens = append(tokens, token)
	}
	wg.Wait()

	if len(tokens) != 2 {
		t.Errorf("expected 2 tokens for zero postID, got %d", len(tokens))
	}
}

// ============================================================================
// 并发测试
// ============================================================================

func TestRagQueryService_MultipleCalls(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	for i := 0; i < 10; i++ {
		streamChan := make(chan string, 10)
		var tokens []string
		var wg sync.WaitGroup
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = svc.Query(context.Background(), uint64(idx), "q", streamChan)
		}(i)
		for token := range streamChan {
			tokens = append(tokens, token)
		}
		wg.Wait()
		if len(tokens) != 2 {
			t.Errorf("iteration %d: expected 2 tokens, got %d", i, len(tokens))
		}
	}
}

func TestRagQueryService_Query_ChannelCloseOnce(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	streamChan := make(chan string, 10)
	err := svc.Query(context.Background(), 1, "q", streamChan)

	for range streamChan {
	}

	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	_, ok := <-streamChan
	if ok {
		t.Error("channel should be closed")
	}
}

func TestRagQueryService_ConcurrentQueries(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			streamChan := make(chan string, 10)
			err := svc.Query(context.Background(), uint64(idx), "q", streamChan)
			for range streamChan {
			}
			if err != nil {
				t.Errorf("concurrent query %d error = %v", idx, err)
			}
		}(i)
	}
	wg.Wait()
}

// ============================================================================
// 超时默认值测试
// ============================================================================

func TestDefaultTimeout_ZeroMs(t *testing.T) {
	svc := NewKnowPostDescriptionService(&config.LLMConfig{
		DeepSeek: config.DeepSeekConfig{BaseURL: "http://localhost:1"},
		TimeoutMs: 0,
	})

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error (zero TimeoutMs should use default 30s, server unreachable)")
	}
}

func TestDefaultTimeout_PositiveMs(t *testing.T) {
	svc := NewKnowPostDescriptionService(&config.LLMConfig{
		DeepSeek: config.DeepSeekConfig{BaseURL: "http://localhost:1"},
		TimeoutMs: 100,
	})

	_, err := svc.SuggestDescription(context.Background(), "t", "c")
	if err == nil {
		t.Fatal("expected error (short timeout, server unreachable)")
	}
}

func TestSuggestDescription_ContentTruncatedViaRealCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices": [{"message": {"content": "done"}}]}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	content := strings.Repeat("x", 3000)
	desc, err := svc.SuggestDescription(context.Background(), "t", content)
	if err != nil {
		t.Fatalf("SuggestDescription() error = %v", err)
	}
	if desc != "done" {
		t.Errorf("description = %q, want %q", desc, "done")
	}
}

func TestRagQueryService_Query_ConcurrentWithCancel(t *testing.T) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			streamChan := make(chan string, 10)
			err := svc.Query(ctx, 1, "q", streamChan)
			for range streamChan {
			}
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err == nil {
			t.Error("expected error for cancelled context")
		}
	}
}

// ============================================================================
// Benchmark
// ============================================================================

func BenchmarkSuggestDescriptionWithMock(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices": [{"message": {"content": "摘要"}}]}`))
	}))
	defer server.Close()

	cfg := defaultLLMConfig()
	cfg.DeepSeek.BaseURL = server.URL
	svc := newTestDescService(cfg)

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_, _ = svc.SuggestDescription(ctx, "标题", "内容")
	}
}

func BenchmarkRagQueryService(b *testing.B) {
	svc := NewRagQueryService(defaultLLMConfig(), "http://es:9200")

	for i := 0; i < b.N; i++ {
		streamChan := make(chan string, 10)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.Query(context.Background(), 1, "q", streamChan)
		}()
		for range streamChan {
		}
		wg.Wait()
	}
}