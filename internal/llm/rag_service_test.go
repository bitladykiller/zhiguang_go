package llm

import (
	"context"
	"errors"
	"testing"

	"github.com/zhiguang/app/pkg/config"
)

func TestRagQueryServiceQueryStreamsPlaceholderTokens(t *testing.T) {
	service := NewRagQueryService(&config.LLMConfig{}, "http://example.com")
	streamChan := make(chan string, 2)

	err := service.Query(context.Background(), 1, "question", streamChan)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil", err)
	}

	var tokens []string
	for token := range streamChan {
		tokens = append(tokens, token)
	}

	if len(tokens) != 2 {
		t.Fatalf("expected 2 placeholder tokens, got %d", len(tokens))
	}
	if tokens[1] != "data: [DONE]\n\n" {
		t.Fatalf("unexpected final token: %q", tokens[1])
	}
}

func TestRagQueryServiceQueryHonorsCanceledContext(t *testing.T) {
	service := NewRagQueryService(&config.LLMConfig{}, "http://example.com")
	streamChan := make(chan string, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := service.Query(ctx, 1, "question", streamChan)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Query() error = %v, want context.Canceled", err)
	}

	if _, ok := <-streamChan; ok {
		t.Fatal("expected stream channel to be closed without tokens")
	}
}
