package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/zhiguang/app/pkg/config"
)

type KnowPostDescriptionService struct {
	cfg    *config.LLMConfig
	client *http.Client
}

func NewKnowPostDescriptionService(cfg *config.LLMConfig) *KnowPostDescriptionService {
	timeout := 30 * time.Second
	if cfg != nil && cfg.TimeoutMs > 0 {
		timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond
	}
	return &KnowPostDescriptionService{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
	}
}

func (s *KnowPostDescriptionService) SuggestDescription(ctx context.Context, title, content string) (string, error) {
	if utf8.RuneCountInString(content) > 2000 {
		content = string([]rune(content)[:2000])
	} else if len(content) > 2000 {
		content = content[:2000]
	}

	reqBody := map[string]interface{}{
		"model": s.cfg.DeepSeek.Model,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": "你是一个专业的内容摘要助手，请用简洁的语言为以下文章生成一段不超过50字的摘要描述。",
			},
			{
				"role":    "user",
				"content": fmt.Sprintf("标题：%s\n内容：%s\n\n请生成不超过50字的摘要：", title, content),
			},
		},
		"temperature": s.cfg.DeepSeek.Temperature,
		"max_tokens":  100,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("suggest description: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.cfg.DeepSeek.BaseURL+"/v1/chat/completions",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("deepseek api: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("suggest description: read response: %w", err)
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}

	if result.Error != nil {
		return "", fmt.Errorf("api error: %s", result.Error.Message)
	}

	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices returned")
	}

	return strings.TrimSpace(result.Choices[0].Message.Content), nil
}

type RagQueryService struct {
	llmCfg *config.LLMConfig
	esURL  string
}

func NewRagQueryService(llmCfg *config.LLMConfig, esURL string) *RagQueryService {
	return &RagQueryService{llmCfg: llmCfg, esURL: esURL}
}

func (s *RagQueryService) Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error {
	defer close(streamChan)

	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case streamChan <- "data: {\"token\": \"RAG 问答系统已就绪，等待接入向量检索和流式生成。\"}\n\n":
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case streamChan <- "data: [DONE]\n\n":
	}

	return nil
}