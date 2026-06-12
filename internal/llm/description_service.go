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

	"github.com/zhiguang/app/pkg/config"
)

// KnowPostDescriptionService 为知文内容生成简洁中文摘要。
type KnowPostDescriptionService struct {
	cfg *config.LLMConfig
}

// NewKnowPostDescriptionService 创建 AI 摘要生成服务。
func NewKnowPostDescriptionService(cfg *config.LLMConfig) *KnowPostDescriptionService {
	return &KnowPostDescriptionService{cfg: cfg}
}

// SuggestDescription 调用 DeepSeek Chat API 为知文生成不超过 50 字的中文摘要。
//
// WHY 透传 context：
//   - 这条链路会发起外部 HTTP 请求；
//   - 当客户端断开或上游超时取消时，底层请求应该尽快停止，避免浪费连接和配额。
func (s *KnowPostDescriptionService) SuggestDescription(ctx context.Context, title, content string) (string, error) {
	// 截断正文，避免超过模型 token 限制
	if len(content) > 2000 {
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
		return "", err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		s.cfg.DeepSeek.BaseURL+"/v1/chat/completions",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("deepseek api: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("parse response: %w, body: %s", err, string(body))
	}

	if result.Error != nil {
		return "", fmt.Errorf("api error: %s", result.Error.Message)
	}

	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices returned")
	}

	return strings.TrimSpace(result.Choices[0].Message.Content), nil
}
