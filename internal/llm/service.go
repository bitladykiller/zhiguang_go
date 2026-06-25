// llm 包提供一组 AI 能力服务：
//   - KnowPostDescriptionService：通过 DeepSeek API 生成帖子简洁的中文摘要（不超过 50 字）
//   - RagQueryService：执行向量检索并以流式 SSE 方式生成问答结果
//
// 使用方式：
//   这些服务在配置不完整时不会阻塞服务启动，而是由调用方判断并返回 503。
//   在 bootstrap 中通过 buildDescriptionService / buildRagQueryService 函数
//   检测配置完整性后创建服务实例或返回 nil。
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

const (
	defaultLLMTimeout   = 30 * time.Second
	maxContentLength    = 2000
	defaultLLMMaxTokens = 100
)

// ============================================================================
// KnowPostDescriptionService：AI 摘要生成
// ============================================================================

// KnowPostDescriptionService 为知文内容生成简洁中文摘要。
type KnowPostDescriptionService struct {
	cfg *config.LLMConfig
}

// NewKnowPostDescriptionService 创建 AI 摘要生成服务。
//
// 参数：
//   - cfg: LLM 配置（包含 DeepSeek 的 APIKey、BaseURL、Model、Temperature）
func NewKnowPostDescriptionService(cfg *config.LLMConfig) *KnowPostDescriptionService {
	return &KnowPostDescriptionService{cfg: cfg}
}

// SuggestDescription 调用 DeepSeek Chat API 为知文生成不超过 50 字的中文摘要。
//
// 参数：
//   - title: 知文标题
//   - content: 知文正文（超过 2000 字会被自动截断）
//
// 返回值：
//   - string: AI 生成的摘要文本
//   - error: 如果 DeepSeek API 调用失败、响应解析失败或返回空结果则返回错误
//
// 函数调用说明：
//   - http.Post(url, contentType, body):
//     Go 标准库的 HTTP POST 请求。需要设置超时防止 API 阻塞。
//   - json.Marshal(reqBody):
//     构造请求体。messages 数组包含 system prompt 和 user prompt 两个消息。
//   - io.ReadAll(resp.Body):
//     读取完整的 API 响应体后解析。大响应场景下应使用流式解析。
//   - json.Unmarshal(body, &result):
//     解析 DeepSeek 兼容的 OpenAI 格式响应。
//     标准格式：{"choices": [{"message": {"content": "..."}}]}
func (s *KnowPostDescriptionService) SuggestDescription(ctx context.Context, title, content string) (string, error) {
	// 截断正文，避免超过模型 token 限制
	if len(content) > maxContentLength {
		content = content[:maxContentLength]
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
		"max_tokens":  defaultLLMMaxTokens,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("suggest description: marshal request: %w", err)
	}

	// 使用配置的超时，未配置则默认 30 秒
	timeout := defaultLLMTimeout
	if s.cfg.TimeoutMs > 0 {
		timeout = time.Duration(s.cfg.TimeoutMs) * time.Millisecond
	}
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.cfg.DeepSeek.BaseURL+"/v1/chat/completions",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
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

// ============================================================================
// RagQueryService：流式 RAG 问答
// ============================================================================

// RagQueryService 执行向量检索与 LLM 流式问答。
type RagQueryService struct {
	llmCfg *config.LLMConfig
	esURL  string
}

// NewRagQueryService 创建 RAG 问答服务。
//
// 参数：
//   - llmCfg: LLM 配置（含 DeepSeek chat 模型和 OpenAI embedding 模型）
//   - esURL: Elasticsearch 集群地址
func NewRagQueryService(llmCfg *config.LLMConfig, esURL string) *RagQueryService {
	return &RagQueryService{llmCfg: llmCfg, esURL: esURL}
}

// Query 执行基于 RAG 的问答，并把输出 token 流式写入目标 channel。
//
// 完整流程（当前为占位实现，待向量检索链路就绪后启用）：
//  1. 使用 OpenAI 兼容接口为问题生成 embedding。
//  2. 在 ES 中检索 top-K 相似文本片段（余弦相似度）。
//  3. 用检索到的上下文拼装 prompt。
//  4. 调用 DeepSeek API 开启 stream=true 做流式生成。
//  5. 将 token 逐段按 SSE 格式写入 streamChan。
//
// 当前实现：
//   返回一条占位消息，用于验证 SSE 链路是否正常。
//   客户端可通过检查消息内容判断服务端是否真正就绪。
//
// 参数：
//   - postID: 知文 ID（用于筛选检索范围）
//   - question: 用户提出的问题
//   - streamChan: 用于写入 SSE 格式 token 的 channel（函数会在完成后 close）
func (s *RagQueryService) Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error {
	defer close(streamChan)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rag query: context: %w", err)
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
