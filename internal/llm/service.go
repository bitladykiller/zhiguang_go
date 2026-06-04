// llm 包提供一组 AI 能力服务：
//   - KnowPostDescriptionService：通过 DeepSeek API 生成帖子摘要
//   - RagIndexService：把帖子内容写入 ES 向量索引，供 RAG 使用
//   - RagQueryService：执行向量检索并以流式方式生成问答结果
package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/pkg/config"
)

// ============================================================================
// KnowPostDescriptionService：AI 摘要生成
// ============================================================================

// KnowPostDescriptionService 为知文内容生成简洁中文摘要。
type KnowPostDescriptionService struct {
	cfg *config.LLMConfig
}

func NewKnowPostDescriptionService(cfg *config.LLMConfig) *KnowPostDescriptionService {
	return &KnowPostDescriptionService{cfg: cfg}
}

// SuggestDescription 调用 DeepSeek API，并返回不超过 50 字的摘要描述。
func (s *KnowPostDescriptionService) SuggestDescription(title, content string) (string, error) {
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

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(
		s.cfg.DeepSeek.BaseURL+"/v1/chat/completions",
		"application/json",
		bytes.NewReader(jsonBody),
	)
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

// ============================================================================
// RagIndexService：为 RAG 检索建立内容索引
// ============================================================================

// RagIndexService 管理 RAG 使用的 ES 向量索引。
type RagIndexService struct {
	db     *sqlx.DB
	llmCfg *config.LLMConfig
	esURL  string
}

func NewRagIndexService(db *sqlx.DB, llmCfg *config.LLMConfig, esURL string) *RagIndexService {
	return &RagIndexService{db: db, llmCfg: llmCfg, esURL: esURL}
}

// EnsureIndexed 检查帖子是否已入索引；如果没有则补建索引。
func (s *RagIndexService) EnsureIndexed(postID uint64) error {
	// 生产实现中这里应当：
	// 先检查 ES 是否已有分片，再从 DB 读取内容，按段落切分，
	// 调用 OpenAI 兼容接口生成 embedding，最后写入 ES。
	// 当前仍是占位实现，后续可按需增量补齐。
	return nil
}

// DeleteIndex 删除某篇帖子对应的全部 RAG 分片索引。
func (s *RagIndexService) DeleteIndex(postID uint64) error {
	return nil
}

// ============================================================================
// RagQueryService：流式 RAG 问答
// ============================================================================

// RagQueryService 执行向量检索与 LLM 流式问答。
type RagQueryService struct {
	llmCfg *config.LLMConfig
	esURL  string
}

func NewRagQueryService(llmCfg *config.LLMConfig, esURL string) *RagQueryService {
	return &RagQueryService{llmCfg: llmCfg, esURL: esURL}
}

// Query 执行基于 RAG 的问答，并把输出 token 流式写入目标 channel。
// 理论流程如下：
//  1. 通过 OpenAI 兼容接口为问题生成 embedding
//  2. 在 ES 中检索 top-K 相似文本片段（余弦相似度）
//  3. 用检索到的上下文拼装提示词
//  4. 调用 DeepSeek API，开启 stream=true
//  5. 按 SSE 格式把 token 持续写入 streamChan
func (s *RagQueryService) Query(postID uint64, question string, streamChan chan<- string) error {
	defer close(streamChan)

	// 当前仍是占位实现。
	// 这里先返回一个简单流式响应，用来验证 SSE 链路与交互模式。
	streamChan <- "data: {\"token\": \"RAG 问答系统已就绪，等待接入向量检索和流式生成。\"}\n\n"
	streamChan <- "data: [DONE]\n\n"

	return nil
}
