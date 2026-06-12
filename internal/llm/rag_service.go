package llm

import (
	"context"

	"github.com/zhiguang/app/pkg/config"
)

// RagQueryService 执行向量检索与 LLM 流式问答。
type RagQueryService struct {
	llmCfg *config.LLMConfig
	esURL  string
}

// NewRagQueryService 创建 RAG 问答服务。
func NewRagQueryService(llmCfg *config.LLMConfig, esURL string) *RagQueryService {
	return &RagQueryService{llmCfg: llmCfg, esURL: esURL}
}

// Query 执行基于 RAG 的问答，并把输出 token 流式写入目标 channel。
//
// 当前实现仍是占位逻辑，但接口已经显式接收 context，便于未来接入：
//   - embedding 请求
//   - ES 检索
//   - LLM 流式输出
//
// 这些步骤都属于可取消的外部 IO，不应该脱离 HTTP 请求生命周期。
func (s *RagQueryService) Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error {
	defer close(streamChan)

	if err := ctx.Err(); err != nil {
		return err
	}

	for _, token := range []string{
		"data: {\"token\": \"RAG 问答系统已就绪，等待接入向量检索和流式生成。\"}\n\n",
		"data: [DONE]\n\n",
	} {
		if err := ctx.Err(); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case streamChan <- token:
		}
	}

	return nil
}
