package llm

import "context"

// DescriptionServiceInterface 定义 AI 摘要生成服务对外暴露的业务方法。
type DescriptionServiceInterface interface {
	SuggestDescription(ctx context.Context, title, content string) (string, error)
}

// RagQueryServiceInterface 定义 RAG 问答服务对外暴露的业务方法。
type RagQueryServiceInterface interface {
	Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error
}

// 编译期断言。
var (
	_ DescriptionServiceInterface = (*KnowPostDescriptionService)(nil)
	_ RagQueryServiceInterface    = (*RagQueryService)(nil)
)
