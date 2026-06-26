package llm

import "context"

// DescriptionServicer 定义 AI 摘要生成服务对外暴露的业务方法。
type DescriptionServicer interface {
	SuggestDescription(ctx context.Context, title, content string) (string, error)
}

// RagQueryServicer 定义 RAG 问答服务对外暴露的业务方法。
type RagQueryServicer interface {
	Query(ctx context.Context, postID uint64, question string, streamChan chan<- string) error
}

// RagQueryServiceInterface 是 RagQueryServicer 的类型别名，保持向后兼容。
//
// Deprecated: 请直接使用 RagQueryServicer 接口。
type RagQueryServiceInterface = RagQueryServicer

// 编译期断言。
var (
	_ DescriptionServicer = (*KnowPostDescriptionService)(nil)
	_ RagQueryServicer    = (*RagQueryService)(nil)
)
