package llm

// DescriptionSuggester 定义 AI 摘要接口。
//
// 摘要生成单独抽成接口后，可以和 RAG 问答独立降级、独立替换实现。
type DescriptionSuggester interface {
	SuggestDescription(title, content string) (string, error)
}

// RagQueryUseCase 定义 RAG 流式问答接口。
//
// Handler 只依赖“流式问答”这个业务语义，不直接依赖检索、向量化或具体大模型客户端。
type RagQueryUseCase interface {
	Query(postID uint64, question string, streamChan chan<- string) error
}
