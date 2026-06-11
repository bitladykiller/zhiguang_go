package search

import "context"

// SearchUseCase 定义搜索 HTTP 层依赖的业务接口。
//
// 搜索 handler 面向的是查询协议，而不是 Elasticsearch 的具体实现，
// 这样在降级返回 503、替换搜索实现或做 handler 测试时都更稳定。
type SearchUseCase interface {
	Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	Suggest(ctx context.Context, prefix string, size int) ([]string, error)
}
