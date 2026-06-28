package search

import "context"

// SearchServiceInterface 定义搜索模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *SearchService，使得 handler 可以独立于
// service 实现进行单元测试，同时支持在搜索服务不可用时注入 nil。
type SearchServiceInterface interface {
	Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	Suggest(ctx context.Context, prefix string, size int) ([]string, error)
}

// 编译期断言：*SearchService 实现了 SearchServiceInterface。
var _ SearchServiceInterface = (*SearchService)(nil)
