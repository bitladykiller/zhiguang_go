package search

import "context"

// SearchServicer 定义搜索模块对外暴露的业务方法。
//
// Handler 依赖此接口而非具体 *SearchService，使得 handler 可以独立于
// service 实现进行单元测试，也支持在搜索服务不可用时注入 nil 值。
type SearchServicer interface {
	Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error)
	Suggest(ctx context.Context, prefix string, size int) ([]string, error)
}

// DocumentIndexer 定义搜索索引文档写入能力，便于 projector 与 service 解耦。
type DocumentIndexer interface {
	IndexDocument(ctx context.Context, doc *SearchIndexDoc) error
}

// SearchServiceInterface 是 SearchServicer + DocumentIndexer 的组合接口，
// 供 bootstrap 在初始化时统一传递搜索服务实例。
type SearchServiceInterface interface {
	SearchServicer
	DocumentIndexer
}

// 编译期断言：*SearchService 实现了 SearchServicer 和 SearchServiceInterface。
var (
	_ SearchServicer         = (*SearchService)(nil)
	_ DocumentIndexer        = (*SearchService)(nil)
	_ SearchServiceInterface = (*SearchService)(nil)
)
