package search

import "context"

// CounterReader 定义搜索索引投影过程中所需的计数读取接口子集。
//
// WHY：
//   - projector 只需要读取 like/fav 聚合值；
//   - 不应该为了这一点能力依赖整个 counter service 的写路径语义。
type CounterReader interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
}

// DocumentIndexer 定义投影器写入搜索文档所需的最小接口。
//
// projector 只关心“把文档写进搜索索引”，不应该绑定 SearchService 的全部查询能力。
type DocumentIndexer interface {
	IndexDocument(ctx context.Context, doc *SearchIndexDoc) error
}
