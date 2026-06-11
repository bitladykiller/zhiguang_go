// search 包实现基于 Elasticsearch 的内容搜索能力，
// 同时提供面向知文内容的自动补全建议功能。
//
// 主要特性：
//   - 基于 BM25 相关性评分的全文检索
//   - 使用 function_score 融合 BM25 与 like_count / view_count 的权重，
//     使热门内容能获得合理的排序提升。
//   - 使用 search_after 游标分页，替代传统的 from/size 深分页，
//     避免深分页场景下 ES 集群的排序性能问题。
//   - 基于 completion suggester 的前缀自动补全，
//     同时支持标题和标签作为补全建议的来源。
//   - 索引映射与 Java 版对齐，确保同集群下混用 Java/Go 应用时索引兼容。
//
// 数据同步流程：
//
//	写操作（事务内） → outbox 表 → Canal 捕获 binlog → Kafka canal-outbox 主题
//	→ search.OutboxConsumer → search.KnowPostProjector → Elasticsearch
package search

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/zhiguang/app/internal/knowpost"
)

// SearchIndexDoc 是知文内容在 ES 中的文档结构。
type SearchIndexDoc struct {
	ID            string        `json:"id"`
	Title         string        `json:"title"`
	Description   string        `json:"description"`
	Body          string        `json:"body,omitempty"`
	TagID         *uint64       `json:"tag_id,omitempty"`
	Tags          []string      `json:"tags"`
	AuthorID      string        `json:"author_id"`
	AuthorAvatar  *string       `json:"author_avatar,omitempty"`
	AuthorName    string        `json:"author_name"`
	AuthorTagJSON *string       `json:"author_tag_json,omitempty"`
	ImgURLs       []string      `json:"img_urls,omitempty"`
	LikeCount     int64         `json:"like_count"`
	FavCount      int64         `json:"favorite_count"`
	ViewCount     int64         `json:"view_count"`
	PublishTime   *string       `json:"publish_time,omitempty"`
	IsTop         bool          `json:"is_top"`
	Status        string        `json:"status"`
	Visible       string        `json:"visible"`
	TitleSuggest  string        `json:"title_suggest,omitempty"`
	Suggest       *SuggestField `json:"suggest,omitempty"`
}

// SuggestField 表示 ES completion suggest 字段结构。
type SuggestField struct {
	Input  []string `json:"input"`
	Weight int      `json:"weight,omitempty"`
}

// SearchResponse 是搜索接口的响应结构，对齐 Java 版返回。
type SearchResponse struct {
	Items     []knowpost.FeedItemResponse `json:"items"`
	NextAfter *string                     `json:"next_after,omitempty"`
	HasMore   bool                        `json:"has_more"`
}

// SearchCounterClient 定义搜索结果需要的用户态计数读取接口。
type SearchCounterClient interface {
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
}

// SearchService 封装 Elasticsearch 客户端并提供搜索相关操作。
type SearchService struct {
	client    *elasticsearch.Client
	indexName string
	counter   SearchCounterClient
}

// ServiceConfig 描述搜索服务构造期需要的配置和跨领域依赖。
//
// Counter 属于可选依赖：
//   - 为空时，搜索结果仍可返回内容主体；
//   - 仅当前用户的 liked/faved 衍生状态会降级为空。
type ServiceConfig struct {
	URIs      []string
	IndexName string
	Counter   SearchCounterClient
}
