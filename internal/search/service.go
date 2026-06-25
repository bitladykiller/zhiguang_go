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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/jsonutil"
)

// searchIndexDoc 是知文内容在 ES 中的文档结构。
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

// SearchCounterClient 定义搜索结果需要的用户态计数读取接口。
type SearchCounterClient interface {
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	BatchIsLiked(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
	BatchIsFaved(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
}

// indexMapping 是知文搜索索引的 ES mapping 模板。
const indexMapping = `{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "analysis": {
      "analyzer": {
        "zh_analyzer": {
          "type": "standard"
}
}
    }
  },
  "mappings": {
    "properties": {
      "id":           { "type": "keyword" },
      "title":        { "type": "text", "analyzer": "zh_analyzer" },
      "description":  { "type": "text", "analyzer": "zh_analyzer" },
      "tag_id":       { "type": "long" },
      "tags":         { "type": "keyword" },
      "author_id":    { "type": "keyword" },
      "author_avatar": { "type": "keyword", "index": false },
      "author_name":  { "type": "text" },
      "author_tag_json": { "type": "keyword", "index": false },
      "img_urls":     { "type": "keyword", "index": false },
      "body":         { "type": "text", "analyzer": "zh_analyzer" },
      "like_count":   { "type": "long" },
      "favorite_count": { "type": "long" },
      "view_count":   { "type": "long" },
      "publish_time": { "type": "date" },
      "is_top":       { "type": "boolean" },
      "status":       { "type": "keyword" },
      "visible":      { "type": "keyword" },
      "title_suggest": { "type": "completion", "analyzer": "zh_analyzer" },
      "suggest":      { "type": "completion", "analyzer": "zh_analyzer" }
    }
  }
}`

// SearchService 封装 Elasticsearch 客户端并对外提供搜索相关操作。
type SearchService struct {
	client    *elasticsearch.Client
	indexName string
	counter   SearchCounterClient
	logger    *zap.Logger
}

// NewSearchService 创建 ES 客户端并确保索引存在。
func NewSearchService(cfg struct {
	URIs       []string
	IndexName  string
	MaxRetries int
}, counter SearchCounterClient) (*SearchService, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     cfg.URIs,
		MaxRetries:    cfg.MaxRetries,
		RetryOnStatus: defaultRetryOnStatus,
	})
	if err != nil {
		return nil, fmt.Errorf("create es client: %w", err)
	}

	svc := &SearchService{client: client, indexName: cfg.IndexName, counter: counter, logger: zap.L()}

	// 启动时确保索引已存在
	if err := svc.EnsureIndex(); err != nil {
		return nil, fmt.Errorf("ensure index: %w", err)
	}

	return svc, nil
}

// readESError 从 ES 错误响应中读取 body 并构造错误信息。
func readESError(res *esapi.Response, action string) error {
	defer res.Body.Close()
	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		return fmt.Errorf("search error (status=%d, failed to read body: %w)", res.StatusCode, readErr)
	}
	return fmt.Errorf("%s failed: %s", action, string(body))
}

// EnsureIndex 检查索引是否存在，不存在时创建。
func (s *SearchService) EnsureIndex() error {
	res, err := s.client.Indices.Exists([]string{s.indexName})
	if err != nil {
		return fmt.Errorf("ensure index: exists: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return s.ensureCompatibleMappings()
	}

	createRes, err := s.client.Indices.Create(s.indexName, s.client.Indices.Create.WithBody(
		bytes.NewReader([]byte(indexMapping)),
	))
	if err != nil {
		return fmt.Errorf("ensure index: create: %w", err)
	}
	defer createRes.Body.Close()

	if createRes.IsError() {
		return readESError(createRes, "create index")
	}

	return nil
}

// ensureCompatibleMappings 为旧索引补齐新查询路径依赖的字段映射。
func (s *SearchService) ensureCompatibleMappings() error {
	const mappingUpdate = `{
	  "properties": {
	    "tag_id": { "type": "long" },
	    "author_avatar": { "type": "keyword", "index": false },
	    "author_tag_json": { "type": "keyword", "index": false },
	    "img_urls": { "type": "keyword", "index": false },
	    "body": { "type": "text", "analyzer": "zh_analyzer" },
	    "favorite_count": { "type": "long" },
	    "view_count": { "type": "long" },
	    "title_suggest": { "type": "completion", "analyzer": "zh_analyzer" }
}
	}`

	res, err := s.client.Indices.PutMapping(
		[]string{s.indexName},
		bytes.NewReader([]byte(mappingUpdate)),
	)
	if err != nil {
		return fmt.Errorf("ensure compatible mappings: put mapping: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return readESError(res, "put mapping")
	}

	return nil
}

const (
	defaultSearchSize  = 20
	defaultSuggestSize = 10
)

var defaultRetryOnStatus = []int{502, 503, 504, 429}

// Search 执行全文检索，使用 function_score 融合 BM25 和相关指标权重，并通过 search_after 游标分页。
func (s *SearchService) Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error) {
	if size <= 0 {
		size = defaultSearchSize
	}

	tags := parseCSV(tagsCSV)
	afterValues := parseAfter(after)

	query := s.buildSearchQuery(keyword, tags, afterValues, size)

	raw, err := s.executeSearch(ctx, query)
	if err != nil {
		return nil, err
	}

	items, likedMap, favedMap := s.decodeAndEnrich(ctx, raw, currentUserID)
	nextAfter, hasMore := s.buildCursor(raw, size)

	items = s.applyLikedFaved(items, likedMap, favedMap)

	return &SearchResponse{
		Items:     items,
		NextAfter: nextAfter,
		HasMore:   hasMore,
	}, nil
}

// searchHit 表示 ES 搜索结果中的单个 hit。
type searchHit struct {
	Source    SearchIndexDoc      `json:"_source"`
	Score     float64             `json:"_score"`
	Sort      []interface{}       `json:"sort"`
	Highlight map[string][]string `json:"highlight"`
}

func (s *SearchService) doSearchRequest(ctx context.Context, body interface{}) (*esapi.Response, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return nil, fmt.Errorf("search: encode query: %w", err)
	}

	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("search: es request: %w", err)
	}

	if res.IsError() {
		defer res.Body.Close()
		bodyBytes, readErr := io.ReadAll(res.Body)
		if readErr != nil {
			return nil, fmt.Errorf("search error (status=%d, failed to read body: %w)", res.StatusCode, readErr)
		}
		return nil, fmt.Errorf("search failed: %s", string(bodyBytes))
	}
	return res, nil
}

// executeSearch 发送 ES 搜索请求并返回原始响应。
func (s *SearchService) executeSearch(ctx context.Context, query map[string]interface{}) ([]searchHit, error) {
	res, err := s.doSearchRequest(ctx, query)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result struct {
		Hits struct {
			Hits []searchHit `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("search: decode response: %w", err)
	}
	return result.Hits.Hits, nil
}

// decodeAndEnrich 将 ES 结果解析为 SearchItem 列表，并返回 liked/faved 状态映射。
func (s *SearchService) decodeAndEnrich(ctx context.Context, hits []searchHit, currentUserID *uint64) ([]SearchItem, map[string]bool, map[string]bool) {
	items := make([]SearchItem, 0, len(hits))

	var likedMap, favedMap map[string]bool
	if currentUserID != nil && s.counter != nil && len(hits) > 0 {
		hitIDs := make([]string, len(hits))
		for i, hit := range hits {
			hitIDs[i] = hit.Source.ID
		}
		var err error
		likedMap, err = s.counter.BatchIsLiked(ctx, *currentUserID, "knowpost", hitIDs)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("batch is liked failed", zap.Error(err))
			}
		}
		favedMap, err = s.counter.BatchIsFaved(ctx, *currentUserID, "knowpost", hitIDs)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("batch is faved failed", zap.Error(err))
			}
		}
	}

	for _, hit := range hits {
		source := hit.Source
		description := source.Description
		if snippet := buildSnippet(hit.Highlight); snippet != "" {
			description = snippet
		}
		var coverImage *string
		if len(source.ImgURLs) > 0 {
			coverImage = &source.ImgURLs[0]
		}
		items = append(items, SearchItem{
			ID:             source.ID,
			Title:          jsonutil.StrPtr(source.Title),
			Description:    jsonutil.StrPtr(description),
			CoverImage:     coverImage,
			Tags:           source.Tags,
			AuthorAvatar:   source.AuthorAvatar,
			AuthorNickname: source.AuthorName,
			TagJson:        source.AuthorTagJSON,
			LikeCount:      source.LikeCount,
			FavoriteCount:  source.FavCount,
			IsTop:          boolPtr(source.IsTop),
		})
	}
	return items, likedMap, favedMap
}

// applyLikedFaved 为每篇结果填充当前用户的点赞/收藏状态。
func (s *SearchService) applyLikedFaved(items []SearchItem, likedMap, favedMap map[string]bool) []SearchItem {
	if likedMap == nil && favedMap == nil {
		return items
	}
	for i := range items {
		items[i].ApplyLikedFaved(likedMap, favedMap)
	}
	return items
}

// buildCursor 根据 ES 结果构建翻页游标和 hasMore 标记。
func (s *SearchService) buildCursor(hits []searchHit, size int) (*string, bool) {
	hasMore := len(hits) >= size
	var nextAfter *string
	if len(hits) > 0 {
		lastSort := hits[len(hits)-1].Sort
		if len(lastSort) > 0 {
			cursor := encodeAfter(lastSort)
			nextAfter = &cursor
		}
	}
	return nextAfter, hasMore
}

// Suggest 根据前缀返回自动补全建议列表。
func (s *SearchService) Suggest(ctx context.Context, prefix string, size int) ([]string, error) {
	if size <= 0 {
		size = defaultSuggestSize
	}

	query := map[string]interface{}{
		"suggest": map[string]interface{}{
			"title-suggest": map[string]interface{}{
				"prefix": prefix,
				"completion": map[string]interface{}{
					"field": "suggest",
					"size":  size,
				},
			},
		},
	}

	res, err := s.doSearchRequest(ctx, query)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result struct {
		Suggest map[string][]struct {
			Options []struct {
				Text string `json:"text"`
			} `json:"options"`
		} `json:"suggest"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("suggest: decode response: %w", err)
	}

	options := result.Suggest["title-suggest"]
	seen := make(map[string]struct{}, size)
	suggestions := make([]string, 0, size)
	for _, entry := range options {
		for _, option := range entry.Options {
			text := option.Text
			if text == "" {
				continue
			}
			if _, exists := seen[text]; exists {
				continue
			}
			seen[text] = struct{}{}
			suggestions = append(suggestions, text)
			if len(suggestions) >= size {
				return suggestions, nil
			}
		}
	}

	return suggestions, nil
}

// IndexDocument 将搜索文档索引到 ES（创建或全量替换）。
func (s *SearchService) IndexDocument(ctx context.Context, doc *SearchIndexDoc) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		return fmt.Errorf("index document: encode: %w", err)
	}

	res, err := s.client.Index(
		s.indexName,
		&buf,
		s.client.Index.WithContext(ctx),
		s.client.Index.WithDocumentID(doc.ID),
	)
	if err != nil {
		return fmt.Errorf("index document: es request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return readESError(res, "index")
	}

	return nil
}

// DeleteDocument 从搜索索引中删除一篇文档。
func (s *SearchService) DeleteDocument(ctx context.Context, id string) error {
	res, err := s.client.Delete(
		s.indexName,
		id,
		s.client.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("delete document: es request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return readESError(res, "delete")
	}
	return nil
}
