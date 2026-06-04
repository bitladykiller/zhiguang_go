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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

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

// SearchService 封装 Elasticsearch 客户端并提供搜索相关操作。
type SearchService struct {
	client    *elasticsearch.Client
	indexName string
	counter   SearchCounterClient
}

// NewSearchService 创建 ES 客户端，并确保索引已存在。
func NewSearchService(cfg struct {
	URIs      []string
	IndexName string
}) (*SearchService, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.URIs,
	})
	if err != nil {
		return nil, fmt.Errorf("create es client: %w", err)
	}

	svc := &SearchService{client: client, indexName: cfg.IndexName}

	// 启动时确保索引已存在
	if err := svc.EnsureIndex(); err != nil {
		return nil, fmt.Errorf("ensure index: %w", err)
	}

	return svc, nil
}

// SetCounterClient 注入搜索结果所需的用户态计数依赖。
func (s *SearchService) SetCounterClient(counter SearchCounterClient) {
	s.counter = counter
}

// EnsureIndex 在索引不存在时按预期 mapping 创建索引。
func (s *SearchService) EnsureIndex() error {
	res, err := s.client.Indices.Exists([]string{s.indexName})
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return s.ensureCompatibleMappings() // 索引已存在时，补齐兼容字段
	}

	res, err = s.client.Indices.Create(s.indexName, s.client.Indices.Create.WithBody(
		bytes.NewReader([]byte(indexMapping)),
	))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("create index failed: %s", string(body))
	}

	return nil
}

func (s *SearchService) ensureCompatibleMappings() error {
	// 为已经存在的旧开发索引补齐新查询路径依赖的字段。
	// WHY：本地环境可能保留了旧版本 schema 的索引，
	// 如果不补 mapping，按 tag_id 搜索会一直静默失效，直到用户手动删索引。
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
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("put mapping failed: %s", string(body))
	}

	return nil
}

// Search 执行全文检索，并使用 search_after 游标分页。
func (s *SearchService) Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error) {
	if size <= 0 {
		size = 20
	}

	tags := parseCSV(tagsCSV)
	afterValues := parseAfter(after)

	// 与 Java 版对齐：function_score + search_after + 高亮
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"function_score": map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{"multi_match": map[string]interface{}{
								"query":  keyword,
								"fields": []string{"title^3", "body"},
							}},
						},
						"filter": []map[string]interface{}{
							{"term": map[string]interface{}{"status": "published"}},
							{"term": map[string]interface{}{"visible": "public"}},
						},
					},
				},
				"functions": []map[string]interface{}{
					{
						"field_value_factor": map[string]interface{}{
							"field":    "like_count",
							"modifier": "log1p",
						},
						"weight": 2.0,
					},
					{
						"field_value_factor": map[string]interface{}{
							"field":    "view_count",
							"modifier": "log1p",
						},
						"weight": 1.0,
					},
				},
				"boost_mode": "sum",
			},
		},
		"size": size,
		"highlight": map[string]interface{}{
			"fields": map[string]interface{}{
				"title": map[string]interface{}{},
				"body":  map[string]interface{}{},
			},
		},
		"sort": []map[string]interface{}{
			{"_score": map[string]string{"order": "desc"}},
			{"publish_time": map[string]string{"order": "desc"}},
			{"like_count": map[string]string{"order": "desc"}},
			{"view_count": map[string]string{"order": "desc"}},
			{"id": map[string]string{"order": "desc"}},
		},
	}

	if len(tags) > 0 {
		query["query"].(map[string]interface{})["function_score"].(map[string]interface{})["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = append(
			query["query"].(map[string]interface{})["function_score"].(map[string]interface{})["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"].([]map[string]interface{}),
			map[string]interface{}{"terms": map[string]interface{}{"tags": tags}},
		)
	}
	if len(afterValues) > 0 {
		query["search_after"] = afterValues
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}

	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("search failed: %s", string(body))
	}

	var result struct {
		Hits struct {
			Hits []struct {
				Source    SearchIndexDoc      `json:"_source"`
				Score     float64             `json:"_score"`
				Sort      []interface{}       `json:"sort"`
				Highlight map[string][]string `json:"highlight"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	items := make([]knowpost.FeedItemResponse, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		source := hit.Source
		description := source.Description
		if snippet := buildSnippet(hit.Highlight); snippet != "" {
			description = snippet
		}
		var coverImage *string
		if len(source.ImgURLs) > 0 {
			first := source.ImgURLs[0]
			coverImage = &first
		}
		liked, faved := s.userFlags(ctx, currentUserID, source.ID)
		items = append(items, knowpost.FeedItemResponse{
			ID:             source.ID,
			Title:          stringPtrOrNil(source.Title),
			Description:    stringPtrOrNil(description),
			CoverImage:     coverImage,
			Tags:           source.Tags,
			AuthorAvatar:   source.AuthorAvatar,
			AuthorNickname: source.AuthorName,
			TagJson:        source.AuthorTagJSON,
			LikeCount:      source.LikeCount,
			FavoriteCount:  source.FavCount,
			Liked:          liked,
			Faved:          faved,
			IsTop:          boolPtr(source.IsTop),
		})
	}

	var nextAfter *string
	hasMore := len(items) >= size
	if len(result.Hits.Hits) > 0 {
		lastSort := result.Hits.Hits[len(result.Hits.Hits)-1].Sort
		if len(lastSort) > 0 {
			cursor := encodeAfter(lastSort)
			nextAfter = &cursor
		}
	}

	return &SearchResponse{
		Items:     items,
		NextAfter: nextAfter,
		HasMore:   hasMore,
	}, nil
}

// Suggest 返回给定前缀的自动补全建议。
func (s *SearchService) Suggest(ctx context.Context, prefix string, size int) ([]string, error) {
	if size <= 0 {
		size = 10
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

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}

	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("suggest failed: %s", string(body))
	}

	var result struct {
		Suggest map[string][]struct {
			Options []struct {
				Text string `json:"text"`
			} `json:"options"`
		} `json:"suggest"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
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

func parseCSV(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		text := strings.TrimSpace(part)
		if text != "" {
			result = append(result, text)
		}
	}
	return result
}

func parseAfter(after string) []interface{} {
	if strings.TrimSpace(after) == "" {
		return nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(after)
	if err != nil {
		return nil
	}
	parts := strings.Split(string(decoded), ",")
	values := make([]interface{}, 0, len(parts))
	for i, part := range parts {
		if i == 0 {
			if value, err := strconv.ParseFloat(part, 64); err == nil {
				values = append(values, value)
				continue
			}
		}
		if value, err := strconv.ParseInt(part, 10, 64); err == nil {
			values = append(values, value)
			continue
		}
		values = append(values, part)
	}
	return values
}

func encodeAfter(sortValues []interface{}) string {
	parts := make([]string, 0, len(sortValues))
	for _, value := range sortValues {
		switch typed := value.(type) {
		case float64:
			parts = append(parts, strconv.FormatFloat(typed, 'f', -1, 64))
		case string:
			parts = append(parts, typed)
		case json.Number:
			parts = append(parts, typed.String())
		default:
			parts = append(parts, fmt.Sprint(typed))
		}
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, ",")))
}

func buildSnippet(highlight map[string][]string) string {
	if len(highlight) == 0 {
		return ""
	}
	parts := make([]string, 0, 2)
	if values := highlight["title"]; len(values) > 0 {
		parts = append(parts, values[0])
	}
	if values := highlight["body"]; len(values) > 0 {
		parts = append(parts, values[0])
	}
	return strings.Join(parts, " ")
}

func stringPtrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func (s *SearchService) userFlags(ctx context.Context, currentUserID *uint64, entityID string) (*bool, *bool) {
	if currentUserID == nil || s.counter == nil {
		return nil, nil
	}
	liked, _ := s.counter.IsLiked(ctx, *currentUserID, "knowpost", entityID)
	faved, _ := s.counter.IsFaved(ctx, *currentUserID, "knowpost", entityID)
	return &liked, &faved
}

// IndexDocument 创建或更新一篇搜索文档。
func (s *SearchService) IndexDocument(ctx context.Context, doc *SearchIndexDoc) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		return err
	}

	res, err := s.client.Index(
		s.indexName,
		&buf,
		s.client.Index.WithContext(ctx),
		s.client.Index.WithDocumentID(doc.ID),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("index failed: %s", string(body))
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
		return err
	}
	defer res.Body.Close()
	return nil
}
