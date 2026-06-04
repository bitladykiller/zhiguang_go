// search 包实现基于 Elasticsearch 的内容搜索能力，
// 同时提供面向知文内容的自动补全建议功能。
//
// 主要特性：
//   - 基于 BM25 相关性评分的全文检索
//   - 使用 function_score 融合 BM25 与 like_count 权重
//   - 使用 from/size 的标准分页查询
//   - 基于 completion suggester 的前缀自动补全
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8"
)

// SearchIndexDoc 是知文内容在 ES 中的文档结构。
type SearchIndexDoc struct {
	ID          string        `json:"id"`
	Title       string        `json:"title"`
	Description string        `json:"description"`
	TagID       *uint64       `json:"tag_id,omitempty"`
	Tags        []string      `json:"tags"`
	AuthorID    string        `json:"author_id"`
	AuthorName  string        `json:"author_name"`
	LikeCount   int64         `json:"like_count"`
	FavCount    int64         `json:"fav_count"`
	PublishTime *string       `json:"publish_time,omitempty"`
	IsTop       bool          `json:"is_top"`
	Status      string        `json:"status"`
	Visible     string        `json:"visible"`
	Suggest     *SuggestField `json:"suggest,omitempty"`
}

// SuggestField 表示 ES completion suggest 字段结构。
type SuggestField struct {
	Input  []string `json:"input"`
	Weight int      `json:"weight,omitempty"`
}

// SearchResponse 是搜索接口的响应结构。
type SearchResponse struct {
	Items    []SearchItem `json:"items"`
	Total    int64        `json:"total"`
	Page     int          `json:"page"`
	PageSize int          `json:"page_size"`
}

// SearchItem 表示一条搜索结果。
type SearchItem struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	TagID       *uint64  `json:"tag_id,omitempty"`
	Tags        []string `json:"tags"`
	AuthorID    string   `json:"author_id"`
	AuthorName  string   `json:"author_name"`
	LikeCount   int64    `json:"like_count"`
	FavCount    int64    `json:"fav_count"`
	Score       float64  `json:"_score"`
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
      "author_name":  { "type": "text" },
      "like_count":   { "type": "long" },
      "fav_count":    { "type": "long" },
      "publish_time": { "type": "date" },
      "is_top":       { "type": "boolean" },
      "status":       { "type": "keyword" },
      "visible":      { "type": "keyword" },
      "suggest":      { "type": "completion", "analyzer": "zh_analyzer" }
    }
  }
}`

// SearchService 封装 Elasticsearch 客户端并提供搜索相关操作。
type SearchService struct {
	client    *elasticsearch.Client
	indexName string
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
	    "tag_id": { "type": "long" }
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

// Search 执行全文检索，并支持可选的 tag 过滤。
func (s *SearchService) Search(ctx context.Context, keyword string, tagID *uint64, page, size int) (*SearchResponse, error) {
	if size <= 0 {
		size = 20
	}
	if page <= 0 {
		page = 1
	}

	// 构建 function_score 查询，把 BM25 与 like_count 权重混合起来
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"function_score": map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{"multi_match": map[string]interface{}{
								"query":  keyword,
								"fields": []string{"title^3", "description^2", "tags"},
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
							"factor":   0.1,
							"modifier": "log1p",
						},
						"weight": 0.3,
					},
				},
				"boost_mode": "sum",
			},
		},
		"from": (page - 1) * size,
		"size": size,
		"sort": []map[string]interface{}{
			{"_score": map[string]string{"order": "desc"}},
			{"publish_time": map[string]string{"order": "desc"}},
		},
	}

	if tagID != nil {
		// 追加标签过滤条件
		query["query"].(map[string]interface{})["function_score"].(map[string]interface{})["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"] = append(
			query["query"].(map[string]interface{})["function_score"].(map[string]interface{})["query"].(map[string]interface{})["bool"].(map[string]interface{})["filter"].([]map[string]interface{}),
			map[string]interface{}{"term": map[string]interface{}{"tag_id": *tagID}},
		)
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
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source SearchItem `json:"_source"`
				Score  float64    `json:"_score"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	items := make([]SearchItem, len(result.Hits.Hits))
	for i, hit := range result.Hits.Hits {
		hit.Source.Score = hit.Score
		items[i] = hit.Source
	}

	return &SearchResponse{
		Items:    items,
		Total:    result.Hits.Total.Value,
		Page:     page,
		PageSize: size,
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
