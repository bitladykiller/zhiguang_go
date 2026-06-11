package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8"
)

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

// NewSearchService 使用给定配置创建 ES 客户端，并在启动期确保索引存在。
func NewSearchService(cfg ServiceConfig) (*SearchService, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.URIs,
	})
	if err != nil {
		return nil, fmt.Errorf("create es client: %w", err)
	}

	svc := &SearchService{
		client:    client,
		indexName: cfg.IndexName,
		counter:   cfg.Counter,
	}

	if err := svc.EnsureIndex(); err != nil {
		return nil, fmt.Errorf("ensure index: %w", err)
	}

	return svc, nil
}

// EnsureIndex 检查索引是否存在，不存在时按预定义 mapping 创建索引。
func (s *SearchService) EnsureIndex() error {
	res, err := s.client.Indices.Exists([]string{s.indexName})
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return s.ensureCompatibleMappings()
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

// ensureCompatibleMappings 为旧版本索引补齐当前查询链路依赖的字段映射。
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
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("put mapping failed: %s", string(body))
	}

	return nil
}

// IndexDocument 将搜索文档索引到 Elasticsearch 中。
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
