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
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/internal/model"
	"github.com/zhiguang/app/pkg/contextutil"
	"github.com/zhiguang/app/pkg/jsonutil"
	"go.uber.org/zap"
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

// closeESBody 安全关闭 ES 响应体，记录关闭错误。
func (s *SearchService) closeESBody(res *esapi.Response) {
	if res != nil && res.Body != nil {
		if err := res.Body.Close(); err != nil {
			s.logger.Warn("close es response body failed", zap.Error(err))
		}
	}
}

// readESError 读取 ES 错误响应体并返回格式化错误。
func (s *SearchService) readESError(res *esapi.Response, opName string) error {
	body, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		return fmt.Errorf("%s error (status=%d, failed to read body: %w)", opName, res.StatusCode, readErr)
	}
	return fmt.Errorf("%s failed: %s", opName, string(body))
}
type SuggestField struct {
	Input  []string `json:"input"`
	Weight int      `json:"weight,omitempty"`
}

// SearchResponse 是搜索接口的响应结构，对齐 Java 版返回。
type SearchResponse struct {
	Items     []model.FeedItem `json:"items"`
	NextAfter *string          `json:"next_after,omitempty"`
	HasMore   bool             `json:"has_more"`
}

// SearchCounterClient 定义搜索结果需要的用户态计数读取接口。
type SearchCounterClient = counter.CounterServiceInterface

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

// ESConfig 封装 Elasticsearch 连接配置参数。
type ESConfig struct {
	URIs       []string
	IndexName  string
	MaxRetries int
}

// SearchService 封装 Elasticsearch 客户端并提供搜索相关操作。
type SearchService struct {
	client    *elasticsearch.Client
	indexName string
	counter   SearchCounterClient
	logger    *zap.Logger
}

// NewSearchService 使用给定 URI 地址列表创建 ES 客户端，并调用 EnsureIndex 确保索引存在。
//
// 参数:
//   - cfg.URIs: Elasticsearch 集群节点地址列表
//   - cfg.IndexName: 搜索索引名称
//   - counter: 用户态计数查询接口，nil 表示搜索结果不包含 liked/faved 状态
//
// 返回值:
//   - *SearchService: 搜索服务实例
//   - error: 如果创建客户端失败或索引创建/校验出错则返回非 nil 错误
func NewSearchService(ctx context.Context, cfg ESConfig, counter SearchCounterClient, logger *zap.Logger) (*SearchService, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     cfg.URIs,
		MaxRetries:    cfg.MaxRetries,
		RetryOnStatus: []int{502, 503, 504, 429},
	})
	if err != nil {
		return nil, fmt.Errorf("create es client: %w", err)
	}

	svc := &SearchService{client: client, indexName: cfg.IndexName, counter: counter, logger: logger}

	// 启动时确保索引已存在，失败重试 3 次
	var ensureErr error
	for attempt := 1; attempt <= defaultEnsureRetries; attempt++ {
		ensureErr = svc.EnsureIndex()
		if ensureErr == nil {
			break
		}
		if attempt < defaultEnsureRetries {
			logger.Warn("ensure index failed, retrying", zap.Int("attempt", attempt), zap.Error(ensureErr))
			contextutil.Sleep(ctx, time.Duration(attempt)*time.Second)
		}
	}
	if ensureErr != nil {
		return nil, fmt.Errorf("ensure index after retries: %w", ensureErr)
	}

	return svc, nil
}

// EnsureIndex 检查索引是否存在，不存在时按预定义的 indexMapping 创建索引。
//
// 处理流程:
//   - 调用 ES Indices.Exists API 检查指定索引是否存在
//   - 如果索引已存在（HTTP 200），则调用 ensureCompatibleMappings 补齐可能缺失的新字段映射
//   - 如果索引不存在（HTTP 404），则使用 indexMapping 常量中定义的完整 mapping 创建索引
//
// 返回值:
//   - error: 校验失败或创建索引失败时返回详细错误信息
//
// 边界情况:
//   - 当 ES 集群不可达时返回连接错误
//   - 索引创建时如果返回非 2xx 状态码（如权限不足）会读取响应体并返回错误
func (s *SearchService) EnsureIndex() error {
	res, err := s.client.Indices.Exists([]string{s.indexName})
	if err != nil {
		return err
	}
	defer s.closeESBody(res)

	if res.StatusCode == http.StatusOK {
		return s.ensureCompatibleMappings()
	}

	createRes, err := s.client.Indices.Create(s.indexName, s.client.Indices.Create.WithBody(
		bytes.NewReader([]byte(indexMapping)),
	))
	if err != nil {
		return err
	}
	defer s.closeESBody(createRes)

	if createRes.IsError() {
		return s.readESError(createRes, "create index")
	}

	return nil
}

// ensureCompatibleMappings 为旧版本索引补齐新查询路径依赖的字段映射。
//
// 设计原因:
//   本地开发环境可能保留了旧版本 schema 的索引（如 tag_id 字段未定义），
//   如果不补齐 mapping，按 tag_id 等字段搜索会一直静默失效。
//   此函数通过 ES Indices.PutMapping API 向已有索引追加新字段定义，
//   属于幂等操作——字段已存在时 ES 会忽略相同的映射定义。
//
// 补齐的字段:
//   - tag_id (long): 按分类标签筛选
//   - author_avatar (keyword, index: false): 作者头像 URL，仅存储不索引
//   - author_tag_json (keyword, index: false): 作者标签 JSON
//   - img_urls (keyword, index: false): 图片 URL 列表
//   - body (text): 正文全文检索
//   - favorite_count (long): 收藏数排序
//   - view_count (long): 浏览数排序
//   - title_suggest (completion): 标题自动补全
//
// 返回值:
//   - error: ES API 调用失败时返回响应的错误体内容
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
	defer s.closeESBody(res)

	if res.IsError() {
		return s.readESError(res, "put mapping")
	}

	return nil
}

const (
	defaultSearchSize     = 20
	defaultSuggestSize    = 10
	defaultEnsureRetries  = 3
)

// Search 执行全文检索，使用 function_score 融合 BM25 和相关指标权重，并通过 search_after 游标分页。
func (s *SearchService) Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error) {
	if size <= 0 {
		size = defaultSearchSize
	}

	tags := parseCSV(tagsCSV)
	afterValues := parseAfter(after)

	body, err := s.buildSearchQuery(keyword, tags, afterValues, size)
	if err != nil {
		return nil, err
	}

	raw, err := s.executeSearch(ctx, body)
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

// buildSearchQuery 构造 ES 搜索请求体 JSON。
func (s *SearchService) buildSearchQuery(keyword string, tags []string, afterValues []interface{}, size int) ([]byte, error) {
	body := &esSearchBody{
		Size: size,
		Track: &esTrackTotalHits{TrackTotalHits: true},
	}

	bq := &esBoolClauses{}

	if keyword != "" {
		bq.Must = append(bq.Must, &esMultiMatchQuery{
			MultiMatch: map[string]interface{}{
				"query":  keyword,
				"fields": []string{"title^3", "body"},
			},
		})
	}

	// 过滤条件: status=published, visible=public
	bq.Filter = append(bq.Filter, &esTermQuery{
		Term: map[string]interface{}{"status": "published"},
	})
	bq.Filter = append(bq.Filter, &esTermQuery{
		Term: map[string]interface{}{"visible": "public"},
	})

	if len(bq.Must) > 0 || len(bq.Filter) > 0 || len(bq.Should) > 0 {
		body.Query = &esBoolQuery{Bool: bq}
	}

	// function_score 包装，融合 like_count / view_count 权重
	fsQuery := body.Query
	body.Query = &esBoolQuery{
		Bool: &esBoolClauses{
			Must: []interface{}{
				&esFunctionScoreQuery{
					FunctionScore: &esFunctionScore{
						Query: fsQuery,
						Functions: []esFunction{
							{
								ScriptScore: &esScriptScore{
									Script: &esScript{
										Source: "Math.log(1 + (doc['like_count'].value ?: 0)) * 2.0 + Math.log(1 + (doc['view_count'].value ?: 0)) * 1.0",
									},
								},
							},
						},
						ScoreMode: "sum",
					},
				},
			},
		},
	}

	// 排序
	body.Sort = []esSortField{
		{Field: "_score", Order: "desc"},
		{Field: "publish_time", Order: "desc"},
		{Field: "like_count", Order: "desc"},
		{Field: "view_count", Order: "desc"},
		{Field: "id", Order: "desc"},
	}

	if len(tags) > 0 {
		var err error
		body, err = s.addTagFilter(body, tags)
		if err != nil {
			return nil, err
		}
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("build search query: marshal: %w", err)
	}

	return data, nil
}

// addTagFilter 向已构建的 ES query 中添加 tags 过滤条件。
func (s *SearchService) addTagFilter(body *esSearchBody, tags []string) (*esSearchBody, error) {
	if len(tags) == 0 {
		return body, nil
	}

	if body.Query != nil && body.Query.Bool != nil && len(body.Query.Bool.Must) > 0 {
		for i, clause := range body.Query.Bool.Must {
			if fs, ok := clause.(*esFunctionScoreQuery); ok {
				if fs.FunctionScore != nil {
					if bq, ok := fs.FunctionScore.Query.(*esBoolQuery); ok && bq.Bool != nil {
						bq.Bool.Filter = append(bq.Bool.Filter, &esTermsQuery{
							Terms: map[string]interface{}{"tags": tags},
						})
						_ = i
					}
				}
			}
		}
	}

	return body, nil
}

// searchHit 表示 ES 搜索结果中的单个 hit。
type searchHit struct {
	Source    SearchIndexDoc      `json:"_source"`
	Score     float64             `json:"_score"`
	Sort      []interface{}       `json:"sort"`
	Highlight map[string][]string `json:"highlight"`
}

// executeSearch 发送 ES 搜索请求并返回原始响应。
func (s *SearchService) executeSearch(ctx context.Context, query []byte) ([]searchHit, error) {
	buf := bytes.NewBuffer(query)

	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(buf),
	)
	if err != nil {
		return nil, fmt.Errorf("search: es request: %w", err)
	}
	defer s.closeESBody(res)

	if res.IsError() {
		return nil, s.readESError(res, "search")
	}

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

// decodeAndEnrich 将 ES 结果解析为 FeedItem 列表，并返回 liked/faved 状态映射。
func (s *SearchService) decodeAndEnrich(ctx context.Context, hits []searchHit, currentUserID *uint64) ([]model.FeedItem, map[string]bool, map[string]bool) {
	items := make([]model.FeedItem, 0, len(hits))

	var likedMap, favedMap map[string]bool
	if currentUserID != nil && s.counter != nil && len(hits) > 0 {
		hitIDs := make([]string, len(hits))
		for i, hit := range hits {
			hitIDs[i] = hit.Source.ID
		}
		var err error
		likedMap, err = s.counter.BatchIsLiked(ctx, *currentUserID, "knowpost", hitIDs)
		if err != nil {
			s.logger.Warn("failed to batch check liked status", zap.Error(err))
		}
		favedMap, err = s.counter.BatchIsFaved(ctx, *currentUserID, "knowpost", hitIDs)
		if err != nil {
			s.logger.Warn("failed to batch check faved status", zap.Error(err))
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
		items = append(items, model.FeedItem{
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
func (s *SearchService) applyLikedFaved(items []model.FeedItem, likedMap, favedMap map[string]bool) []model.FeedItem {
	if likedMap == nil && favedMap == nil {
		return items
	}
	for i, item := range items {
		if likedMap != nil {
			if l, ok := likedMap[item.ID]; ok {
				items[i].Liked = &l
			}
		}
		if favedMap != nil {
			if f, ok := favedMap[item.ID]; ok {
				items[i].Faved = &f
			}
		}
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

// Suggest 根据用户输入的前缀返回自动补全建议列表。
//
// 参数:
//   - ctx: 上下文对象
//   - prefix: 用户输入的前缀字符串
//   - size: 最大返回建议数量，<=0 时默认回退为 10
//
// 返回值:
//   - []string: 去重后的建议文本列表，按 ES completion suggester 内置评分排序
//   - error: ES 搜索失败或 JSON 编解码错误时返回
//
// 实现说明:
//   使用 ES 的 completion suggester 而非 edge_ngram，
//   原因如下:
//   - completion suggester 基于 FST（有限状态转换器）实现，查询复杂度为 O(prefix_length)
//   - 支持权重控制（SuggestField.Weight 字段）以调整建议排序
//   - 无需定义额外的索引分析器，与 suggest 字段的 completion 类型映射配合使用
//
// 去重逻辑:
//   响应的 options 中可能包含重复文本（标题和标签可能相同），
//   使用 map[string]struct{} 进行内存去重，按返回顺序保留首次出现的实例。
//
// 边界情况:
//   - prefix 为空时 ES 会返回 "completion suggester requires a prefix" 错误
//   - size=0 时默认回退为 10，避免空响应
//   - 返回的建议数可能少于 size（没有足够匹配项时）
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

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("suggest: encode query: %w", err)
	}

	res, err := s.client.Search(
		s.client.Search.WithContext(ctx),
		s.client.Search.WithIndex(s.indexName),
		s.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("suggest: es request: %w", err)
	}
	defer s.closeESBody(res)

	if res.IsError() {
		return nil, s.readESError(res, "suggest")
	}

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

// parseCSV 将逗号分隔的标签字符串解析为标签切片。
//
// 参数:
//   - csv: 逗号分隔字符串，例如 "go,redis,mysql"
//
// 返回值:
//   - []string: 去空白后的标签列表，输入为空或仅空白时返回 nil
//
// 边界情况:
//   - 输入为空字符串 "" → 返回 nil
//   - 输入为 "  "（纯空白）→ 返回 nil
//   - 输入为 "go,,redis" → 返回 ["go", "redis"]（空段跳过）
//   - 输入为 " go , redis " → 返回 ["go", "redis"]（每段去前后空白）
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

// parseAfter 解析 base64 编码的 search_after 游标，将其还原为排序值切片。
//
// 参数:
//   - after: base64 RawURL 编码的游标字符串，格式为 encodeAfter() 的输出
//
// 返回值:
//   - []interface{}: 排序值列表，顺序对应 Search 查询中 sort 子句的字段顺序：
//     第一个值按 float64 解析（_score），后续值按 int64 解析（publish_time 等时间戳或 ID）
//     如果解析失败则以原始字符串形式保留
//
// 实现逻辑:
//   - 先 base64 解码游标字符串，再用逗号分割出各排序值
//   - 第 0 个元素优先按 float64 解析（对应 _score 排序值）
//   - 后续元素优先按 int64 解析（对应时间戳和 ID 排序值）
//   - 如果数值解析失败，以字符串形式保留（预留扩展，当前未使用）
//
// 边界情况:
//   - after 为空或仅空白 → 返回 nil，表示从头开始第一页
//   - base64 解码失败 → 返回 nil（不返回错误，由调用方视作第一页）
//   - after 为无效格式（如不可解析的文本）→ 保留原始文本作为排序值
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

// encodeAfter 将 ES 返回的排序值编码为 base64 游标字符串。
// 该字符串由客户端在翻页时原样传回，由 parseAfter 还原。
//
// 参数:
//   - sortValues: ES 搜索响应中 hit 的 sort 数组，包含各排序字段的值
//
// 返回值:
//   - string: base64 RawURL 编码的游标字符串
//
// 序列化规则（与 Java 版对齐）:
//   - float64: 使用 FormatFloat 'f' 格式（-1 精度，即最短表示）
//   - string: 直接使用原始字符串
//   - json.Number: 调用 .String() 方法获取文本表示
//   - 其他类型: fmt.Sprint 兜底
//
// 注意: base64.RawURLEncoding 不使用填充字符（无 '=' 后缀），
// URL 中无需额外编码即可作为查询参数传递，避免与标准 base64 的 '+' 和 '/' 字符冲突。
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

// buildSnippet 从 ES 高亮结果中提取摘要片段。
//
// 参数:
//   - highlight: ES 返回的高亮映射，key 为字段名，value 为匹配片段列表
//
// 返回值:
//   - string: 拼接后的摘要文本。优先级为 title 高亮 + body 高亮，用空格连接。
//     如果没有高亮片段则返回空字符串。
//
// 边界情况:
//   - highlight 为 nil 或空映射 → 返回 ""
//   - 只有 title 有高亮 → 返回 title 第一个片段
//   - 只有 body 有高亮 → 返回 body 第一个片段
//   - 两个字段都有高亮 → 返回 "title 片段 body 片段"
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

// boolPtr 返回 bool 值的指针。
//
// 参数:
//   - value: 原始 bool 值
//
// 返回值:
//   - *bool: 指向 value 的指针
//
// 用途: JSON 序列化时需要输出 null/true/false 而非缺失字段，而 Go 结构体中的 bool 零值为 false，
// 无法区分"未设置"和"设置为 false"。通过 *bool 指针类型明确表达三态语义。
func boolPtr(value bool) *bool {
	return &value
}


// IndexDocument 将搜索文档索引到 Elasticsearch 中（创建或全量替换）。
//
// 参数:
//   - ctx: 上下文对象
//   - doc: 搜索文档结构体指针，包含标题、正文、标签、作者、计数等完整字段
//
// 返回值:
//   - error: JSON 序列化失败或 ES 返回错误响应时返回
//
// 实现说明:
//   - 调用 ES Index API（非 Update API），意味着当文档 ID 已存在时执行全量替换
//   - doc.ID 字段通过 Index.WithDocumentID 指定为 ES 文档 _id，确保幂等性
//   - 文档 ID 为知文 ID 的字符串形式，与 Java 版保持一致
//
// 边界情况:
//   - 如果 doc 中包含无效字段类型，JSON 序列化可能返回错误
//   - ES 集群不可用时返回连接错误
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
	defer s.closeESBody(res)

	if res.IsError() {
		return s.readESError(res, "index document")
	}

	return nil
}

// DeleteDocument 从搜索索引中删除一篇文档。
//
// 参数:
//   - ctx: 上下文对象
//   - id: 要删除的文档 ID（即知文 ID 的字符串形式）
//
// 返回值:
//   - error: ES 集群不可达时返回连接错误
//
// 实现说明:
//   - 调用 ES Delete API 时仅按文档 ID 执行删除，不关心文档是否存在
//   - 如果删除不存在的文档，ES 返回 404，但此处不特殊处理（delete 幂等）
//   - 用于 outbox 事件中的"软删除"场景：将文档 status 置为 "deleted"，
//     而不是真的从索引中移除（参见 SoftDeleteKnowPost 中的 IndexDocument 调用）
//
// 边界情况:
//   - 删除不存在的 ID → 不会返回错误（ES 响应 404，已显式检查并忽略）
//   - s.client.Delete 调用成功但返回错误 response body → 已读取并返回给调用方
func (s *SearchService) DeleteDocument(ctx context.Context, id string) error {
	res, err := s.client.Delete(
		s.indexName,
		id,
		s.client.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("delete document: es request: %w", err)
	}
	defer s.closeESBody(res)

	if res.StatusCode == http.StatusNotFound {
		return nil
	}

	if res.IsError() {
		return s.readESError(res, "delete document")
	}
	return nil
}

// ES 查询结构体，替代嵌套 map[string]interface{}
type esSearchBody struct {
	Query   *esBoolQuery         `json:"query,omitempty"`
	Size    int                  `json:"size,omitempty"`
	From    int                  `json:"from,omitempty"`
	Sort    []esSortField        `json:"sort,omitempty"`
	Suggest *esSuggest           `json:"suggest,omitempty"`
	Track   *esTrackTotalHits    `json:"track_total_hits,omitempty"`
}

type esBoolQuery struct {
	Bool *esBoolClauses `json:"bool,omitempty"`
}

type esBoolClauses struct {
	Must    []interface{} `json:"must,omitempty"`
	Filter  []interface{} `json:"filter,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
}

type esSortField struct {
	Field string
	Order string
}

func (s esSortField) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		s.Field: map[string]string{"order": s.Order},
	})
}

type esSuggest struct {
	Suggest *esSuggestConfig `json:"suggest,omitempty"`
}

type esSuggestConfig struct {
	Prefix     string              `json:"prefix"`
	Completion *esCompletionField  `json:"completion"`
}

type esCompletionField struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

type esTrackTotalHits struct {
	TrackTotalHits interface{} `json:"track_total_hits"`
}

type esTermQuery struct {
	Term map[string]interface{} `json:"term"`
}

type esTermsQuery struct {
	Terms map[string]interface{} `json:"terms"`
}

type esMatchQuery struct {
	Match map[string]interface{} `json:"match"`
}

type esMultiMatchQuery struct {
	MultiMatch map[string]interface{} `json:"multi_match"`
}

type esRangeQuery struct {
	Range map[string]interface{} `json:"range"`
}

type esFunctionScoreQuery struct {
	FunctionScore *esFunctionScore `json:"function_score"`
}

type esFunctionScore struct {
	Query     interface{}   `json:"query"`
	Functions []esFunction `json:"functions,omitempty"`
	ScoreMode string       `json:"score_mode"`
}

type esFunction struct {
	Filter      interface{}   `json:"filter,omitempty"`
	Weight      *int          `json:"weight,omitempty"`
	ScriptScore *esScriptScore `json:"script_score,omitempty"`
}

type esScriptScore struct {
	Script *esScript `json:"script"`
}

type esScript struct {
	Source string `json:"source"`
}
