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

// NewSearchService 使用给定 URI 地址列表创建 ES 客户端，并调用 EnsureIndex 确保索引存在。
//
// 参数:
//   - cfg.URIs: Elasticsearch 集群节点地址列表，格式如 []string{"http://localhost:9200"}
//   - cfg.IndexName: 搜索索引名称，与 Java 版 zhiguang_be 使用相同的索引名以保证兼容性
//
// 返回值:
//   - *SearchService: 搜索服务实例，上层可通过该实例执行搜索、建议、文档索引等操作
//   - error: 如果创建客户端失败或索引创建/校验出错则返回非 nil 错误
//
// 注意:
//   - 构造函数在启动阶段会一次性完成客户端创建和索引初始化
//   - 如果 ES 集群尚未启动，此处会返回连接错误，调用方应处理降级逻辑
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
//
// 参数:
//   - counter: 实现 SearchCounterClient 接口的实例（通常为 counter.CounterService），
//     提供 IsLiked 和 IsFaved 方法用于判断当前用户对搜索结果的点赞/收藏状态
//
// 说明:
//   - 需要在 Search 接口被调用前注入，否则搜索结果中 liked/faved 字段将为 nil
//   - 使用接口而非具体类型是为了解耦，避免 search 包依赖 counter 包
func (s *SearchService) SetCounterClient(counter SearchCounterClient) {
	s.counter = counter
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
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("put mapping failed: %s", string(body))
	}

	return nil
}

// Search 执行全文检索，使用 function_score 融合 BM25 和相关指标权重，并通过 search_after 游标分页。
//
// 参数:
//   - ctx: 上下文对象，用于链路追踪和请求超时控制
//   - keyword: 搜索关键词，使用 multi_match 在 title(权重 3) 和 body 字段同时检索
//   - size: 每页返回结果数量，<=0 时默认回退为 20
//   - tagsCSV: 按逗号分隔的标签筛选条件（可选），非空时追加 terms 过滤器
//   - after: base64 编码的游标值，由上一页响应的 next_after 字段提供
//   - currentUserID: 当前登录用户 ID（可选），非空时查询用户对每篇结果的点赞/收藏状态
//
// 返回值:
//   - *SearchResponse: 包含搜索结果列表、下一页游标 next_after、是否还有更多 has_more
//   - error: ES 搜索失败或 JSON 编解码错误时返回
//
// 排序策略（与 Java 版对齐）:
//   1. _score: BM25 相关性评分（降序）
//   2. publish_time: 发布时间（降序）
//   3. like_count: 点赞数（降序）
//   4. view_count: 浏览数（降序）
//   5. id: 文档 ID（降序），确保排序的确定性
//
// function_score 权重:
//   - like_count 使用 log1p 修正器乘以权重 2.0
//   - view_count 使用 log1p 修正器乘以权重 1.0
//   - boost_mode 设为 "sum"，即 BM25 分值与函数分值相加
//
// 分页机制:
//   使用 search_after 代替传统的 from/size 深分页
//   原因：深分页场景下 ES 需要在每个分片维持全局排序状态，
//   导致集群内存消耗线性增长。search_after 利用上一页最后一个文档的排序值
//   作为起点，避免了该问题，适合搜索结果翻页场景。
//
// 高亮:
//   对 title 和 body 字段使用 ES 默认高亮配置（<em> 标签包裹匹配片段），
//   如果高亮片段存在，则用它替换原 description 返回给客户端。
//
// 边界情况:
//   - keyword 为空时 ES 会返回空结果而非错误
//   - after 解码失败时视作第一页，不返回错误
//   - 结果数为 0 时 has_more 为 false, next_after 为 nil
//   - currentUserID 为 nil 时 liked/faved 返回 nil
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

// stringPtrOrNil 将 Go 字符串转为 *string 指针。空字符串返回 nil。
//
// 参数:
//   - value: 原始字符串
//
// 返回值:
//   - *string: value 非空时返回指向 value 的指针，否则返回 nil
//
// 设计原因: JSON 响应中空字符串字段应序列化为 null 而非 ""（与 Java 版 Jackson 行为对齐）。
// if source.Title == "" 时使用 nil 指针可以在 JSON 中输出 null 或省略该字段。
func stringPtrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
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

// userFlags 查询当前用户对指定知文是否已点赞/已收藏。
//
// 参数:
//   - ctx: 上下文对象
//   - currentUserID: 当前用户 ID 的指针，nil 表示未登录
//   - entityID: 知文 ID 字符串
//
// 返回值:
//   - liked: *bool 类型，已点赞为 true，未点赞为 false；用户未登录或 counter 未注入时返回 nil
//   - faved: *bool 类型，语义同上
//
// 注意:
//   - 本函数内部忽略 counter 调用返回的错误，在计数值查询失败时静默返回 false
//   - 这是为了确保计数器服务的短暂故障不会影响搜索结果的主要展示
func (s *SearchService) userFlags(ctx context.Context, currentUserID *uint64, entityID string) (*bool, *bool) {
	if currentUserID == nil || s.counter == nil {
		return nil, nil
	}
	liked, _ := s.counter.IsLiked(ctx, *currentUserID, "knowpost", entityID)
	faved, _ := s.counter.IsFaved(ctx, *currentUserID, "knowpost", entityID)
	return &liked, &faved
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
//   - 删除不存在的 ID → 不会返回错误（ES 响应 404，函数中未检查）
//   - s.client.Delete 调用成功但返回错误 response body → 未读取，调用方无从得知
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
