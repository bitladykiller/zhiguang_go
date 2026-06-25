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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// esFilter 构建 ES bool 查询的 filter 子句。
func esFilter(tags []string) []map[string]interface{} {
	filter := []map[string]interface{}{
		{"term": map[string]interface{}{"status": "published"}},
		{"term": map[string]interface{}{"visible": "public"}},
	}
	if len(tags) > 0 {
		filter = append(filter, map[string]interface{}{"terms": map[string]interface{}{"tags": tags}})
	}
	return filter
}

// esFunctionScore 构建 ES function_score 查询的 functions 子句。
func esFunctionScore() []map[string]interface{} {
	return []map[string]interface{}{
		{"field_value_factor": map[string]interface{}{"field": "like_count", "modifier": "log1p"}, "weight": 2.0},
		{"field_value_factor": map[string]interface{}{"field": "view_count", "modifier": "log1p"}, "weight": 1.0},
	}
}

// esSort 构建 ES 搜索的排序子句。
func esSort() []map[string]interface{} {
	return []map[string]interface{}{
		{"_score": map[string]string{"order": "desc"}},
		{"publish_time": map[string]string{"order": "desc"}},
		{"like_count": map[string]string{"order": "desc"}},
		{"view_count": map[string]string{"order": "desc"}},
		{"id": map[string]string{"order": "desc"}},
	}
}

// esHighlight 构建 ES 高亮子句。
func esHighlight() map[string]interface{} {
	return map[string]interface{}{
		"fields": map[string]interface{}{
			"title": map[string]interface{}{},
			"body":  map[string]interface{}{},
		},
	}
}

// buildSearchQuery 构造 ES 搜索请求体 JSON。
func (s *SearchService) buildSearchQuery(keyword string, tags []string, afterValues []interface{}, size int) map[string]interface{} {
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
						"filter": esFilter(tags),
					},
				},
				"functions":  esFunctionScore(),
				"boost_mode": "sum",
			},
		},
		"size":      size,
		"highlight": esHighlight(),
		"sort":      esSort(),
	}

	if len(afterValues) > 0 {
		query["search_after"] = afterValues
	}
	return query
}

// parseCSV 将逗号分隔的标签字符串解析为标签切片。
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

// parseAfter 解析 base64 编码的 search_after 游标。
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

// boolPtr 返回 bool 值的指针。用于 JSON 序列化「未设置」和「false」的区分。
func boolPtr(value bool) *bool {
	return &value
}
