package search

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

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

// parseAfter 解析 base64 编码的 search_after 游标，将其还原为排序值切片。
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

func stringPtrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

// userFlags 查询当前用户对指定知文是否已点赞/已收藏。
//
// 这里故意忽略 counter 查询错误，避免计数器短故障拖垮主搜索结果。
func (s *SearchService) userFlags(ctx context.Context, currentUserID *uint64, entityID string) (*bool, *bool) {
	if currentUserID == nil || s.counter == nil {
		return nil, nil
	}

	liked, _ := s.counter.IsLiked(ctx, *currentUserID, "knowpost", entityID)
	faved, _ := s.counter.IsFaved(ctx, *currentUserID, "knowpost", entityID)
	return &liked, &faved
}
