package search

import "strings"

// buildSuggestField 构建 ES completion suggester 字段，包含标题和标签。
func buildSuggestField(title *string, tags *string) *SuggestField {
	inputs := make([]string, 0, 1)
	if text := strings.TrimSpace(strValue(title)); text != "" {
		inputs = append(inputs, text)
	}
	for _, tag := range parseJSONTags(tags) {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			inputs = append(inputs, tag)
		}
	}
	if len(inputs) == 0 {
		return nil
	}
	return &SuggestField{Input: inputs}
}

// strValue 安全地解引用 *string，nil 指针返回空字符串。
func strValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
