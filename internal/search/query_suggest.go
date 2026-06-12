package search

import "context"

// Suggest 根据用户输入的前缀返回自动补全建议列表。
func (s *SearchService) Suggest(ctx context.Context, prefix string, size int) ([]string, error) {
	if size <= 0 {
		size = 10
	}

	var result suggestResultPayload
	if err := s.executeSearchRequest(ctx, buildSuggestQuery(prefix, size), &result, "suggest"); err != nil {
		return nil, err
	}

	return collectSuggestions(result.Suggest[suggestQueryName], size), nil
}

func buildSuggestQuery(prefix string, size int) map[string]interface{} {
	return map[string]interface{}{
		"suggest": map[string]interface{}{
			suggestQueryName: map[string]interface{}{
				"prefix": prefix,
				"completion": map[string]interface{}{
					"field": "suggest",
					"size":  size,
				},
			},
		},
	}
}

func collectSuggestions(entries []suggestEntry, size int) []string {
	seen := make(map[string]struct{}, size)
	suggestions := make([]string, 0, size)
	for _, entry := range entries {
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
				return suggestions
			}
		}
	}

	return suggestions
}
