package search

import "context"

// Search 执行全文检索，使用 function_score 融合 BM25 和计数权重，并通过 search_after 分页。
func (s *SearchService) Search(
	ctx context.Context,
	keyword string,
	size int,
	tagsCSV, after string,
	currentUserID *uint64,
) (*SearchResponse, error) {
	if size <= 0 {
		size = 20
	}

	var result searchResultPayload
	if err := s.executeSearchRequest(
		ctx,
		buildSearchQuery(keyword, size, parseCSV(tagsCSV), parseAfter(after)),
		&result,
		"search",
	); err != nil {
		return nil, err
	}

	return s.buildSearchResponse(ctx, result.Hits.Hits, size, currentUserID), nil
}

func buildSearchQuery(keyword string, size int, tags []string, afterValues []interface{}) map[string]interface{} {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"function_score": map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{
								"multi_match": map[string]interface{}{
									"query":  keyword,
									"fields": []string{"title^3", "body"},
								},
							},
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
		boolQuery := query["query"].(map[string]interface{})["function_score"].(map[string]interface{})["query"].(map[string]interface{})["bool"].(map[string]interface{})
		boolQuery["filter"] = append(
			boolQuery["filter"].([]map[string]interface{}),
			map[string]interface{}{"terms": map[string]interface{}{"tags": tags}},
		)
	}
	if len(afterValues) > 0 {
		query["search_after"] = afterValues
	}

	return query
}
