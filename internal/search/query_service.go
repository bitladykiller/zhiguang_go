package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/zhiguang/app/internal/knowpost"
)

const suggestQueryName = "title-suggest"

type searchResultPayload struct {
	Hits struct {
		Hits []searchHit `json:"hits"`
	} `json:"hits"`
}

type searchHit struct {
	Source    SearchIndexDoc      `json:"_source"`
	Sort      []interface{}       `json:"sort"`
	Highlight map[string][]string `json:"highlight"`
}

type suggestResultPayload struct {
	Suggest map[string][]suggestEntry `json:"suggest"`
}

type suggestEntry struct {
	Options []suggestOption `json:"options"`
}

type suggestOption struct {
	Text string `json:"text"`
}

// Search 执行全文检索，使用 function_score 融合 BM25 和计数权重，并通过 search_after 分页。
func (s *SearchService) Search(ctx context.Context, keyword string, size int, tagsCSV, after string, currentUserID *uint64) (*SearchResponse, error) {
	if size <= 0 {
		size = 20
	}

	query := buildSearchQuery(keyword, size, parseCSV(tagsCSV), parseAfter(after))

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

	var result searchResultPayload
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
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

func (s *SearchService) buildSearchResponse(ctx context.Context, hits []searchHit, size int, currentUserID *uint64) *SearchResponse {
	items := make([]knowpost.FeedItemResponse, 0, len(hits))
	for _, hit := range hits {
		items = append(items, s.buildFeedItemFromHit(ctx, hit, currentUserID))
	}

	var nextAfter *string
	if len(hits) > 0 {
		lastSort := hits[len(hits)-1].Sort
		if len(lastSort) > 0 {
			cursor := encodeAfter(lastSort)
			nextAfter = &cursor
		}
	}

	return &SearchResponse{
		Items:     items,
		NextAfter: nextAfter,
		HasMore:   len(items) >= size,
	}
}

func (s *SearchService) buildFeedItemFromHit(ctx context.Context, hit searchHit, currentUserID *uint64) knowpost.FeedItemResponse {
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
	return knowpost.FeedItemResponse{
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
	}
}

// Suggest 根据用户输入的前缀返回自动补全建议列表。
func (s *SearchService) Suggest(ctx context.Context, prefix string, size int) ([]string, error) {
	if size <= 0 {
		size = 10
	}

	query := map[string]interface{}{
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

	var result suggestResultPayload
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	options := result.Suggest[suggestQueryName]
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
