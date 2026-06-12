package search

import (
	"context"
	"reflect"
	"testing"
)

func TestBuildSearchQuery(t *testing.T) {
	t.Parallel()

	query := buildSearchQuery("golang", 15, []string{"redis", "mysql"}, []interface{}{9.5, int64(10), "post-1"})

	if got := query["size"]; got != 15 {
		t.Fatalf("size = %v, want 15", got)
	}
	if _, ok := query["search_after"]; !ok {
		t.Fatal("expected search_after to be present")
	}

	boolQuery := query["query"].(map[string]interface{})["function_score"].(map[string]interface{})["query"].(map[string]interface{})["bool"].(map[string]interface{})
	filters := boolQuery["filter"].([]map[string]interface{})
	if len(filters) != 3 {
		t.Fatalf("len(filters) = %d, want 3", len(filters))
	}

	termsFilter := filters[2]["terms"].(map[string]interface{})
	tags := termsFilter["tags"].([]string)
	if !reflect.DeepEqual(tags, []string{"redis", "mysql"}) {
		t.Fatalf("tags filter = %#v", tags)
	}
}

func TestBuildSearchResponse(t *testing.T) {
	t.Parallel()

	service := &SearchService{}
	hits := []searchHit{
		{
			Source: SearchIndexDoc{
				ID:          "1",
				Title:       "Go",
				Description: "desc",
				Tags:        []string{"redis"},
				LikeCount:   3,
				FavCount:    2,
				ViewCount:   9,
			},
			Sort: []interface{}{9.5, int64(100), "1"},
		},
	}

	result := service.buildSearchResponse(context.Background(), hits, 1, nil)
	if len(result.Items) != 1 {
		t.Fatalf("len(result.Items) = %d, want 1", len(result.Items))
	}
	if !result.HasMore {
		t.Fatal("HasMore should be true when len(items) >= size")
	}
	if result.NextAfter == nil || *result.NextAfter == "" {
		t.Fatal("NextAfter should be populated")
	}
}

func TestCollectSuggestions(t *testing.T) {
	t.Parallel()

	entries := []suggestEntry{
		{
			Options: []suggestOption{
				{Text: "go"},
				{Text: "redis"},
				{Text: "go"},
				{Text: ""},
			},
		},
		{
			Options: []suggestOption{
				{Text: "mysql"},
				{Text: "redis"},
			},
		},
	}

	got := collectSuggestions(entries, 3)
	want := []string{"go", "redis", "mysql"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collectSuggestions() = %#v, want %#v", got, want)
	}
}
