package knowpost

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestClamp(t *testing.T) {
	if got := clamp(0, 1, 50); got != 1 {
		t.Fatalf("clamp lower = %d", got)
	}
	if got := clamp(99, 1, 50); got != 50 {
		t.Fatalf("clamp upper = %d", got)
	}
	if got := clamp(20, 1, 50); got != 20 {
		t.Fatalf("clamp middle = %d", got)
	}
}

func TestBoolToStr(t *testing.T) {
	if got := boolToStr(true); got != "1" {
		t.Fatalf("boolToStr(true) = %q", got)
	}
	if got := boolToStr(false); got != "0" {
		t.Fatalf("boolToStr(false) = %q", got)
	}
}

func TestParseFeedPage(t *testing.T) {
	service := &KnowPostFeedService{}
	title := "hello"
	expected := &FeedPageResponse{
		Items: []FeedItemResponse{
			{ID: "101", Title: &title},
		},
		Page:    2,
		Size:    10,
		HasMore: true,
	}

	data, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("marshal expected page: %v", err)
	}

	got, err := service.parseFeedPage(data)
	if err != nil {
		t.Fatalf("parseFeedPage() error = %v", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("parseFeedPage() = %#v, want %#v", got, expected)
	}
}
