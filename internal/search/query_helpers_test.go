package search

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestParseCSV(t *testing.T) {
	t.Parallel()

	got := parseCSV(" go , , redis ,mysql ")
	want := []string{"go", "redis", "mysql"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected parsed csv, got %#v want %#v", got, want)
	}

	if got := parseCSV("   "); got != nil {
		t.Fatalf("expected blank csv to return nil, got %#v", got)
	}
}

func TestEncodeAfterAndParseAfterRoundTrip(t *testing.T) {
	t.Parallel()

	original := []interface{}{12.5, int64(1718000000), "post-42", json.Number("99")}
	encoded := encodeAfter(original)
	decoded := parseAfter(encoded)
	want := []interface{}{12.5, int64(1718000000), "post-42", int64(99)}

	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("unexpected decoded values, got %#v want %#v", decoded, want)
	}
}

func TestParseAfterInvalidCursor(t *testing.T) {
	t.Parallel()

	if got := parseAfter("%%%"); got != nil {
		t.Fatalf("expected invalid cursor to return nil, got %#v", got)
	}
}

func TestBuildSnippet(t *testing.T) {
	t.Parallel()

	got := buildSnippet(map[string][]string{
		"title": {"Go"},
		"body":  {"Redis"},
	})
	if got != "Go Redis" {
		t.Fatalf("unexpected snippet %q", got)
	}

	if got := buildSnippet(nil); got != "" {
		t.Fatalf("expected empty snippet, got %q", got)
	}
}
