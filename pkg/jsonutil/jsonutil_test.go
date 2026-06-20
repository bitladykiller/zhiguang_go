package jsonutil

import (
	"testing"
)

func TestParseStringArray_Nil(t *testing.T) {
	result := ParseStringArray(nil)
	if result == nil {
		t.Fatal("expected non-nil empty slice")
	}
	if len(result) != 0 {
		t.Fatalf("length = %d, want 0", len(result))
	}
}

func TestParseStringArray_Valid(t *testing.T) {
	raw := `["go","python","java"]`
	result := ParseStringArray(&raw)
	if len(result) != 3 {
		t.Fatalf("length = %d, want 3", len(result))
	}
	if result[0] != "go" || result[1] != "python" || result[2] != "java" {
		t.Errorf("result = %v, want [go python java]", result)
	}
}

func TestParseStringArray_SingleElement(t *testing.T) {
	raw := `["only"]`
	result := ParseStringArray(&raw)
	if len(result) != 1 || result[0] != "only" {
		t.Fatalf("result = %v, want [only]", result)
	}
}

func TestParseStringArray_EmptyArray(t *testing.T) {
	raw := `[]`
	result := ParseStringArray(&raw)
	if result == nil || len(result) != 0 {
		t.Fatal("expected empty slice")
	}
}

func TestParseStringArray_InvalidJSON(t *testing.T) {
	raw := `[invalid`
	result := ParseStringArray(&raw)
	if result == nil || len(result) != 0 {
		t.Fatal("expected empty slice on invalid JSON")
	}
}

func TestParseStringArray_NotAnArray(t *testing.T) {
	raw := `"just a string"`
	result := ParseStringArray(&raw)
	if result == nil || len(result) != 0 {
		t.Fatal("expected empty slice when JSON is not an array")
	}
}

func TestStrPtr_NonEmpty(t *testing.T) {
	s := "hello"
	p := StrPtr(s)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != "hello" {
		t.Errorf("*p = %q, want %q", *p, "hello")
	}
}

func TestStrPtr_Empty(t *testing.T) {
	p := StrPtr("")
	if p != nil {
		t.Fatal("expected nil for empty string")
	}
}

func TestStrPtr_SpecialChars(t *testing.T) {
	s := "a/b/c"
	p := StrPtr(s)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != s {
		t.Errorf("*p = %q, want %q", *p, s)
	}
}