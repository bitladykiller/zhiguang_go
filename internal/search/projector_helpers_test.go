package search

import "testing"

func TestParseJSONTags(t *testing.T) {
	tests := []struct {
		name string
		raw  *string
		want []string
	}{
		{name: "nil", raw: nil, want: []string{}},
		{name: "blank", raw: strPtr(" "), want: []string{}},
		{name: "invalid", raw: strPtr("{"), want: []string{}},
		{name: "valid", raw: strPtr(`["go"," redis "]`), want: []string{"go", " redis "}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseJSONTags(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("len(parseJSONTags()) = %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("parseJSONTags()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestBuildSuggestField(t *testing.T) {
	t.Run("nil when no inputs", func(t *testing.T) {
		if got := buildSuggestField(nil, nil); got != nil {
			t.Fatalf("buildSuggestField() = %#v, want nil", got)
		}
	})

	t.Run("includes title and trimmed tags", func(t *testing.T) {
		got := buildSuggestField(strPtr("  title  "), strPtr(`["go"," ","redis"]`))
		if got == nil {
			t.Fatal("buildSuggestField() = nil, want non-nil")
		}

		want := []string{"title", "go", "redis"}
		if len(got.Input) != len(want) {
			t.Fatalf("len(buildSuggestField().Input) = %d, want %d", len(got.Input), len(want))
		}
		for i := range want {
			if got.Input[i] != want[i] {
				t.Fatalf("buildSuggestField().Input[%d] = %q, want %q", i, got.Input[i], want[i])
			}
		}
	})
}

func strPtr(v string) *string {
	return &v
}
