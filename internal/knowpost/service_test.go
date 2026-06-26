package knowpost

import (
	"context"
	"errors"
	"testing"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/errcode"
)

// --- helper ---
func ptr[T any](v T) *T { return &v }

// ============================================================================
// 纯函数 & 工具函数测试
// ============================================================================

func TestIsValidVisible(t *testing.T) {
	valid := []KnowPostVisibility{KnowPostVisibilityPublic, KnowPostVisibilityFollowers, KnowPostVisibilitySchool, KnowPostVisibilityPrivate, KnowPostVisibilityUnlisted}
	for _, v := range valid {
		if !isValidVisible(v) {
			t.Errorf("isValidVisible(%q) = false, want true", v)
		}
	}
	invalid := []KnowPostVisibility{"", "unknown", "PUBLIC", "PUB"}
	for _, v := range invalid {
		if isValidVisible(v) {
			t.Errorf("isValidVisible(%q) = true, want false", v)
		}
	}
}

func TestStrVal(t *testing.T) {
	s := "hello"
	if got := strVal(&s); got != "hello" {
		t.Errorf("strVal(&%q) = %q, want %q", s, got, s)
	}
	if got := strVal(nil); got != "" {
		t.Errorf("strVal(nil) = %q, want \"\"", got)
	}
}

func TestToJSON(t *testing.T) {
	if got := toJSON([]string{"a", "b"}); got != `["a","b"]` {
		t.Errorf("toJSON([a b]) = %q, want %q", got, `["a","b"]`)
	}
	if got := toJSON(nil); got != "null" {
		t.Errorf("toJSON(nil) = %q, want %q", got, "null")
	}
}

func TestKnowPostOutboxOp(t *testing.T) {
	if got := knowPostOutboxOp(outboxTypeKnowPostDeleted); got != "delete" {
		t.Errorf("op(deleted) = %q, want %q", got, "delete")
	}
	if got := knowPostOutboxOp(outboxTypeKnowPostPublished); got != "upsert" {
		t.Errorf("op(published) = %q, want %q", got, "upsert")
	}
	if got := knowPostOutboxOp(outboxTypeKnowPostMetadataUpdated); got != "upsert" {
		t.Errorf("op(metadata) = %q, want %q", got, "upsert")
	}
	if got := knowPostOutboxOp(outboxTypeKnowPostVisibilityUpdated); got != "upsert" {
		t.Errorf("op(visibility) = %q, want %q", got, "upsert")
	}
	if got := knowPostOutboxOp(outboxTypeKnowPostTopUpdated); got != "upsert" {
		t.Errorf("op(top) = %q, want %q", got, "upsert")
	}
	if got := knowPostOutboxOp("UnknownEvent"); got != "upsert" {
		t.Errorf("op(unknown) = %q, want %q", got, "upsert")
	}
}

func TestPublicURL(t *testing.T) {
	cfg := &config.OssConfig{PublicDomain: "https://cdn.example.com", Folder: "knowpost"}
	svc := &KnowPostService{ossCfg: cfg}
	got := svc.publicURL("kp/abc/def.jpg")
	want := "https://cdn.example.com/kp/abc/def.jpg"
	if got != want {
		t.Errorf("publicURL() = %q, want %q", got, want)
	}
}

func TestPublicURL_TrailingSlash(t *testing.T) {
	cfg := &config.OssConfig{PublicDomain: "https://cdn.example.com/", Folder: "kp"}
	svc := &KnowPostService{ossCfg: cfg}
	got := svc.publicURL("abc.jpg")
	want := "https://cdn.example.com/abc.jpg"
	if got != want {
		t.Errorf("publicURL() = %q, want %q", got, want)
	}
}

func TestPublicURL_NoDomain(t *testing.T) {
	cfg := &config.OssConfig{Bucket: "my-bucket", Endpoint: "oss-cn-hangzhou.aliyuncs.com"}
	svc := &KnowPostService{ossCfg: cfg}
	got := svc.publicURL("a/b.jpg")
	want := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/a/b.jpg"
	if got != want {
		t.Errorf("publicURL() = %q, want %q", got, want)
	}
}

func TestParseDetail_Valid(t *testing.T) {
	svc := &KnowPostService{}
	data := []byte(`{"id":"1","title":"test","author_id":"42","author_nickname":"nick"}`)
	resp, err := svc.parseDetail(data)
	if err != nil {
		t.Fatalf("parseDetail() error = %v", err)
	}
	if resp.ID != "1" {
		t.Errorf("ID = %q, want %q", resp.ID, "1")
	}
}

func TestParseDetail_InvalidJSON(t *testing.T) {
	svc := &KnowPostService{}
	_, err := svc.parseDetail([]byte(`{invalid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseDetail_EmptySlice(t *testing.T) {
	svc := &KnowPostService{}
	_, err := svc.parseDetail([]byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

// ============================================================================
// EnrichDetail 测试
// ============================================================================

type stubCounter struct{ counts map[string]int32; liked bool; faved bool }

func (s *stubCounter) GetCounts(_ context.Context, _, _ string, _ []string) (map[string]int32, error) {
	if s.counts == nil {
		return map[string]int32{"like": 0, "fav": 0}, nil
	}
	return s.counts, nil
}
func (s *stubCounter) GetCountsBatch(_ context.Context, _ string, _, _ []string) (map[string]map[string]int32, error) {
	return nil, nil
}
func (s *stubCounter) IsLiked(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return s.liked, nil
}
func (s *stubCounter) IsFaved(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return s.faved, nil
}
func (s *stubCounter) BatchIsLiked(_ context.Context, _ uint64, _ string, hits []string) (map[string]bool, error) {
	m := make(map[string]bool, len(hits))
	for _, id := range hits {
		m[id] = s.liked
	}
	return m, nil
}
func (s *stubCounter) BatchIsFaved(_ context.Context, _ uint64, _ string, hits []string) (map[string]bool, error) {
	m := make(map[string]bool, len(hits))
	for _, id := range hits {
		m[id] = s.faved
	}
	return m, nil
}

func TestEnrichDetail_NilCounter(t *testing.T) {
	svc := &KnowPostService{}
	base := &KnowPostDetailResponse{ID: "1"}
	result := svc.enrichDetail(context.Background(), base, ptr(uint64(1)), true)
	if result != base {
		t.Error("should return same pointer when counter is nil")
	}
}

func TestEnrichDetail_LoggedIn(t *testing.T) {
	counter := &stubCounter{counts: map[string]int32{"like": 5, "fav": 3}, liked: true, faved: false}
	svc := &KnowPostService{counter: counter}
	base := &KnowPostDetailResponse{ID: "1"}

	result := svc.enrichDetail(context.Background(), base, ptr(uint64(1)), true)
	if result.LikeCount != 5 {
		t.Errorf("LikeCount = %d, want 5", result.LikeCount)
	}
	if result.FavoriteCount != 3 {
		t.Errorf("FavoriteCount = %d, want 3", result.FavoriteCount)
	}
	if result.Liked == nil || !*result.Liked {
		t.Error("Liked should be true")
	}
	if result.Faved == nil || *result.Faved {
		t.Error("Faved should be false")
	}
}

func TestEnrichDetail_Anonymous(t *testing.T) {
	counter := &stubCounter{counts: map[string]int32{"like": 5, "fav": 3}}
	svc := &KnowPostService{counter: counter}
	base := &KnowPostDetailResponse{ID: "1"}

	result := svc.enrichDetail(context.Background(), base, nil, false)
	if result.Liked != nil {
		t.Error("Liked should be nil for anonymous user")
	}
	if result.Faved != nil {
		t.Error("Faved should be nil for anonymous user")
	}
}

// ============================================================================
// 业务错误分类测试（无 DB 依赖）
// ============================================================================

func TestToAppErr_AppError(t *testing.T) {
	appErr := errcode.ErrNotFound.WithMsg("test")
	result := toAppErr(appErr)
	if result != appErr {
		t.Error("toAppErr should return the same AppError instance")
	}
}

func TestToAppErr_PlainError(t *testing.T) {
	result := toAppErr(errors.New("some db error"))
	if result.Code != errcode.CodeInternalError {
		t.Errorf("code = %d, want %d", result.Code, errcode.CodeInternalError)
	}
	if result.Message != "some db error" {
		t.Errorf("message = %q, want %q", result.Message, "some db error")
	}
}

// ============================================================================
// DTO 零值/边界测试
// ============================================================================

func TestFeedItemResponse_ZeroValues(t *testing.T) {
	resp := FeedItemResponse{}
	if resp.Liked != nil || resp.Faved != nil || resp.IsTop != nil {
		t.Error("optional fields should be nil by default")
	}
	if resp.ID != "" {
		t.Error("ID should be empty by default")
	}
}

func TestKnowPostDetailResponse_ZeroValues(t *testing.T) {
	resp := KnowPostDetailResponse{}
	if resp.Liked != nil || resp.Faved != nil {
		t.Error("user-state fields should be nil by default")
	}
}

func TestKnowPostModel_ZeroValues(t *testing.T) {
	p := KnowPost{}
	if p.Status != "" {
		t.Error("Status should be empty by default")
	}
	if p.Visible != "" {
		t.Error("Visible should be empty by default")
	}
	if p.IsTop {
		t.Error("IsTop should be false by default")
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

func BenchmarkStrVal(b *testing.B) {
	s := "hello world"
	for i := 0; i < b.N; i++ {
		_ = strVal(&s)
	}
}

func BenchmarkToJSON(b *testing.B) {
	tags := []string{"go", "redis", "mysql", "elasticsearch", "kafka"}
	for i := 0; i < b.N; i++ {
		_ = toJSON(tags)
	}
}

func BenchmarkParseDetail(b *testing.B) {
	svc := &KnowPostService{}
	data := []byte(`{"id":"1","title":"test","author_id":"42","author_nickname":"nick"}`)
	for i := 0; i < b.N; i++ {
		_, _ = svc.parseDetail(data)
	}
}
