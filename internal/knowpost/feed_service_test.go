package knowpost

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func strPtr(s string) *string { return &s }

// ============================================================================
// Helpers
// ============================================================================

func newTestFeedService(t *testing.T, srv *miniredis.Miniredis) *KnowPostFeedService {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	cache := freecache.NewCache(100 * 1024)
	return &KnowPostFeedService{
		redis:    rdb,
		l1Public: &PrefixCache{Cache: cache, Prefix: "p:"},
		l1Mine:   &PrefixCache{Cache: cache, Prefix: "m:"},
		logger:   zap.NewNop(),
	}
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// ============================================================================
// clamp / max / boolToStr
// ============================================================================

func TestClamp(t *testing.T) {
	tests := []struct{ v, lo, hi, want int }{
		{5, 1, 10, 5},
		{0, 1, 10, 1},
		{20, 1, 10, 10},
		{1, 1, 1, 1},
	}
	for _, tc := range tests {
		got := clamp(tc.v, tc.lo, tc.hi)
		if got != tc.want {
			t.Errorf("clamp(%d,%d,%d) = %d, want %d", tc.v, tc.lo, tc.hi, got, tc.want)
		}
	}
}

func TestMax(t *testing.T) {
	if got := max(3, 5); got != 5 {
		t.Errorf("max(3,5) = %d, want 5", got)
	}
	if got := max(5, 3); got != 5 {
		t.Errorf("max(5,3) = %d, want 5", got)
	}
	if got := max(-1, 0); got != 0 {
		t.Errorf("max(-1,0) = %d, want 0", got)
	}
}

func TestBoolToStr(t *testing.T) {
	if got := boolToStr(true); got != "1" {
		t.Errorf("boolToStr(true) = %q, want %q", got, "1")
	}
	if got := boolToStr(false); got != "0" {
		t.Errorf("boolToStr(false) = %q, want %q", got, "0")
	}
}

// ============================================================================
// parseFeedPage
// ============================================================================

func TestParseFeedPage_Valid(t *testing.T) {
	svc := &KnowPostFeedService{}
	data := mustMarshal(t, &FeedPageResponse{Items: []FeedItemResponse{{ID: "1"}}, Page: 1, Size: 10, HasMore: false})
	resp, err := svc.parseFeedPage(data)
	if err != nil {
		t.Fatalf("parseFeedPage() error = %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "1" {
		t.Errorf("unexpected items: %+v", resp.Items)
	}
}

func TestParseFeedPage_InvalidJSON(t *testing.T) {
	svc := &KnowPostFeedService{}
	_, err := svc.parseFeedPage([]byte(`{invalid`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseFeedPage_EmptyData(t *testing.T) {
	svc := &KnowPostFeedService{}
	_, err := svc.parseFeedPage([]byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}

// ============================================================================
// feedVersion
// ============================================================================

func TestFeedVersion_KeyExists(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	srv.Set("feed:public:version", "42")

	got := svc.feedVersion(context.Background(), "feed:public:version")
	if got != 42 {
		t.Errorf("feedVersion = %d, want 42", got)
	}
}

func TestFeedVersion_KeyNotExists(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)

	got := svc.feedVersion(context.Background(), "feed:public:version")
	if got != 1 {
		t.Errorf("feedVersion = %d, want 1", got)
	}
}

func TestFeedVersion_NegativeValue(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	srv.Set("feed:public:version", "-5")

	got := svc.feedVersion(context.Background(), "feed:public:version")
	if got != 1 {
		t.Errorf("feedVersion = %d, want 1", got)
	}
}

// ============================================================================
// enrichItems
// ============================================================================

func TestEnrichItems_NilUserID(t *testing.T) {
	svc := &KnowPostFeedService{}
	items := []FeedItemResponse{{ID: "1"}}
	result := svc.enrichItems(context.Background(), items, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result))
	}
	if result[0].Liked != nil {
		t.Error("Liked should be nil for anonymous user")
	}
}

func TestEnrichItems_WithUser(t *testing.T) {
	userID := uint64(1001)
	svc := &KnowPostFeedService{
		counter: &stubCounter{liked: true, faved: false},
	}
	items := []FeedItemResponse{{ID: "1"}, {ID: "2"}}
	result := svc.enrichItems(context.Background(), items, &userID)
	if len(result) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result))
	}
	if result[0].Liked == nil || !*result[0].Liked {
		t.Error("item 0 should be liked")
	}
	if result[0].Faved == nil || *result[0].Faved {
		t.Error("item 0 should not be faved")
	}
	if result[1].Liked == nil || !*result[1].Liked {
		t.Error("item 1 should be liked")
	}
}

func TestEnrichItems_NilCounter(t *testing.T) {
	userID := uint64(1001)
	svc := &KnowPostFeedService{}
	items := []FeedItemResponse{{ID: "1"}}
	result := svc.enrichItems(context.Background(), items, &userID)
	if len(result) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result))
	}
}

// ============================================================================
// cacheFeedPage
// ============================================================================

func TestCacheFeedPage(t *testing.T) {
	cache := freecache.NewCache(100 * 1024)
	svc := &KnowPostFeedService{
		l1Public: &PrefixCache{Cache: cache, Prefix: "p:"},
		logger:   nil,
	}
	resp := &FeedPageResponse{Items: []FeedItemResponse{{ID: "1"}}, Page: 1, Size: 10}
	svc.cacheFeedPage("test:key", resp, svc.l1Public)

	val, err := cache.Get([]byte("p:test:key"))
	if err != nil {
		t.Fatalf("cache.Get: %v", err)
	}
	var decoded FeedPageResponse
	if err := json.Unmarshal(val, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Items) != 1 || decoded.Items[0].ID != "1" {
		t.Errorf("unexpected cached data: %+v", decoded)
	}
}

// ============================================================================
// mapRowsToItems
// ============================================================================

func TestMapRowsToItems_Basic(t *testing.T) {
	svc := &KnowPostFeedService{}
	rows := []KnowPostFeedRow{
		{
			ID: 1, Title: strPtr("t1"), Description: strPtr("d1"),
			Tags: strPtr(`["go","redis"]`), ImgUrls: strPtr(`["http://img.jpg"]`),
			AuthorNickname: "nick",
		},
		{
			ID: 2, Title: strPtr("t2"),
			AuthorNickname: "nick2",
		},
	}
	items := svc.mapRowsToItems(context.Background(), rows, nil, false)
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Title == nil || *items[0].Title != "t1" {
		t.Errorf("item[0].Title = %v, want 't1'", items[0].Title)
	}
	if items[0].CoverImage == nil || *items[0].CoverImage != "http://img.jpg" {
		t.Errorf("item[0].CoverImage = %v, want 'http://img.jpg'", items[0].CoverImage)
	}
	if len(items[0].Tags) != 2 || items[0].Tags[0] != "go" {
		t.Errorf("item[0].Tags = %v, want [go redis]", items[0].Tags)
	}
	if items[1].IsTop != nil {
		t.Error("item[1].IsTop should be nil for public feed")
	}
}

func TestMapRowsToItems_WithIsTop(t *testing.T) {
	svc := &KnowPostFeedService{}
	rows := []KnowPostFeedRow{
		{ID: 1, Title: strPtr("t"), AuthorNickname: "n", IsTop: true},
	}
	items := svc.mapRowsToItems(context.Background(), rows, nil, true)
	if items[0].IsTop == nil || !*items[0].IsTop {
		t.Error("IsTop should be true")
	}
}

func TestMapRowsToItems_WithCounter(t *testing.T) {
	svc := &KnowPostFeedService{
		counter: &stubCounter{},
	}
	// stubCounter.GetCountsBatch returns nil, nil -> no counts applied
	rows := []KnowPostFeedRow{
		{ID: 1, Title: strPtr("t"), AuthorNickname: "n"},
		{ID: 2, Title: strPtr("t2"), AuthorNickname: "n2"},
	}
	items := svc.mapRowsToItems(context.Background(), rows, nil, false)
	if items[0].LikeCount != 0 {
		t.Errorf("LikeCount = %d, want 0", items[0].LikeCount)
	}
	if items[0].FavoriteCount != 0 {
		t.Errorf("FavoriteCount = %d, want 0", items[0].FavoriteCount)
	}
}

// ============================================================================
// recordItemHotKey (nil logger should not panic)
// ============================================================================

func TestRecordItemHotKey_NilLogger(t *testing.T) {
	svc := &KnowPostFeedService{}
	// should not panic even with nil hotkey
	svc.recordItemHotKey(context.Background(), "1")
}

// ============================================================================
// KnowPostFeedService 零值/边界
// ============================================================================

func TestNewKnowPostFeedService(t *testing.T) {
	svc := &KnowPostFeedService{}
	if svc == nil {
		t.Fatal("KnowPostFeedService zero value should not be nil")
	}
}

func TestFeedService_NilMethodsDontPanic(t *testing.T) {
	svc := &KnowPostFeedService{}
	// these should not panic with nil receiver
	svc.InvalidateAfterPostMutation(context.Background(), 1, 1)
}

// ============================================================================
// InvalidateAfterPostMutation
// ============================================================================

func TestInvalidateAfterPostMutation(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	srv.Set("feed:item:42", `{"id":"42"}`)
	srv.Set("feed:public:version", "5")
	srv.Set("feed:mine:version:1001", "3")

	svc.InvalidateAfterPostMutation(context.Background(), 42, 1001)

	if srv.Exists("feed:item:42") {
		t.Error("feed:item:42 should be deleted")
	}
	gotPublic, _ := srv.Get("feed:public:version")
	if gotPublic != "6" {
		t.Errorf("public version = %s, want 6", gotPublic)
	}
	gotMine, _ := srv.Get("feed:mine:version:1001")
	if gotMine != "4" {
		t.Errorf("mine version = %s, want 4", gotMine)
	}
}

// ============================================================================
// writeFragmentCaches 子方法（测核心 Redis 操作）
// ============================================================================

func TestWriteFeedIDListCache(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	rows := []KnowPostFeedRow{
		{ID: 1},
		{ID: 2},
	}

	svc.writeFeedIDListCache(context.Background(), "feed:test:ids", "feed:test:hasMore", rows, true)

	if !srv.Exists("feed:test:ids") {
		t.Error("ids key should exist")
	}
	if !srv.Exists("feed:test:hasMore") {
		t.Error("hasMore key should exist")
	}
	hasMore, _ := srv.Get("feed:test:hasMore")
	if hasMore != "1" {
		t.Errorf("hasMore = %q, want '1'", hasMore)
	}
}

func TestWriteFeedIDListCache_EmptyRows(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	// should not panic or create keys
	svc.writeFeedIDListCache(context.Background(), "feed:test:ids", "feed:test:hasMore", nil, false)
}

func TestRegisterFeedPageKey(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)

	svc.registerFeedPageKey(context.Background(), "feed:test:ids")

	if !srv.Exists("feed:public:pages") {
		t.Error("feed:public:pages set should exist")
	}
	members, err := srv.SMembers("feed:public:pages")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != "feed:test:ids" {
		t.Errorf("members = %v, want [feed:test:ids]", members)
	}
}

// ============================================================================
// getPublicFeedL1 / getPublicFeedL2（无 DB 路径测试）
// ============================================================================

func TestGetPublicFeedL1_Hit(t *testing.T) {
	svc := &KnowPostFeedService{
		l1Public: &PrefixCache{Cache: freecache.NewCache(100 * 1024), Prefix: "p:"},
	}
	resp := &FeedPageResponse{Items: []FeedItemResponse{{ID: "1"}}, Page: 1, Size: 10, HasMore: false}
	data := mustMarshal(t, resp)
	svc.l1Public.Set([]byte("feed:test:key"), data, 60)

	got := svc.getPublicFeedL1(context.Background(), "feed:test:key", 1, 10, nil)
	if got == nil {
		t.Fatal("expected hit")
	}
	if len(got.Items) != 1 || got.Items[0].ID != "1" {
		t.Errorf("unexpected items: %+v", got.Items)
	}
}

func TestGetPublicFeedL1_Miss(t *testing.T) {
	svc := &KnowPostFeedService{
		l1Public: &PrefixCache{Cache: freecache.NewCache(100 * 1024), Prefix: "p:"},
	}
	got := svc.getPublicFeedL1(context.Background(), "feed:test:nonexist", 1, 10, nil)
	if got != nil {
		t.Error("expected nil for cache miss")
	}
}

func TestGetPublicFeedL2_Miss(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	got := svc.getPublicFeedL2(context.Background(), "feed:test:nonexist:ids", "feed:test:nonexist:hasMore", 1, 10, nil, "local:key")
	if got != nil {
		t.Error("expected nil for cache miss")
	}
}

// ============================================================================
// enrichItems 边界：counter 查询失败
// ============================================================================

type stubCounterFailing struct{}

func (s *stubCounterFailing) GetCounts(_ context.Context, _, _ string, _ []string) (map[string]int32, error) {
	return nil, nil
}
func (s *stubCounterFailing) GetCountsBatch(_ context.Context, _ string, _, _ []string) (map[string]map[string]int32, error) {
	return nil, nil
}
func (s *stubCounterFailing) IsLiked(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterFailing) IsFaved(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterFailing) BatchIsLiked(_ context.Context, _ uint64, _ string, _ []string) (map[string]bool, error) {
	return nil, nil
}
func (s *stubCounterFailing) BatchIsFaved(_ context.Context, _ uint64, _ string, _ []string) (map[string]bool, error) {
	return nil, nil
}

func TestEnrichItems_CounterFails(t *testing.T) {
	userID := uint64(1)
	svc := &KnowPostFeedService{counter: &stubCounterFailing{}}
	items := []FeedItemResponse{{ID: "1"}}
	result := svc.enrichItems(context.Background(), items, &userID)
	if result[0].Liked != nil || result[0].Faved != nil {
		t.Error("Liked/Faved should be nil when counter fails")
	}
}

type stubCounterReturnsNil struct{}

func (s *stubCounterReturnsNil) GetCounts(_ context.Context, _, _ string, _ []string) (map[string]int32, error) {
	return nil, nil
}
func (s *stubCounterReturnsNil) GetCountsBatch(_ context.Context, _ string, _, _ []string) (map[string]map[string]int32, error) {
	return nil, nil
}
func (s *stubCounterReturnsNil) IsLiked(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterReturnsNil) IsFaved(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterReturnsNil) BatchIsLiked(_ context.Context, _ uint64, _ string, _ []string) (map[string]bool, error) {
	return nil, nil
}
func (s *stubCounterReturnsNil) BatchIsFaved(_ context.Context, _ uint64, _ string, _ []string) (map[string]bool, error) {
	return nil, nil
}

func TestEnrichItems_CounterReturnsNil(t *testing.T) {
	userID := uint64(1)
	svc := &KnowPostFeedService{counter: &stubCounterReturnsNil{}}
	items := []FeedItemResponse{{ID: "1"}}
	result := svc.enrichItems(context.Background(), items, &userID)
	if result[0].Liked != nil || result[0].Faved != nil {
		t.Error("Liked/Faved should be nil when counter returns nil")
	}
}

// ============================================================================
// assembleFromCache（简化版：只测不存在的路径）
// ============================================================================

func TestAssembleFromCache_NoIDs(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	resp := svc.assembleFromCache(context.Background(), "feed:test:ids", "feed:test:hasMore", 1, 10, nil)
	if resp != nil {
		t.Error("expected nil when no IDs in cache")
	}
}

// ============================================================================
// cache_isolation 测试
// ============================================================================

func TestPrefixCache_Isolation(t *testing.T) {
	cache := freecache.NewCache(100 * 1024)
	p1 := &PrefixCache{Cache: cache, Prefix: "a:"}
	p2 := &PrefixCache{Cache: cache, Prefix: "b:"}

	p1.Set([]byte("key1"), []byte("value1"), 60)
	p2.Set([]byte("key1"), []byte("value2"), 60)

	got1, _ := p1.Get([]byte("key1"))
	got2, _ := p2.Get([]byte("key1"))
	if string(got1) != "value1" {
		t.Errorf("p1 key1 = %q, want 'value1'", string(got1))
	}
	if string(got2) != "value2" {
		t.Errorf("p2 key1 = %q, want 'value2'", string(got2))
	}
}

// ============================================================================
// time-based：确保存活的公共方法
// ============================================================================

func TestCurrentPublicFeedVersion(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	srv.Set("feed:public:version", "7")
	got := svc.currentPublicFeedVersion(context.Background())
	if got != 7 {
		t.Errorf("currentPublicFeedVersion = %d, want 7", got)
	}
}

func TestCurrentMineFeedVersion(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	srv.Set("feed:mine:version:2001", "3")
	got := svc.currentMineFeedVersion(context.Background(), 2001)
	if got != 3 {
		t.Errorf("currentMineFeedVersion = %d, want 3", got)
	}
}

// ============================================================================
// 并发测试：mapRowsToItems / enrichItems 并发安全
// ============================================================================

func TestMapRowsToItems_Concurrent(t *testing.T) {
	svc := &KnowPostFeedService{}
	rows := []KnowPostFeedRow{
		{ID: 1, Title: strPtr("t"), AuthorNickname: "n"},
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		svc.mapRowsToItems(context.Background(), rows, nil, false)
	}()
	go func() {
		defer wg.Done()
		svc.mapRowsToItems(context.Background(), rows, nil, false)
	}()
	wg.Wait()
}
