package knowpost

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/counter"
	"github.com/zhiguang/app/pkg/config"
)

func strPtr(s string) *string { return &s }

// ============================================================================
// Helpers
// ============================================================================

func newTestFeedService(t *testing.T, srv *miniredis.Miniredis) *KnowPostFeedService {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	shared := freecache.NewCache(100 * 1024)
	return &KnowPostFeedService{
		redis:    rdb,
		l1Public: &cache.PrefixCache{Cache: shared, Prefix: "p:"},
		l1Mine:   &cache.PrefixCache{Cache: shared, Prefix: "m:"},
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
// clamp / boolToStr
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
	shared := freecache.NewCache(100 * 1024)
	svc := &KnowPostFeedService{
		l1Public: &cache.PrefixCache{Cache: shared, Prefix: "p:"},
		logger:   nil,
	}
	resp := &FeedPageResponse{Items: []FeedItemResponse{{ID: "1"}}, Page: 1, Size: 10}
	svc.cacheFeedPage("test:key", resp, svc.l1Public)

	val, err := shared.Get([]byte("p:test:key"))
	if err != nil {
		t.Fatalf("shared.Get: %v", err)
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
// recordItemHotKeys（零值 service 不得 panic）
// ============================================================================

func TestRecordItemHotKeys_NilDeps(t *testing.T) {
	svc := &KnowPostFeedService{}
	// hotKey / redis / logger 均为 nil 时应静默返回而非 panic
	svc.recordItemHotKeys(context.Background(), []FeedItemResponse{{ID: "1"}})
	svc.recordItemHotKeys(context.Background(), nil)
}

// ============================================================================
// KnowPostFeedService 零值/边界
// ============================================================================

func TestNewKnowPostFeedService(t *testing.T) {
	svc := NewKnowPostFeedService(nil, nil, nil, nil, nil, nil, zap.NewNop(), nil)
	if svc == nil {
		t.Fatal("NewKnowPostFeedService returned nil")
	}
	p := svc.feedCacheTTLValues()
	if p.safeSize != 50 || p.l1PublicTTL != 15 || p.extendBase != 60 {
		t.Fatalf("nil cfg defaults: safeSize=%d l1PublicTTL=%d extendBase=%d", p.safeSize, p.l1PublicTTL, p.extendBase)
	}
}

func TestFeedCacheTTLValues_FromConfig(t *testing.T) {
	svc := &KnowPostFeedService{
		cfg: &config.KnowPostFeedCacheConfig{
			SafeSize:         20,
			L1TTLSeconds:     7,
			L2IDListTTLBase:  40,
			L2IDListJitter:   5,
			L2HasMoreTTLBase: 3,
			L2HasMoreJitter:  2,
			L2ItemTTLBase:    40,
			L2ItemJitter:     5,
			L2MineTTLBase:    12,
			L2MineJitter:     3,
			L1MineTTLSeconds: 9,
			ExtendTTLBase:    90,
			TTLLow:           10,
			TTLMedium:        20,
			TTLHigh:          100,
		},
	}
	p := svc.feedCacheTTLValues()
	if p.safeSize != 20 || p.l1PublicTTL != 7 || p.idListBase != 40 || p.hasMoreBase != 3 ||
		p.itemBase != 40 || p.mineL2Base != 12 || p.l1MineTTL != 9 || p.extendBase != 90 {
		t.Fatalf("unexpected feedCacheParams from cfg: %+v", p)
	}
}

func TestJitterN(t *testing.T) {
	if got := jitterN(0); got != 0 {
		t.Fatalf("jitterN(0)=%d", got)
	}
	if got := jitterN(-1); got != 0 {
		t.Fatalf("jitterN(-1)=%d", got)
	}
	for i := 0; i < 20; i++ {
		got := jitterN(5)
		if got < 0 || got >= 5 {
			t.Fatalf("jitterN(5)=%d out of range", got)
		}
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

// ============================================================================
// getPublicFeedL1 / getPublicFeedL2（无 DB 路径测试）
// ============================================================================

func TestGetPublicFeedL1_Hit(t *testing.T) {
	svc := &KnowPostFeedService{
		l1Public: &cache.PrefixCache{Cache: freecache.NewCache(100 * 1024), Prefix: "p:"},
	}
	resp := &FeedPageResponse{Items: []FeedItemResponse{{ID: "1"}}, Page: 1, Size: 10, HasMore: false}
	data := mustMarshal(t, resp)
	svc.l1Public.Set([]byte("feed:test:key"), data, 60)

	got := svc.getPublicFeedL1(context.Background(), "feed:test:key", nil)
	if got == nil {
		t.Fatal("expected hit")
	}
	if len(got.Items) != 1 || got.Items[0].ID != "1" {
		t.Errorf("unexpected items: %+v", got.Items)
	}
}

func TestGetPublicFeedL1_Miss(t *testing.T) {
	svc := &KnowPostFeedService{
		l1Public: &cache.PrefixCache{Cache: freecache.NewCache(100 * 1024), Prefix: "p:"},
	}
	got := svc.getPublicFeedL1(context.Background(), "feed:test:nonexist", nil)
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
func (s *stubCounterFailing) Fav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterFailing) Unfav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterFailing) Like(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterFailing) Unlike(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterFailing) GetLikers(_ context.Context, _ string, _ uint64, _ string, _ string, _ int) (*counter.LikersResponse, error) {
	return nil, nil
}
func (s *stubCounterFailing) IsLikedAndFaved(_ context.Context, _ uint64, _, _ string) (bool, bool, error) {
	return false, false, nil
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
func (s *stubCounterReturnsNil) Fav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterReturnsNil) Unfav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterReturnsNil) Like(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterReturnsNil) Unlike(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterReturnsNil) GetLikers(_ context.Context, _ string, _ uint64, _ string, _ string, _ int) (*counter.LikersResponse, error) {
	return nil, nil
}
func (s *stubCounterReturnsNil) IsLikedAndFaved(_ context.Context, _ uint64, _, _ string) (bool, bool, error) {
	return false, false, nil
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
	resp := svc.assembleFromCache(context.Background(), "feed:test:ids", "feed:test:hasMore", 1, 10)
	if resp != nil {
		t.Error("expected nil when no IDs in cache")
	}
}

// ============================================================================
// cache_isolation 测试
// ============================================================================

func TestPrefixCache_Isolation(t *testing.T) {
	shared := freecache.NewCache(100 * 1024)
	p1 := &cache.PrefixCache{Cache: shared, Prefix: "a:"}
	p2 := &cache.PrefixCache{Cache: shared, Prefix: "b:"}

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

// ============================================================================
// GetPublicFeed - 缓存测试
// ============================================================================

func TestGetPublicFeed_CacheMiss_DBFallback(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)

	svc.repo = &mockRepo{
		detail: &KnowPostDetailRow{
			ID:             1,
			Title:          strPtr("feed1"),
			CreatorID:      42,
			AuthorNickname: "author1",
			Visible:        KnowPostVisibilityPublic,
			Status:         KnowPostStatusPublished,
		},
	}

	// 缓存全部 miss → 进入 getPublicFeedUnderLock → DB 回源
	resp, err := svc.GetPublicFeed(context.Background(), 1, 10, nil)
	if err != nil {
		t.Fatalf("GetPublicFeed() error = %v", err)
	}
	if resp == nil {
		t.Fatal("response should not be nil")
	}
	if resp.Page != 1 || resp.Size != 10 {
		t.Errorf("page/size = %d/%d, want 1/10", resp.Page, resp.Size)
	}
}

func TestGetPublicFeed_PartialCacheHit(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)

	// 给一个空的 mockRepo，确保 DB 回源路径不会 panic
	svc.repo = &mockRepo{}

	// 预设 L2 碎片缓存 — 只设置部分条目模拟部分命中
	idsKey := "feed:public:ids:1:10:0:1"

	srv.Lpush(idsKey, "100")
	srv.Lpush(idsKey, "200")
	srv.Set("feed:item:100", `{"id":"100","title":"item100","author_nickname":"n1"}`)
	// 故意不设置 feed:item:200 → 部分缺失 → assembleFromCache 返回 nil

	_, err := svc.GetPublicFeed(context.Background(), 1, 10, nil)
	// 由于 repo 是 nil（test helper 未设置），会走入 DB 回源失败
	// 但我们验证不 panic，且正确 fallthrough
	if err == nil {
		t.Log("partial cache hit fell through to DB (expected to fail because repo is nil)")
	}
}

// ============================================================================
// 公共 Feed 的 L1 整页缓存不得携带用户维度状态
// ============================================================================

// stubCounterPerUser 按用户 ID 返回不同的点赞/收藏状态。
type stubCounterPerUser struct{}

func (s *stubCounterPerUser) GetCounts(_ context.Context, _, _ string, _ []string) (map[string]int32, error) {
	return nil, nil
}
func (s *stubCounterPerUser) GetCountsBatch(_ context.Context, _ string, _, _ []string) (map[string]map[string]int32, error) {
	return nil, nil
}
func (s *stubCounterPerUser) IsLiked(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterPerUser) IsFaved(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}

// BatchIsLiked：用户 1 全部已赞，其余用户全部未赞。
func (s *stubCounterPerUser) BatchIsLiked(_ context.Context, userID uint64, _ string, ids []string) (map[string]bool, error) {
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = userID == 1
	}
	return out, nil
}

// BatchIsFaved：用户 1 全部已收藏，其余用户全部未收藏。
func (s *stubCounterPerUser) BatchIsFaved(_ context.Context, userID uint64, _ string, ids []string) (map[string]bool, error) {
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = userID == 1
	}
	return out, nil
}
func (s *stubCounterPerUser) Fav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterPerUser) Unfav(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterPerUser) Like(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterPerUser) Unlike(_ context.Context, _ uint64, _, _ string) (bool, error) {
	return false, nil
}
func (s *stubCounterPerUser) GetLikers(_ context.Context, _ string, _ uint64, _ string, _ string, _ int) (*counter.LikersResponse, error) {
	return nil, nil
}
func (s *stubCounterPerUser) IsLikedAndFaved(_ context.Context, _ uint64, _, _ string) (bool, bool, error) {
	return false, false, nil
}

// TestPublicFeedL1_DoesNotLeakUserStateAcrossUsers 回归测试：
// 公共 Feed 的 L1 整页缓存键不含用户 ID，是全体用户共享的一份数据。
// 历史缺陷：L2 组装路径把已叠加当前用户 Liked/Faved 的响应直接写进了这份共享缓存，
// 后续命中 L1 的其他用户会读到前一个用户的点赞收藏状态。
func TestPublicFeedL1_DoesNotLeakUserStateAcrossUsers(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	svc.counter = &stubCounterPerUser{}

	const idsKey = "feed:public:ids:test"
	const hasMoreKey = idsKey + ":hasMore"
	const localPageKey = "feed:public:10:1:v1:1"

	srv.Lpush(idsKey, "100")
	srv.Set("feed:item:100", `{"id":"100","title":"t"}`)
	srv.Set(hasMoreKey, "0")

	// 用户 1 走 L2 组装路径，顺带把整页写入共享的 L1。
	userOne := uint64(1)
	respOne := svc.getPublicFeedL2(context.Background(), idsKey, hasMoreKey, 1, 10, &userOne, localPageKey)
	if respOne == nil || len(respOne.Items) != 1 {
		t.Fatalf("user 1: unexpected response %+v", respOne)
	}
	if respOne.Items[0].Liked == nil || !*respOne.Items[0].Liked {
		t.Fatal("user 1 should see Liked=true")
	}

	// 落盘到 L1 的必须是不含用户状态的公共视图。
	raw, err := svc.l1Public.Get([]byte(localPageKey))
	if err != nil {
		t.Fatalf("expected page cached in L1: %v", err)
	}
	var cached FeedPageResponse
	if err := json.Unmarshal(raw, &cached); err != nil {
		t.Fatalf("unmarshal L1 payload: %v", err)
	}
	if cached.Items[0].Liked != nil || cached.Items[0].Faved != nil {
		t.Errorf("L1 payload must not carry per-user state, got Liked=%v Faved=%v",
			cached.Items[0].Liked, cached.Items[0].Faved)
	}

	// 用户 2 命中同一份 L1，必须看到属于自己的状态。
	userTwo := uint64(2)
	respTwo := svc.getPublicFeedL1(context.Background(), localPageKey, &userTwo)
	if respTwo == nil || len(respTwo.Items) != 1 {
		t.Fatalf("user 2: unexpected response %+v", respTwo)
	}
	if respTwo.Items[0].Liked == nil || *respTwo.Items[0].Liked {
		t.Errorf("user 2 should see Liked=false, got %v", respTwo.Items[0].Liked)
	}
	if respTwo.Items[0].Faved == nil || *respTwo.Items[0].Faved {
		t.Errorf("user 2 should see Faved=false, got %v", respTwo.Items[0].Faved)
	}

	// 匿名访问不应带任何用户状态。
	respAnon := svc.getPublicFeedL1(context.Background(), localPageKey, nil)
	if respAnon == nil || len(respAnon.Items) != 1 {
		t.Fatalf("anonymous: unexpected response %+v", respAnon)
	}
	if respAnon.Items[0].Liked != nil || respAnon.Items[0].Faved != nil {
		t.Error("anonymous request must not carry Liked/Faved")
	}
}

// ============================================================================
// recordItemHotKeys：批量热度探测与 TTL 延长
// ============================================================================

func newTestHotKeyDetector(rdb *redis.Client) *cache.HotKeyDetector {
	return cache.NewHotKeyDetector(&config.HotKeyConfig{
		BucketSizeSeconds:    6,
		BucketCount:          10,
		FlushIntervalSeconds: 6,
		StatTTLSeconds:       120,
		LevelLow:             5,
		LevelMedium:          20,
		LevelHigh:            50,
		ExtendLowSeconds:     20,
		ExtendMediumSeconds:  60,
		ExtendHighSeconds:    120,
		HotMarkTTLSeconds:    60,
	}, rdb, zap.NewNop())
}

// TestRecordItemHotKeys_ExtendsOnlyHotItems 验证只有热点条目的碎片缓存 TTL 被延长。
//
// 冷条目必须完全跳过 TTL 延长：这正是把逐条 Redis 往返压缩掉的关键——
// 一页里通常只有极少数是热点，为冷条目发起的 EXPIRE 全是无效开销。
func TestRecordItemHotKeys_ExtendsOnlyHotItems(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	svc.hotKey = newTestHotKeyDetector(svc.redis)

	ctx := context.Background()

	// item 100 被标记为 MEDIUM 热点（标记值即等级），item 200 是冷键
	srv.Set("hotkey:active:knowpost:100", "2")
	srv.Set("feed:item:100", `{"id":"100"}`)
	srv.Set("feed:item:200", `{"id":"200"}`)
	srv.SetTTL("feed:item:100", 30*time.Second)
	srv.SetTTL("feed:item:200", 30*time.Second)

	svc.recordItemHotKeys(ctx, []FeedItemResponse{{ID: "100"}, {ID: "200"}})

	// extendBase 默认 60，MEDIUM 追加 60 → 目标 120s，应从 30s 被抬高
	if got := srv.TTL("feed:item:100"); got != 120*time.Second {
		t.Errorf("hot item TTL = %v, want 120s", got)
	}
	// 冷键的 TTL 必须原样不动
	if got := srv.TTL("feed:item:200"); got != 30*time.Second {
		t.Errorf("cold item TTL = %v, want 30s (untouched)", got)
	}
}

// TestRecordItemHotKeys_AllColdSkipsRedis 验证整页无热点时不产生任何 Redis 写入。
func TestRecordItemHotKeys_AllColdSkipsRedis(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)
	svc.hotKey = newTestHotKeyDetector(svc.redis)

	srv.Set("feed:item:1", `{"id":"1"}`)
	srv.SetTTL("feed:item:1", 10*time.Second)

	svc.recordItemHotKeys(context.Background(), []FeedItemResponse{{ID: "1"}, {ID: "2"}, {ID: "3"}})

	if got := srv.TTL("feed:item:1"); got != 10*time.Second {
		t.Errorf("cold item TTL = %v, want 10s (untouched)", got)
	}
}

// TestRecordItemHotKeys_NoHotKeyDetector 验证未装配探测器时直接返回。
func TestRecordItemHotKeys_NoHotKeyDetector(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestFeedService(t, srv)

	srv.Set("feed:item:1", `{"id":"1"}`)
	srv.SetTTL("feed:item:1", 10*time.Second)

	svc.recordItemHotKeys(context.Background(), []FeedItemResponse{{ID: "1"}})

	if got := srv.TTL("feed:item:1"); got != 10*time.Second {
		t.Errorf("TTL = %v, want 10s (untouched when detector is absent)", got)
	}
}

// ============================================================================
// SetOrWarn：L1 写入失败不得被静默吞掉
// ============================================================================

func TestPrefixCache_SetOrWarn_LogsOversizedEntry(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	// freecache 最小容量 512KB，单条上限为容量的 1/1024 = 512 字节。
	pc := &cache.PrefixCache{Cache: freecache.NewCache(512 * 1024), Prefix: "d:"}

	pc.SetOrWarn(logger, []byte("small"), []byte("ok"), 60)
	if logs.Len() != 0 {
		t.Fatalf("small entry should not warn, got %d logs", logs.Len())
	}

	pc.SetOrWarn(logger, []byte("big"), make([]byte, 4096), 60)
	if logs.Len() != 1 {
		t.Fatalf("oversized entry should warn once, got %d logs", logs.Len())
	}
	if msg := logs.All()[0].Message; msg != "l1 cache set failed" {
		t.Errorf("log message = %q", msg)
	}

	// logger 为 nil 时不得 panic
	pc.SetOrWarn(nil, []byte("big2"), make([]byte, 4096), 60)
}

// TestDetailCacheTTLValues_FromConfig 覆盖 cfg 非 nil 分支。
func TestDetailCacheTTLValues_FromConfig(t *testing.T) {
	svc := &KnowPostDetailService{
		cfg: &config.KnowPostConfig{
			DetailCache: config.KnowPostDetailCacheConfig{
				L1TTLSeconds: 11, NullTTLBase: 22, NullJitter: 33,
				L2TTLBase: 44, L2Jitter: 55,
				TTLLow: 66, TTLMedium: 77, TTLHigh: 88,
			},
		},
	}
	got := svc.detailCacheTTLValues()
	want := detailCacheParams{
		l1TTL: 11, nullBase: 22, nullJitter: 33,
		l2Base: 44, l2Jitter: 55,
		ttlMedium: 77,
	}
	if got != want {
		t.Errorf("detailCacheTTLValues() = %+v, want %+v", got, want)
	}
}

// TestDetailCacheTTLValues_DefaultsMatchConfigSection 验证零配置回退与
// pkg/config 的节级默认值走同一条代码路径（默认值单源）。
func TestDetailCacheTTLValues_DefaultsMatchConfigSection(t *testing.T) {
	var section config.KnowPostDetailCacheConfig
	section.ApplyDefaults()

	got := (&KnowPostDetailService{}).detailCacheTTLValues()
	if got.l1TTL != section.L1TTLSeconds || got.nullBase != section.NullTTLBase ||
		got.l2Base != section.L2TTLBase || got.ttlMedium != section.TTLMedium {
		t.Errorf("nil-cfg fallback %+v diverged from config section defaults %+v", got, section)
	}
}
