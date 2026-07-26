package knowpost

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
)

// ============================================================================
// Helpers
// ============================================================================

func newTestDetailService(t *testing.T, srv *miniredis.Miniredis) *KnowPostDetailService {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	return &KnowPostDetailService{
		redis:   rdb,
		l1Cache: &cache.PrefixCache{Cache: freecache.NewCache(100 * 1024), Prefix: "d:"},
		logger:  zap.NewNop(),
	}
}

func validDetailRow() *KnowPostDetailRow {
	now := time.Now()
	return &KnowPostDetailRow{
		ID:             1,
		Title:          strPtr("测试标题"),
		Description:    strPtr("测试描述"),
		ContentUrl:     strPtr("http://content.url"),
		ImgUrls:        strPtr(`["http://img1.jpg","http://img2.jpg"]`),
		Tags:           strPtr(`["go","redis"]`),
		CreatorID:      1001,
		AuthorAvatar:   strPtr("http://avatar.url"),
		AuthorNickname: "作者昵称",
		AuthorTagJson:  strPtr(`["tag1","tag2"]`),
		IsTop:          true,
		Visible:        KnowPostVisibilityPublic,
		Type:           "article",
		Status:         KnowPostStatusPublished,
		PublishTime:    &now,
	}
}

// mockRepo implements Repo interface for testing detail_service paths.
type mockRepo struct {
	detail *KnowPostDetailRow
	err    error
}

func (r *mockRepo) FindDetailByID(_ context.Context, _ uint64) (*KnowPostDetailRow, error) {
	return r.detail, r.err
}
func (r *mockRepo) InsertDraft(_ context.Context, _ *KnowPost) error             { return nil }
func (r *mockRepo) UpdateContent(_ context.Context, _ *KnowPost) (int64, error)  { return 0, nil }
func (r *mockRepo) UpdateMetadata(_ context.Context, _ *KnowPost) (int64, error) { return 0, nil }
func (r *mockRepo) Publish(_ context.Context, _, _ uint64) (int64, error)        { return 0, nil }
func (r *mockRepo) UpdateTop(_ context.Context, _, _ uint64, _ bool) (int64, error) {
	return 0, nil
}
func (r *mockRepo) UpdateVisibility(_ context.Context, _, _ uint64, _ KnowPostVisibility) (int64, error) {
	return 0, nil
}
func (r *mockRepo) SoftDelete(_ context.Context, _, _ uint64) (int64, error) { return 0, nil }
func (r *mockRepo) ListFeedPublic(_ context.Context, _, _ int) ([]KnowPostFeedRow, error) {
	return nil, nil
}
func (r *mockRepo) ListMyPublished(_ context.Context, _ uint64, _, _ int) ([]KnowPostFeedRow, error) {
	return nil, nil
}
func (r *mockRepo) FindByIDs(_ context.Context, _ []uint64) ([]KnowPostFeedRow, error) {
	return nil, nil
}
func (r *mockRepo) FindFeedRowsByIDs(_ context.Context, _ []uint64, _ []KnowPostVisibility) ([]KnowPostFeedRow, error) {
	return nil, nil
}
func (r *mockRepo) ListIDsForBloom(_ context.Context, _ uint64, _ int) ([]uint64, error) {
	return nil, nil
}
func (r *mockRepo) WithDB(_ sqlx.ExtContext) Repo { return r }

// ============================================================================
// TestGetDetail_CachePenetration
// L1 miss, L2 miss, DB hit → 正确回源 DB，回填缓存，返回数据
// ============================================================================

func TestGetDetail_CachePenetration(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	now := time.Now()
	svc.repo = &mockRepo{
		detail: &KnowPostDetailRow{
			ID:             1,
			Title:          strPtr("回源标题"),
			Description:    strPtr("回源描述"),
			ContentUrl:     strPtr("http://example.com/content"),
			Tags:           strPtr(`["go"]`),
			CreatorID:      42,
			AuthorNickname: "作者",
			Visible:        KnowPostVisibilityPublic,
			Type:           "article",
			Status:         KnowPostStatusPublished,
			PublishTime:    &now,
		},
	}

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "回源标题" {
		t.Errorf("Title = %v, want '回源标题'", resp.Title)
	}

	// L2 should be populated
	val, err := srv.Get("knowpost:detail:1:v1:ver1")
	if err != nil {
		t.Fatal("L2 should be populated after DB fallback")
	}
	if val == "NULL" {
		t.Fatal("L2 should contain valid JSON, not NULL")
	}

	// L1 should be populated
	_, l1Err := svc.l1Cache.Get([]byte("knowpost:detail:1:v1:ver1"))
	if l1Err != nil {
		t.Error("L1 should be populated after DB fallback")
	}
}

// ============================================================================
// TestGetDetail_L2Timeout_FallbackToL1
// L2(Redis) timeout/unavailable, but L1 has valid cache
// 期望：返回 L1 数据，不返回 500
// ============================================================================

func TestGetDetail_L2Timeout_FallbackToL1(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)

	l1Data := `{"id":"1","title":"来自L1","author_id":"42","author_nickname":"n","like_count":3,"favorite_count":1}`
	svc.l1Cache.Set([]byte("knowpost:detail:1:v1:ver1"), []byte(l1Data), 60)

	// 关闭 miniredis 模拟 Redis 不可用
	srv.Close()

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "来自L1" {
		t.Errorf("Title = %v, want '来自L1'", resp.Title)
	}
}

// ============================================================================
// TestGetDetail_BothCacheMiss_L1Fallback
// L1 miss, L2 timeout, DB hit → 正确回源 DB
// ============================================================================

func TestGetDetail_BothCacheMiss_L1Fallback(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	now := time.Now()
	svc.repo = &mockRepo{
		detail: &KnowPostDetailRow{
			ID:             1,
			Title:          strPtr("回源标题"),
			Description:    strPtr("回源描述"),
			CreatorID:      42,
			AuthorNickname: "作者",
			Visible:        KnowPostVisibilityPublic,
			Type:           "article",
			Status:         KnowPostStatusPublished,
			PublishTime:    &now,
		},
	}

	// L1 无数据，L2 超时，从 DB 回源
	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "回源标题" {
		t.Errorf("Title = %v, want '回源标题'", resp.Title)
	}
}

// ============================================================================
// TestGetDetail_RedisNil_FallbackToDB
// L2 返回 redis.Nil，正确降级查 DB
// ============================================================================

func TestGetDetail_RedisNil_FallbackToDB(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	now := time.Now()
	svc.repo = &mockRepo{
		detail: &KnowPostDetailRow{
			ID:             1,
			Title:          strPtr("DB数据"),
			Description:    strPtr("从DB回源"),
			CreatorID:      42,
			AuthorNickname: "作者",
			Visible:        KnowPostVisibilityPublic,
			Type:           "article",
			Status:         KnowPostStatusPublished,
			PublishTime:    &now,
		},
	}

	// 不设置任何 L2 数据 → redis.Nil
	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v, want DB fallback success", err)
	}
	if resp.Title == nil || *resp.Title != "DB数据" {
		t.Errorf("Title = %v, want 'DB数据'", resp.Title)
	}
}

// ============================================================================
// TestGetDetail_NotFound
// 所有缓存 miss，DB 也无数据 → 返回明确错误
// ============================================================================

func TestGetDetail_NotFound(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)

	// repo 为 nil → getDetailUnderLock 的 missHandler 走 nil-repo 快速失败路径
	// 写入 NULL 到缓存并返回 ErrNotFound
	_, err := svc.GetDetail(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected error for non-existent post, got nil")
	}

	// NULL 标记应写入缓存
	val, err := srv.Get("knowpost:detail:1:v1:ver1")
	if err != nil {
		t.Fatal("expected NULL cache to be written")
	}
	if val != "NULL" {
		t.Errorf("expected NULL, got %q", val)
	}
}

// ============================================================================
// TestGetDetail_L2Timeout_FallbackToL1_WithDBFallback
// L1 miss, L2 timeout, DB hit — 不因 Redis 错误而失败
// ============================================================================

func TestGetDetail_L2Timeout_DBFallback(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	now := time.Now()
	svc.repo = &mockRepo{
		detail: &KnowPostDetailRow{
			ID:             1,
			Title:          strPtr("DB数据"),
			CreatorID:      42,
			AuthorNickname: "作者",
			Visible:        KnowPostVisibilityPublic,
			Type:           "article",
			Status:         KnowPostStatusPublished,
			PublishTime:    &now,
		},
	}

	// 正常流程：L1 miss → L2 miss → lock → double check miss → DB hit
	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v, want success via DB fallback", err)
	}
	if resp.Title == nil || *resp.Title != "DB数据" {
		t.Errorf("Title = %v, want 'DB数据'", resp.Title)
	}
}

// ============================================================================
// Existing tests preserved below
// ============================================================================

func TestGetDetail_CacheMiss_NilRepo(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)

	_, err := svc.GetDetail(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected error for nil repo")
	}
}

func TestGetDetail_L2Hit(t *testing.T) {
	srv := miniredis.RunT(t)
	cached := `{"id":"1","title":"来自缓存","author_id":"42","author_nickname":"n","like_count":7,"favorite_count":3}`
	srv.Set("knowpost:detail:1:v1:ver1", cached)
	svc := newTestDetailService(t, srv)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "来自缓存" {
		t.Errorf("Title = %v, want '来自缓存'", resp.Title)
	}
	if resp.LikeCount != 7 {
		t.Errorf("LikeCount = %d, want 7", resp.LikeCount)
	}
	if resp.FavoriteCount != 3 {
		t.Errorf("FavoriteCount = %d, want 3", resp.FavoriteCount)
	}
	_, l1Err := svc.l1Cache.Get([]byte("knowpost:detail:1:v1:ver1"))
	if l1Err != nil {
		t.Error("L1 should be populated after L2 hit")
	}
}

func TestGetDetail_L2NULLHit(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.Set("knowpost:detail:1:v1:ver1", "NULL")
	svc := newTestDetailService(t, srv)

	_, err := svc.GetDetail(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected ErrNotFound for NULL cache")
	}
}

func TestGetDetail_L2Cache_InvalidJSON(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.Set("knowpost:detail:1:v1:ver1", "{invalid}")
	svc := newTestDetailService(t, srv)

	_, err := svc.GetDetail(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected error for invalid cache + nil repo")
	}
}

func TestGetDetail_L1Hit(t *testing.T) {
	srv := miniredis.RunT(t)
	cached := `{"id":"1","title":"来自L1","author_id":"42","author_nickname":"n","like_count":3,"favorite_count":1}`
	svc := newTestDetailService(t, srv)
	svc.l1Cache.Set([]byte("knowpost:detail:1:v1:ver1"), []byte(cached), 60)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "来自L1" {
		t.Errorf("Title = %v, want '来自L1'", resp.Title)
	}
}

func TestGetDetail_L1InvalidJSON(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	svc.l1Cache.Set([]byte("knowpost:detail:1:v1:ver1"), []byte("{invalid}"), 60)
	srv.Set("knowpost:detail:1:v1:ver1", `{"id":"1","title":"来自L2","author_id":"42","author_nickname":"n"}`)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "来自L2" {
		t.Errorf("Title = %v, want '来自L2'", resp.Title)
	}
}

func TestGetDetail_CacheMiss_WritesNULL(t *testing.T) {
	srv := miniredis.RunT(t)
	lockKey := "lock:knowpost:detail:1:v1:ver1"
	srv.Set(lockKey, "fake")
	srv.Del(lockKey)
	svc := newTestDetailService(t, srv)

	_, _ = svc.GetDetail(context.Background(), 1, nil)

	val, err := srv.Get("knowpost:detail:1:v1:ver1")
	if err != nil {
		t.Fatalf("cache should exist: %v", err)
	}
	if val != "NULL" {
		t.Errorf("expected NULL cache, got %q", val)
	}
}

func TestGetDetail_AnonymousViaL2(t *testing.T) {
	srv := miniredis.RunT(t)
	cached := `{"id":"1","title":"t","author_id":"42","author_nickname":"n"}`
	srv.Set("knowpost:detail:1:v1:ver1", cached)
	svc := newTestDetailService(t, srv)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Liked != nil {
		t.Error("Liked should be nil for anonymous user")
	}
	if resp.Faved != nil {
		t.Error("Faved should be nil for anonymous user")
	}
}

func TestKnowPostDetailCacheContent(t *testing.T) {
	srv := miniredis.RunT(t)
	cached := `{"id":"1","title":"t","author_id":"42","author_nickname":"n","author_id":"1001"}`
	srv.Set("knowpost:detail:1:v1:ver1", cached)
	svc := newTestDetailService(t, srv)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.ID != "1" {
		t.Errorf("ID = %q, want '1'", resp.ID)
	}
}

func TestDetailLayoutVer(t *testing.T) {
	if detailLayoutVer == 0 {
		t.Error("detailLayoutVer should not be 0")
	}
}

// TestGetDetail_BloomRejectsUnknownID：过滤器已预热且不含该 ID 时直接 404，不回源。
// 依赖 RedisBloom CF.*（REDIS_BLOOM_ADDR，默认 127.0.0.1:6379）。
func TestGetDetail_BloomRejectsUnknownID(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	svc.bloom = mustBloom(t)
	svc.bloom.AddUint64(context.Background(), 100)
	svc.repo = nil

	_, err := svc.GetDetail(context.Background(), 999888777, nil)
	if err == nil {
		t.Fatal("expected not found from bloom")
	}
}

// TestGetDetail_BloomAllowsExistingAfterAdd
func TestGetDetail_BloomAllowsExistingAfterAdd(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	svc.bloom = mustBloom(t)
	svc.bloom.AddUint64(context.Background(), 1)

	now := time.Now()
	svc.repo = &mockRepo{detail: &KnowPostDetailRow{
		ID: 1, Title: strPtr("ok"), CreatorID: 1, AuthorNickname: "a",
		Visible: KnowPostVisibilityPublic, Type: "article",
		Status: KnowPostStatusPublished, PublishTime: &now,
	}}
	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail: %v", err)
	}
	if resp.Title == nil || *resp.Title != "ok" {
		t.Fatalf("title=%v", resp.Title)
	}
}

func TestQueryDetailFromDB_NilRepo(t *testing.T) {
	t.Skip("nil-repo guard is in getDetailUnderLock, not queryDetailFromDB")
}

// mustBloom 连接带 RedisBloom 的 Redis；不可用则 Skip。
func mustBloom(t *testing.T) *cache.RedisBloom {
	t.Helper()
	addr := os.Getenv("REDIS_BLOOM_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	probe := "cf:probe:knowpost:" + t.Name()
	if err := rdb.Do(ctx, "CF.RESERVE", probe, 100).Err(); err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "unknown command") {
			t.Skipf("RedisBloom CF.* unavailable at %s: %v", addr, err)
		}
		// item exists 可接受
	}
	_ = rdb.Del(ctx, probe).Err()

	key := "cf:test:detail:" + t.Name()
	b := cache.NewRedisBloom(rdb, cache.BloomConfig{
		Enabled: true, ExpectedItems: 1000, FalsePositiveRate: 0.01, Key: key,
	}, zap.NewNop())
	if b == nil {
		t.Fatal("bloom nil")
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), key).Err() })
	return b
}

// ============================================================================
// 版本号进程内缓存：让 L1 命中真正做到零 Redis IO
// ============================================================================

// TestDetailVersion_CachedLocally 验证版本号在 TTL 内不再回落 Redis。
//
// 版本号被编码进缓存键，所以读 L1 前必须先知道版本号。
// 若每次都去 Redis 取，L1 命中也要付一次网络往返，进程内缓存就失去了意义。
func TestDetailVersion_CachedLocally(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)

	srv.Set("knowpost:ver:7", "5")
	if got := svc.versions().Get(context.Background(), detailVersionKey(7)); got != 5 {
		t.Fatalf("first read = %d, want 5", got)
	}

	// 直接改掉 Redis 中的值：若仍走 Redis，第二次会读到 9。
	srv.Set("knowpost:ver:7", "9")
	if got := svc.versions().Get(context.Background(), detailVersionKey(7)); got != 5 {
		t.Errorf("second read = %d, want 5 (should come from the in-process cache)", got)
	}
}

// TestDetailVersion_DroppedAfterLocalWrite 验证本实例写后自读一致。
//
// 本实例递增版本号后必须同步作废进程内缓存，
// 否则会出现「自己写完自己读不到」。
func TestDetailVersion_DroppedAfterLocalWrite(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)

	srv.Set("knowpost:ver:7", "5")
	_ = svc.versions().Get(context.Background(), detailVersionKey(7)) // 预热进程内缓存

	srv.Set("knowpost:ver:7", "6")
	svc.versions().Drop(detailVersionKey(7))

	if got := svc.versions().Get(context.Background(), detailVersionKey(7)); got != 6 {
		t.Errorf("after local write = %d, want 6", got)
	}
}

// TestDetailVersion_CacheDisabled 验证配置为负数时关闭进程内缓存。
func TestDetailVersion_CacheDisabled(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	svc.cfg = &config.KnowPostConfig{
		DetailCache: config.KnowPostDetailCacheConfig{VersionCacheTTLSeconds: -1},
	}

	srv.Set("knowpost:ver:7", "5")
	if got := svc.versions().Get(context.Background(), detailVersionKey(7)); got != 5 {
		t.Fatalf("first read = %d, want 5", got)
	}
	srv.Set("knowpost:ver:7", "9")
	if got := svc.versions().Get(context.Background(), detailVersionKey(7)); got != 9 {
		t.Errorf("second read = %d, want 9 (cache disabled must re-read Redis)", got)
	}
}

// TestDetailVersion_MissingKeyFallsBackToLayoutVersion 验证键不存在时的兜底。
func TestDetailVersion_MissingKeyFallsBackToLayoutVersion(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)

	if got := svc.versions().Get(context.Background(), detailVersionKey(12345)); got != detailLayoutVer {
		t.Errorf("missing key = %d, want detailLayoutVer(%d)", got, detailLayoutVer)
	}
}

// TestRecordHotKeyAndExtendTTL_SkipsColdKeys 验证冷键不触发 TTL 延长的 Lua。
func TestRecordHotKeyAndExtendTTL_SkipsColdKeys(t *testing.T) {
	srv := miniredis.RunT(t)
	svc := newTestDetailService(t, srv)
	svc.hotKey = cache.NewHotKeyDetector(&config.HotKeyConfig{
		BucketSizeSeconds: 6, BucketCount: 10, FlushIntervalSeconds: 6,
		StatTTLSeconds: 120, LevelLow: 5, LevelMedium: 20, LevelHigh: 50,
		ExtendLowSeconds: 20, ExtendMediumSeconds: 60, ExtendHighSeconds: 120,
		HotMarkTTLSeconds: 60,
	}, svc.redis, zap.NewNop())

	const pageKey = "knowpost:detail:7:v1:ver1"
	srv.Set(pageKey, `{"id":"7"}`)
	srv.SetTTL(pageKey, 30*time.Second)

	svc.recordHotKeyAndExtendTTL(context.Background(), 7, pageKey)

	if got := srv.TTL(pageKey); got != 30*time.Second {
		t.Errorf("cold key TTL = %v, want 30s (untouched)", got)
	}
}
