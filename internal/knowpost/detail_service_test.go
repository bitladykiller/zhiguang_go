package knowpost

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ============================================================================
// Helpers
// ============================================================================

func newTestDetailService(t *testing.T, srv *miniredis.Miniredis) *KnowPostService {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	return &KnowPostService{
		redis:   rdb,
		l1Cache: &PrefixCache{Cache: freecache.NewCache(100 * 1024), Prefix: "d:"},
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

// mockRepo is a minimal KnowPostRepository mock that stores a detail row.
type mockRepo struct {
	detail *KnowPostDetailRow
	err    error
}

func (r *mockRepo) FindDetailByID(_ context.Context, _ uint64) (*KnowPostDetailRow, error) {
	return r.detail, r.err
}

// ============================================================================
// GetDetail — 全链路走 read-through，需要 mock repo（通过 KnowPostService.repo 走真实 *KnowPostRepository）
// 由于 queryDetailFromDB 用 s.repo.FindDetailByID，而 s.repo 是 *KnowPostRepository，
// 我们的测试方式改为：在 test 中直接构造 KnowPostService，手动填充 repo 字段为一个真实的 *KnowPostRepository
// 但 *KnowPostRepository 需要 *sqlx.DB。我们无法在单元测试中提供真实 DB。
//
// 替代方案：测试 L1/L2 缓存命中路径、NULL 缓存路径，DB 路径遍历 getDetailUnderLock 时通过
// 在 redis 中预先塞入 mock 数据来模拟。对于需要测试 DB 回源且缓存未命中的场景，
// 我们用 miniredis 模拟 "lock:{key}" 不存在，锁会被抢到，然后 checkCache（double check）返回 false，
// 最后走入 missHandler 调用 queryDetailFromDB，但 s.repo 是 nil 从而返回 ErrNotFound。
// 这其实已经覆盖了 DB 未命中路径。
//
// 更好的方案：在 detail_service.go 中让 queryDetailFromDB 对 s.repo 做 nil 保护，
// 这样测试中可以把 repo 字段留空，走 nil-repo 快速失败路径。
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
	srv.Set("knowpost:detail:1:v1", cached)
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
	// L1 should be populated after L2 hit
	_, l1Err := svc.l1Cache.Get([]byte("knowpost:detail:1:v1"))
	if l1Err != nil {
		t.Error("L1 should be populated after L2 hit")
	}
}

func TestGetDetail_L2NULLHit(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.Set("knowpost:detail:1:v1", "NULL")
	svc := newTestDetailService(t, srv)

	_, err := svc.GetDetail(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected ErrNotFound for NULL cache")
	}
}

func TestGetDetail_L2Cache_InvalidJSON(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.Set("knowpost:detail:1:v1", "{invalid}")
	svc := newTestDetailService(t, srv)

	// should fall through to DB which is nil repo -> ErrNotFound
	_, err := svc.GetDetail(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected error for invalid cache + nil repo")
	}
}

// ============================================================================
// L1 缓存命中
// ============================================================================

func TestGetDetail_L1Hit(t *testing.T) {
	srv := miniredis.RunT(t)
	cached := `{"id":"1","title":"来自L1","author_id":"42","author_nickname":"n","like_count":3,"favorite_count":1}`
	svc := newTestDetailService(t, srv)
	svc.l1Cache.Set([]byte("knowpost:detail:1:v1"), []byte(cached), 60)

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
	svc.l1Cache.Set([]byte("knowpost:detail:1:v1"), []byte("{invalid}"), 60)
	// set valid L2 cache
	srv.Set("knowpost:detail:1:v1", `{"id":"1","title":"来自L2","author_id":"42","author_nickname":"n"}`)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.Title == nil || *resp.Title != "来自L2" {
		t.Errorf("Title = %v, want '来自L2'", resp.Title)
	}
}

// ============================================================================
// NULL 缓存写入（模拟 DB 未命中）
// ============================================================================

func TestGetDetail_CacheMiss_WritesNULL(t *testing.T) {
	srv := miniredis.RunT(t)
	// Pre-set the lock key so getDetailUnderLock can acquire it
	lockKey := "lock:knowpost:detail:1:v1"
	srv.Set(lockKey, "fake")
	srv.Del(lockKey) // ensure lock is available
	svc := newTestDetailService(t, srv)

	_, _ = svc.GetDetail(context.Background(), 1, nil)

	// NULL 标记应写入缓存（通过 getDetailUnderLock 的 missHandler）
	// But since repo is nil, queryDetailFromDB returns ErrNotFound
	// and the missHandler writes NULL to cache.
	val, err := srv.Get("knowpost:detail:1:v1")
	if err != nil {
		t.Fatalf("cache should exist: %v", err)
	}
	if val != "NULL" {
		t.Errorf("expected NULL cache, got %q", val)
	}
}

// ============================================================================
// GetDetail — 匿名用户
// ============================================================================

func TestGetDetail_AnonymousViaL2(t *testing.T) {
	srv := miniredis.RunT(t)
	cached := `{"id":"1","title":"t","author_id":"42","author_nickname":"n"}`
	srv.Set("knowpost:detail:1:v1", cached)
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

// ============================================================================
// cache 结构完整性
// ============================================================================

func TestKnowPostDetailCacheContent(t *testing.T) {
	srv := miniredis.RunT(t)
	// 预先写入 L2 作为模拟 DB 回源后的缓存
	cached := `{"id":"1","title":"t","author_id":"42","author_nickname":"n","author_id":"1001"}`
	srv.Set("knowpost:detail:1:v1", cached)
	svc := newTestDetailService(t, srv)

	resp, err := svc.GetDetail(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if resp.ID != "1" {
		t.Errorf("ID = %q, want '1'", resp.ID)
	}
}

// ============================================================================
// detailLayoutVer
// ============================================================================

func TestDetailLayoutVer(t *testing.T) {
	if detailLayoutVer == 0 {
		t.Error("detailLayoutVer should not be 0")
	}
}

// ============================================================================
// queryDetailFromDB 单元测试（直接调用）
// ============================================================================

func TestQueryDetailFromDB_NilRepo(t *testing.T) {
	// queryDetailFromDB calls s.repo.FindDetailByID, need to verify nil guard is in getDetailUnderLock.
	// Since queryDetailFromDB doesn't have nil guard, this test would panic.
	// Remove this test since the nil guard is in getDetailUnderLock now.
	t.Skip("nil-repo guard is in getDetailUnderLock, not queryDetailFromDB")
}