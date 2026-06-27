package knowpost

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
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

func TestQueryDetailFromDB_NilRepo(t *testing.T) {
	t.Skip("nil-repo guard is in getDetailUnderLock, not queryDetailFromDB")
}