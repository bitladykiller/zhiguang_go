package fanout

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/model"
)

// ============================================================================
// 测试替身
// ============================================================================

// stubFollowers 以游标方式返回固定的粉丝列表。
type stubFollowers struct {
	fans []uint64
	err  error
}

func (s *stubFollowers) FollowersCursor(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
	if s.err != nil {
		return nil, 0, s.err
	}
	// cursor 用作已返回条数（测试内部约定，语义上等价于「按关注时间递减翻页」）。
	start := int(cursor)
	if start >= len(s.fans) {
		return nil, 0, nil
	}
	end := start + limit
	if end > len(s.fans) {
		end = len(s.fans)
	}
	return s.fans[start:end], int64(end), nil
}

// stubFollowing 返回固定的关注列表。
type stubFollowing struct {
	authors []uint64
	err     error
}

func (s *stubFollowing) FollowingCursor(_ context.Context, _ uint64, limit int, cursor int64) ([]uint64, int64, error) {
	if s.err != nil {
		return nil, 0, s.err
	}
	start := int(cursor)
	if start >= len(s.authors) {
		return nil, 0, nil
	}
	end := start + limit
	if end > len(s.authors) {
		end = len(s.authors)
	}
	return s.authors[start:end], int64(end), nil
}

// stubFollowerCount 返回预置的粉丝数；known=false 模拟计数缺失。
type stubFollowerCount struct {
	counts map[uint64]int64
	known  bool
}

func (s *stubFollowerCount) FollowerCount(_ context.Context, userID uint64) (int64, bool) {
	if !s.known {
		return 0, false
	}
	return s.counts[userID], true
}

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	srv := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return srv, rdb
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.CelebrityThreshold = 3
	cfg.FanoutBatchSize = 2
	cfg.TimelineMaxItems = 50
	cfg.AuthorBoxMaxItems = 10
	cfg.FollowBackfillLimit = 5
	return cfg
}

// ============================================================================
// 写路径：普通作者走推
// ============================================================================

// TestFanoutPost_NormalAuthorPushesToFollowers 验证普通作者的帖子进入每个粉丝的收件箱。
func TestFanoutPost_NormalAuthorPushesToFollowers(t *testing.T) {
	srv, rdb := newTestRedis(t)
	fans := []uint64{1001, 1002}
	svc := NewService(rdb, &stubFollowers{fans: fans},
		&stubFollowerCount{counts: map[uint64]int64{7: 2}, known: true}, zap.NewNop(), testConfig())

	event := &model.FanoutEvent{PostID: 42, CreatorID: 7, CreatedAt: 1700000000}
	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}

	for _, fan := range fans {
		members, err := srv.ZMembers(timelineKey(fan))
		if err != nil || len(members) != 1 || members[0] != "42" {
			t.Errorf("fan %d timeline = %v (err=%v), want [42]", fan, members, err)
		}
	}

	// 发件箱必须同时写入：它是拉路与取关清理的数据源。
	box, err := srv.ZMembers(authorBoxKey(7))
	if err != nil || len(box) != 1 || box[0] != "42" {
		t.Errorf("author box = %v (err=%v), want [42]", box, err)
	}
}

// TestFanoutPost_CelebritySkipsPush 验证大 V 不做写扩散，只写发件箱。
//
// 这是混合方案的核心：大 V 的写放大被彻底消除，读者改为在读取时拉取。
func TestFanoutPost_CelebritySkipsPush(t *testing.T) {
	srv, rdb := newTestRedis(t)
	fans := []uint64{1001, 1002, 1003, 1004}
	svc := NewService(rdb, &stubFollowers{fans: fans},
		&stubFollowerCount{counts: map[uint64]int64{7: 9999}, known: true}, zap.NewNop(), testConfig())

	event := &model.FanoutEvent{PostID: 42, CreatorID: 7, CreatedAt: 1700000000}
	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}

	for _, fan := range fans {
		if srv.Exists(timelineKey(fan)) {
			t.Errorf("fan %d timeline should stay empty for a celebrity author", fan)
		}
	}
	if box, _ := srv.ZMembers(authorBoxKey(7)); len(box) != 1 {
		t.Errorf("author box = %v, want the post to be present for pull readers", box)
	}
}

// TestFanoutPost_CrossesThresholdDuringPush 验证「边推边数」的自我修正。
//
// 粉丝计数不可用时不能盲目推送：本用例让计数缺失（known=false），
// 粉丝数（5）超过阈值（3），扩散应在中途停止并把作者补记为大 V。
func TestFanoutPost_CrossesThresholdDuringPush(t *testing.T) {
	srv, rdb := newTestRedis(t)
	fans := []uint64{1, 2, 3, 4, 5}
	svc := NewService(rdb, &stubFollowers{fans: fans},
		&stubFollowerCount{known: false}, zap.NewNop(), testConfig())

	event := &model.FanoutEvent{PostID: 42, CreatorID: 7, CreatedAt: 1700000000}
	if err := svc.FanoutPost(context.Background(), event); err != nil {
		t.Fatalf("FanoutPost: %v", err)
	}

	// 作者应已被补记为大 V
	isMember, err := rdb.SIsMember(context.Background(), celebritySetKey, uint64(7)).Result()
	if err != nil || !isMember {
		t.Fatalf("author should be marked as celebrity after crossing the threshold (member=%v err=%v)", isMember, err)
	}

	// 推送应在越过阈值时停止，靠后的粉丝不应收到
	if srv.Exists(timelineKey(5)) {
		t.Error("push should have stopped before reaching the last follower")
	}

	// 后续帖子直接走拉路
	if err := svc.FanoutPost(context.Background(), &model.FanoutEvent{PostID: 43, CreatorID: 7, CreatedAt: 1700000100}); err != nil {
		t.Fatalf("second FanoutPost: %v", err)
	}
	members, _ := srv.ZMembers(timelineKey(1))
	for _, m := range members {
		if m == "43" {
			t.Error("post 43 should not be pushed once the author is a celebrity")
		}
	}
}

// TestFanoutPost_TrimsTimeline 验证收件箱长度上限生效。
func TestFanoutPost_TrimsTimeline(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	cfg.TimelineMaxItems = 3
	svc := NewService(rdb, &stubFollowers{fans: []uint64{1001}},
		&stubFollowerCount{counts: map[uint64]int64{7: 1}, known: true}, zap.NewNop(), cfg)

	for i := 1; i <= 5; i++ {
		event := &model.FanoutEvent{PostID: uint64(i), CreatorID: 7, CreatedAt: int64(1700000000 + i)}
		if err := svc.FanoutPost(context.Background(), event); err != nil {
			t.Fatalf("FanoutPost %d: %v", i, err)
		}
	}

	members, _ := srv.ZMembers(timelineKey(1001))
	if len(members) != 3 {
		t.Fatalf("timeline has %d entries (%v), want 3", len(members), members)
	}
	// ZMembers 按 score 升序，保留的应是最新三条 3/4/5
	want := []string{"3", "4", "5"}
	for i, w := range want {
		if members[i] != w {
			t.Errorf("timeline = %v, want %v", members, want)
			break
		}
	}
}

// TestFanoutPost_Idempotent 验证重复消费同一事件不产生重复条目。
func TestFanoutPost_Idempotent(t *testing.T) {
	srv, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{fans: []uint64{1001}},
		&stubFollowerCount{counts: map[uint64]int64{7: 1}, known: true}, zap.NewNop(), testConfig())

	event := &model.FanoutEvent{PostID: 42, CreatorID: 7, CreatedAt: 1700000000}
	for i := 0; i < 3; i++ {
		if err := svc.FanoutPost(context.Background(), event); err != nil {
			t.Fatalf("FanoutPost #%d: %v", i, err)
		}
	}
	if members, _ := srv.ZMembers(timelineKey(1001)); len(members) != 1 {
		t.Errorf("timeline = %v, want exactly one entry after repeated delivery", members)
	}
}

// TestFanoutPost_FollowerListErrorPropagates 验证粉丝列表查询失败会返回错误（触发重投）。
func TestFanoutPost_FollowerListErrorPropagates(t *testing.T) {
	_, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{err: errors.New("db down")},
		&stubFollowerCount{counts: map[uint64]int64{7: 1}, known: true}, zap.NewNop(), testConfig())

	err := svc.FanoutPost(context.Background(), &model.FanoutEvent{PostID: 42, CreatorID: 7, CreatedAt: 1})
	if err == nil {
		t.Fatal("expected an error so the message gets redelivered")
	}
}

// ============================================================================
// 读路径：推拉归并
// ============================================================================

// TestHomeTimeline_MergesPushAndPull 验证收件箱与大 V 发件箱按时间归并。
func TestHomeTimeline_MergesPushAndPull(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	const celeb = uint64(200)

	// 推路：收件箱里两条（普通作者推来的）
	srv.ZAdd(timelineKey(reader), 1000, "1")
	srv.ZAdd(timelineKey(reader), 3000, "3")
	// 拉路：大 V 发件箱两条
	srv.ZAdd(authorBoxKey(celeb), 2000, "2")
	srv.ZAdd(authorBoxKey(celeb), 4000, "4")
	srv.SAdd(celebritySetKey, strconv.FormatUint(celeb, 10))

	rd := NewTimelineReader(rdb, &stubFollowing{authors: []uint64{celeb, 999}}, svc, zap.NewNop(), cfg)

	entries, hasMore, err := rd.HomeTimeline(context.Background(), reader, 0, 10)
	if err != nil {
		t.Fatalf("HomeTimeline: %v", err)
	}
	if hasMore {
		t.Error("hasMore should be false when everything fits on one page")
	}

	want := []uint64{4, 3, 2, 1} // 按发布时间倒序
	if len(entries) != len(want) {
		t.Fatalf("got %d entries (%+v), want %d", len(entries), entries, len(want))
	}
	for i, w := range want {
		if entries[i].PostID != w {
			t.Fatalf("merged order = %+v, want %v", entries, want)
		}
	}
}

// TestHomeTimeline_Deduplicates 验证同一帖子同时出现在两路时只保留一条。
//
// 作者跨越大 V 阈值的那条帖子会既推给部分粉丝、又进发件箱，必然出现重复。
func TestHomeTimeline_Deduplicates(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader, celeb = uint64(100), uint64(200)
	srv.ZAdd(timelineKey(reader), 1000, "7")
	srv.ZAdd(authorBoxKey(celeb), 1000, "7")
	srv.SAdd(celebritySetKey, strconv.FormatUint(celeb, 10))

	rd := NewTimelineReader(rdb, &stubFollowing{authors: []uint64{celeb}}, svc, zap.NewNop(), cfg)
	entries, _, err := rd.HomeTimeline(context.Background(), reader, 0, 10)
	if err != nil {
		t.Fatalf("HomeTimeline: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries (%+v), want 1 after dedup", len(entries), entries)
	}
}

// TestHomeTimeline_Pagination 验证分页与 hasMore。
func TestHomeTimeline_Pagination(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	for i := 1; i <= 5; i++ {
		srv.ZAdd(timelineKey(reader), float64(1000*i), strconv.Itoa(i))
	}

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc, zap.NewNop(), cfg)

	page1, hasMore, err := rd.HomeTimeline(context.Background(), reader, 0, 2)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if !hasMore || len(page1) != 2 || page1[0].PostID != 5 || page1[1].PostID != 4 {
		t.Fatalf("page1 = %+v hasMore=%v, want [5 4] true", page1, hasMore)
	}

	page2, hasMore, err := rd.HomeTimeline(context.Background(), reader, 2, 2)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if !hasMore || len(page2) != 2 || page2[0].PostID != 3 {
		t.Fatalf("page2 = %+v hasMore=%v, want [3 2] true", page2, hasMore)
	}

	page3, hasMore, err := rd.HomeTimeline(context.Background(), reader, 4, 2)
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if hasMore || len(page3) != 1 || page3[0].PostID != 1 {
		t.Fatalf("page3 = %+v hasMore=%v, want [1] false", page3, hasMore)
	}
}

// TestHomeTimeline_DepthLimit 验证超过深度上限直接返回空页。
func TestHomeTimeline_DepthLimit(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	cfg.TimelineMaxItems = 3
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	for i := 1; i <= 5; i++ {
		srv.ZAdd(timelineKey(reader), float64(1000*i), strconv.Itoa(i))
	}

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc, zap.NewNop(), cfg)
	entries, hasMore, err := rd.HomeTimeline(context.Background(), reader, 3, 10)
	if err != nil {
		t.Fatalf("HomeTimeline: %v", err)
	}
	if len(entries) != 0 || hasMore {
		t.Errorf("beyond the depth limit: got %+v hasMore=%v, want empty/false", entries, hasMore)
	}
}

// TestHomeTimeline_PullFailureDegradesGracefully 验证拉路失败时仍返回推路结果。
func TestHomeTimeline_PullFailureDegradesGracefully(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	srv.ZAdd(timelineKey(reader), 1000, "1")

	// 关注列表查询失败 → 拉路整体失败
	rd := NewTimelineReader(rdb, &stubFollowing{err: errors.New("relation down")}, svc, zap.NewNop(), cfg)
	entries, _, err := rd.HomeTimeline(context.Background(), reader, 0, 10)
	if err != nil {
		t.Fatalf("pull failure must not fail the whole timeline: %v", err)
	}
	if len(entries) != 1 || entries[0].PostID != 1 {
		t.Errorf("got %+v, want the pushed entry to still be served", entries)
	}
}

// TestHomeTimelinePostIDs 验证适配方法只暴露有序 ID。
func TestHomeTimelinePostIDs(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	srv.ZAdd(timelineKey(reader), 1000, "1")
	srv.ZAdd(timelineKey(reader), 2000, "2")

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc, zap.NewNop(), cfg)
	ids, _, err := rd.HomeTimelinePostIDs(context.Background(), reader, 0, 10)
	if err != nil {
		t.Fatalf("HomeTimelinePostIDs: %v", err)
	}
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 1 {
		t.Errorf("ids = %v, want [2 1]", ids)
	}
}

// ============================================================================
// 关注 / 取关钩子
// ============================================================================

// TestOnFollow_BackfillsRecentPosts 验证关注后立刻能看到对方近期内容。
//
// 写扩散只在发帖那一刻推送给当时的粉丝，新关注者不在其中；
// 没有回填的话，在对方发下一条之前信息流里看不到任何该作者的内容。
func TestOnFollow_BackfillsRecentPosts(t *testing.T) {
	srv, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{counts: map[uint64]int64{200: 1}, known: true}, zap.NewNop(), testConfig())

	const follower, author = uint64(100), uint64(200)
	srv.ZAdd(authorBoxKey(author), 1000, "1")
	srv.ZAdd(authorBoxKey(author), 2000, "2")

	if err := svc.OnFollow(context.Background(), follower, author); err != nil {
		t.Fatalf("OnFollow: %v", err)
	}

	members, _ := srv.ZMembers(timelineKey(follower))
	if len(members) != 2 {
		t.Errorf("timeline = %v, want the author's recent posts backfilled", members)
	}
}

// TestOnFollow_SkipsCelebrity 验证关注大 V 不做回填（读者会走拉路）。
func TestOnFollow_SkipsCelebrity(t *testing.T) {
	srv, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{counts: map[uint64]int64{200: 99999}, known: true}, zap.NewNop(), testConfig())

	const follower, author = uint64(100), uint64(200)
	srv.ZAdd(authorBoxKey(author), 1000, "1")

	if err := svc.OnFollow(context.Background(), follower, author); err != nil {
		t.Fatalf("OnFollow: %v", err)
	}
	if srv.Exists(timelineKey(follower)) {
		t.Error("following a celebrity should not backfill; the reader pulls instead")
	}
}

// TestOnUnfollow_RemovesAuthorPosts 验证取关后不再看到该作者的历史内容。
//
// 写扩散把帖子复制进了收件箱，取关不会让副本自动消失——
// 不清理的话用户会持续看到已取关作者的内容。
func TestOnUnfollow_RemovesAuthorPosts(t *testing.T) {
	srv, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), testConfig())

	const follower, author = uint64(100), uint64(200)
	// 收件箱里既有该作者的帖子，也有别人的
	srv.ZAdd(timelineKey(follower), 1000, "1")
	srv.ZAdd(timelineKey(follower), 2000, "2")
	srv.ZAdd(timelineKey(follower), 3000, "9")
	srv.ZAdd(authorBoxKey(author), 1000, "1")
	srv.ZAdd(authorBoxKey(author), 2000, "2")

	if err := svc.OnUnfollow(context.Background(), follower, author); err != nil {
		t.Fatalf("OnUnfollow: %v", err)
	}

	members, _ := srv.ZMembers(timelineKey(follower))
	if len(members) != 1 || members[0] != "9" {
		t.Errorf("timeline = %v, want only the unrelated post 9 to remain", members)
	}
}

// ============================================================================
// 归并函数
// ============================================================================

func TestMergeTimelines_StableOrderOnEqualScores(t *testing.T) {
	a := []TimelineEntry{{PostID: 1, Score: 100}, {PostID: 3, Score: 100}}
	b := []TimelineEntry{{PostID: 2, Score: 100}}

	merged := mergeTimelines(a, b, 10)
	want := []uint64{3, 2, 1} // score 相同则按 postID 倒序，保证分页稳定
	for i, w := range want {
		if merged[i].PostID != w {
			t.Fatalf("merged = %+v, want order %v", merged, want)
		}
	}
}

func TestMergeTimelines_Empty(t *testing.T) {
	if got := mergeTimelines(nil, nil, 10); got != nil {
		t.Errorf("mergeTimelines(nil, nil) = %+v, want nil", got)
	}
}
