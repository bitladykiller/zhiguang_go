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
	"github.com/zhiguang/app/internal/outbox"
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

	rd := NewTimelineReader(rdb, &stubFollowing{authors: []uint64{celeb, 999}}, svc.Celebrities(), zap.NewNop(), cfg)

	entries, _, hasMore, err := rd.HomeTimeline(context.Background(), reader, "", 10)
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

	rd := NewTimelineReader(rdb, &stubFollowing{authors: []uint64{celeb}}, svc.Celebrities(), zap.NewNop(), cfg)
	entries, _, _, err := rd.HomeTimeline(context.Background(), reader, "", 10)
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

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc.Celebrities(), zap.NewNop(), cfg)

	page1, cur1, hasMore, err := rd.HomeTimeline(context.Background(), reader, "", 2)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if !hasMore || len(page1) != 2 || page1[0].PostID != 5 || page1[1].PostID != 4 {
		t.Fatalf("page1 = %+v hasMore=%v, want [5 4] true", page1, hasMore)
	}

	page2, cur2, hasMore, err := rd.HomeTimeline(context.Background(), reader, cur1, 2)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if !hasMore || len(page2) != 2 || page2[0].PostID != 3 {
		t.Fatalf("page2 = %+v hasMore=%v, want [3 2] true", page2, hasMore)
	}

	page3, _, hasMore, err := rd.HomeTimeline(context.Background(), reader, cur2, 2)
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

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc.Celebrities(), zap.NewNop(), cfg)
	// 逐页翻到尽头：可见条目受收件箱保留长度约束
	cursor := ""
	total := 0
	for i := 0; i < 10; i++ {
		entries, next, hasMore, err := rd.HomeTimeline(context.Background(), reader, cursor, 2)
		if err != nil {
			t.Fatalf("HomeTimeline: %v", err)
		}
		total += len(entries)
		if !hasMore {
			break
		}
		cursor = next
	}
	if total != 5 {
		t.Errorf("paged total=%d, want 5", total)
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
	rd := NewTimelineReader(rdb, &stubFollowing{err: errors.New("relation down")}, svc.Celebrities(), zap.NewNop(), cfg)
	entries, _, _, err := rd.HomeTimeline(context.Background(), reader, "", 10)
	if err != nil {
		t.Fatalf("pull failure must not fail the whole timeline: %v", err)
	}
	if len(entries) != 1 || entries[0].PostID != 1 {
		t.Errorf("got %+v, want the pushed entry to still be served", entries)
	}
}

// TestHomeTimelinePage 验证适配方法暴露有序 ID 与游标。
func TestHomeTimelinePage(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	srv.ZAdd(timelineKey(reader), 1000, "1")
	srv.ZAdd(timelineKey(reader), 2000, "2")

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc.Celebrities(), zap.NewNop(), cfg)
	ids, _, _, err := rd.HomeTimelinePage(context.Background(), reader, "", 10)
	if err != nil {
		t.Fatalf("HomeTimelinePage: %v", err)
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

// ============================================================================
// 事件消费：从 outbox 行到扩散动作
// ============================================================================

func newHandlerFixture(t *testing.T) (*miniredis.Miniredis, *publishRowHandler) {
	t.Helper()
	srv, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{fans: []uint64{1001}},
		&stubFollowerCount{counts: map[uint64]int64{7: 1}, known: true}, zap.NewNop(), testConfig())
	return srv, &publishRowHandler{service: svc, logger: zap.NewNop()}
}

// TestHandleRow_TriggersFanoutOnPublish 验证发布事件被正确解析并触发扩散。
func TestHandleRow_TriggersFanoutOnPublish(t *testing.T) {
	srv, h := newHandlerFixture(t)

	row := outbox.Row{
		AggregateType: "knowpost",
		AggregateID:   "42",
		Type:          knowPostPublishedType,
		Payload: []byte(`{"entity":"knowpost","type":"KnowPostPublished",
			"id":42,"creator_id":7,"published_at":1700000000}`),
	}
	if err := h.HandleRow(context.Background(), row); err != nil {
		t.Fatalf("HandleRow: %v", err)
	}

	if members, _ := srv.ZMembers(timelineKey(1001)); len(members) != 1 || members[0] != "42" {
		t.Errorf("timeline = %v, want [42]", members)
	}
	if box, _ := srv.ZMembers(authorBoxKey(7)); len(box) != 1 {
		t.Errorf("author box = %v, want the post to be recorded", box)
	}
}

// TestHandleRow_IgnoresOtherEventTypes 验证非发布事件被放行。
//
// canal-outbox 是多消费者共享主题，里面混有关系、搜索等各类事件。
func TestHandleRow_IgnoresOtherEventTypes(t *testing.T) {
	srv, h := newHandlerFixture(t)

	row := outbox.Row{
		Type:    "FollowCreated",
		Payload: []byte(`{"event_type":"FollowCreated","from_user_id":1,"to_user_id":2}`),
	}
	if err := h.HandleRow(context.Background(), row); err != nil {
		t.Fatalf("HandleRow should pass non-publish events through: %v", err)
	}
	if srv.Exists(timelineKey(1001)) {
		t.Error("a non-publish event must not trigger fanout")
	}
}

// TestHandleRow_MalformedPayloadIsSkipped 验证载荷损坏时放行而非卡住分区。
func TestHandleRow_MalformedPayloadIsSkipped(t *testing.T) {
	_, h := newHandlerFixture(t)

	for _, payload := range []string{
		`{not json`,                    // 解析失败
		`{"type":"KnowPostPublished"}`, // 缺 id 与 creator_id
		`{"id":42,"creator_id":0}`,     // creator_id 为 0
	} {
		row := outbox.Row{Type: knowPostPublishedType, Payload: []byte(payload)}
		if err := h.HandleRow(context.Background(), row); err != nil {
			t.Errorf("payload %q: got error %v, want nil (retrying cannot fix a broken payload)", payload, err)
		}
	}
}

// TestHandleRow_MissingPublishedAtIsSkipped 验证无发布时间的历史事件被跳过。
//
// 新消费者组默认可能回放主题历史；若给无时间戳的旧事件补“当前时间”，
// 陈年旧帖会被当作新内容刷满收件箱顶部。历史内容不该再被扩散——跳过即正确行为。
func TestHandleRow_MissingPublishedAtIsSkipped(t *testing.T) {
	srv, h := newHandlerFixture(t)

	row := outbox.Row{
		Type:    knowPostPublishedType,
		Payload: []byte(`{"id":42,"creator_id":7}`),
	}
	if err := h.HandleRow(context.Background(), row); err != nil {
		t.Fatalf("HandleRow should skip silently: %v", err)
	}
	if srv.Exists(timelineKey(1001)) {
		t.Error("pre-feature event must not be fanned out")
	}
	if srv.Exists(authorBoxKey(7)) {
		t.Error("pre-feature event must not enter the author box either")
	}
}

// TestRemovePost 验证删除知文时清理发件箱。
func TestRemovePost(t *testing.T) {
	srv, rdb := newTestRedis(t)
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), testConfig())

	srv.ZAdd(authorBoxKey(7), 1000, "42")
	srv.ZAdd(authorBoxKey(7), 2000, "43")

	if err := svc.RemovePost(context.Background(), 7, 42); err != nil {
		t.Fatalf("RemovePost: %v", err)
	}
	members, _ := srv.ZMembers(authorBoxKey(7))
	if len(members) != 1 || members[0] != "43" {
		t.Errorf("author box = %v, want only [43]", members)
	}
}

// ============================================================================
// CelebrityRegistry：惰性降级
// ============================================================================

// TestCelebrityRegistry_LazyDemotion 验证掉粉的大 V 在下一次判定时自动回到推模式。
//
// 名单此前只进不出：掉粉后永远走拉。惰性降级在 IsCelebrity 内完成，
// 无需任何离线任务；滞回线（threshold×0.8）防止阈值附近反复横跳。
func TestCelebrityRegistry_LazyDemotion(t *testing.T) {
	_, rdb := newTestRedis(t)
	counts := &stubFollowerCount{counts: map[uint64]int64{7: 100}, known: true}
	reg := NewCelebrityRegistry(rdb, counts, 100, zap.NewNop())
	ctx := context.Background()

	reg.Mark(ctx, 7)
	if celeb, known := reg.IsCelebrity(ctx, 7); !known || !celeb {
		t.Fatalf("at threshold: celeb=%v known=%v, want true/true", celeb, known)
	}

	// 掉到滞回线以内（80~100）：仍视为大 V，避免反复横跳
	counts.counts[7] = 85
	if celeb, _ := reg.IsCelebrity(ctx, 7); !celeb {
		t.Fatal("within hysteresis band: should remain celebrity")
	}

	// 跌破滞回线：当场降级
	counts.counts[7] = 50
	if celeb, known := reg.IsCelebrity(ctx, 7); celeb || !known {
		t.Fatalf("below demote line: celeb=%v known=%v, want false/true", celeb, known)
	}
	if member, _ := rdb.SIsMember(ctx, celebritySetKey, uint64(7)).Result(); member {
		t.Fatal("author should be removed from the celebrity set after demotion")
	}
}

// ============================================================================
// 消费者：删除 / 可见性收紧 → 清发件箱
// ============================================================================

func TestHandleRow_DeleteCleansAuthorBox(t *testing.T) {
	srv, h := newHandlerFixture(t)
	srv.ZAdd(authorBoxKey(7), 1000, "42")
	srv.ZAdd(authorBoxKey(7), 2000, "43")

	row := outbox.Row{
		Type:    knowPostDeletedType,
		Payload: []byte(`{"id":42,"creator_id":7}`),
	}
	if err := h.HandleRow(context.Background(), row); err != nil {
		t.Fatalf("HandleRow: %v", err)
	}
	members, _ := srv.ZMembers(authorBoxKey(7))
	if len(members) != 1 || members[0] != "43" {
		t.Fatalf("author box = %v, want only [43] after delete", members)
	}
}

func TestHandleRow_VisibilityTightenedCleansAuthorBox(t *testing.T) {
	srv, h := newHandlerFixture(t)
	srv.ZAdd(authorBoxKey(7), 1000, "42")

	// 转 private：拉路不应再分发
	row := outbox.Row{
		Type:    knowPostVisibilityUpdatedType,
		Payload: []byte(`{"id":42,"creator_id":7,"visible":"private"}`),
	}
	if err := h.HandleRow(context.Background(), row); err != nil {
		t.Fatalf("HandleRow: %v", err)
	}
	if members, _ := srv.ZMembers(authorBoxKey(7)); len(members) != 0 {
		t.Fatalf("author box = %v, want empty after tightening", members)
	}

	// 转 followers：仍可被信息流分发，不清理
	srv.ZAdd(authorBoxKey(7), 1000, "44")
	row.Payload = []byte(`{"id":44,"creator_id":7,"visible":"followers"}`)
	if err := h.HandleRow(context.Background(), row); err != nil {
		t.Fatalf("HandleRow: %v", err)
	}
	if members, _ := srv.ZMembers(authorBoxKey(7)); len(members) != 1 {
		t.Fatalf("author box = %v, want [44] kept for followers visibility", members)
	}
}

// ============================================================================
// 读路径：关注列表全量扫描
// ============================================================================

// TestFollowedCelebrities_ScansBeyondFirstPage 验证大 V 识别覆盖整个关注列表。
//
// 早期缺陷：循环在收集到 MaxPullAuthors 个「关注对象」后即停止，
// 实际只检查最近关注的 500 人——早年关注的大 V 从首页凭空消失。
func TestFollowedCelebrities_ScansBeyondFirstPage(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	cfg.MaxPullAuthors = 10

	// 关注 1200 人；唯一的大 V 排在列表最末（最早关注）
	authors := make([]uint64, 1200)
	for i := range authors {
		authors[i] = uint64(10000 + i)
	}
	celebID := authors[len(authors)-1]
	srv.SAdd(celebritySetKey, strconv.FormatUint(celebID, 10))

	reg := NewCelebrityRegistry(rdb, nil, cfg.CelebrityThreshold, zap.NewNop())
	rd := NewTimelineReader(rdb, &stubFollowing{authors: authors}, reg, zap.NewNop(), cfg)

	celebs, err := rd.followedCelebrities(context.Background(), 1)
	if err != nil {
		t.Fatalf("followedCelebrities: %v", err)
	}
	if len(celebs) != 1 || celebs[0] != celebID {
		t.Fatalf("celebs = %v, want [%d] found beyond the first 500 followings", celebs, celebID)
	}
}

// TestHomeTimeline_CursorTiesNotSkipped 验证并列发布时间横跨页边界时不丢条目。
func TestHomeTimeline_CursorTiesNotSkipped(t *testing.T) {
	srv, rdb := newTestRedis(t)
	cfg := testConfig()
	svc := NewService(rdb, &stubFollowers{}, &stubFollowerCount{known: false}, zap.NewNop(), cfg)

	const reader = uint64(100)
	// 同一秒发布 5 条 + 前后各一条
	srv.ZAdd(timelineKey(reader), 300, "90")
	for i := 1; i <= 5; i++ {
		srv.ZAdd(timelineKey(reader), 200, strconv.Itoa(i))
	}
	srv.ZAdd(timelineKey(reader), 100, "80")

	rd := NewTimelineReader(rdb, &stubFollowing{}, svc.Celebrities(), zap.NewNop(), cfg)

	seen := map[uint64]bool{}
	cursor := ""
	for page := 0; page < 10; page++ {
		entries, next, hasMore, err := rd.HomeTimeline(context.Background(), reader, cursor, 2)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, e := range entries {
			if seen[e.PostID] {
				t.Fatalf("post %d returned twice", e.PostID)
			}
			seen[e.PostID] = true
		}
		if !hasMore {
			break
		}
		cursor = next
	}
	if len(seen) != 7 {
		t.Fatalf("paged %d unique posts, want 7 (ties across page boundaries must survive)", len(seen))
	}
}
