package counter

import (
	"context"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func redisZ(score float64, member string) redis.Z { return redis.Z{Score: score, Member: member} }

func newLikersTimeService(t *testing.T) *CounterService {
	t.Helper()
	rdb, shutdown := startTestRedis(t)
	t.Cleanup(shutdown)
	return NewCounterService(rdb, nil, nil, nil, "counter-events", nil, zap.NewNop(), nil)
}

// TestGetLikers_TimeOrdered 验证主路径按点赞时间倒序（最近在前）。
//
// 这是本次重构的核心语义修正：点赞真值是 Bitmap，但 Bitmap 只能按 userID 枚举，
// 此前列表因此按 userID 排序——存储选型反向决定了 API 语义。
// 现在 toggle 的 Lua 原子维护 likers ZSet(score=时间)，列表回归产品期望的时间序。
func TestGetLikers_TimeOrdered(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	zkey := likersZSetKey("like", "knowpost", "1")
	// 按点赞时间：uid 5(t=100) → uid 2(t=200) → uid 9(t=300)
	svc.redis.ZAdd(ctx, zkey, redisZ(100, "5"), redisZ(200, "2"), redisZ(300, "9"))

	resp, err := svc.GetLikers(ctx, "knowpost", 1, "like", "", 10)
	if err != nil {
		t.Fatalf("GetLikers: %v", err)
	}
	want := []uint64{9, 2, 5} // 时间倒序，而非 userID 序
	if len(resp.Items) != len(want) {
		t.Fatalf("items=%+v want ids %v", resp.Items, want)
	}
	for i, w := range want {
		if resp.Items[i].UserID != w {
			t.Fatalf("order=%+v want %v", resp.Items, want)
		}
	}
	if resp.Items[0].LikedAt != 300 {
		t.Errorf("LikedAt=%d want 300", resp.Items[0].LikedAt)
	}
}

// TestGetLikers_CursorPagination_TiesNotSkipped 验证复合游标在并列 score 下不丢条目。
//
// 若游标只有 score 且用排除式区间，同一秒点赞的成员会在页边界被整体跳过。
func TestGetLikers_CursorPagination_TiesNotSkipped(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	zkey := likersZSetKey("like", "knowpost", "2")
	// 5 人点赞，其中 3 人同一秒（t=100）：并列 score 恰好横跨页边界
	svc.redis.ZAdd(ctx, zkey,
		redisZ(200, "50"),
		redisZ(100, "31"), redisZ(100, "32"), redisZ(100, "33"),
		redisZ(50, "7"),
	)

	seen := map[uint64]bool{}
	cursor := ""
	for page := 0; page < 6; page++ {
		resp, err := svc.GetLikers(ctx, "knowpost", 2, "like", cursor, 2)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, it := range resp.Items {
			if seen[it.UserID] {
				t.Fatalf("user %d returned twice", it.UserID)
			}
			seen[it.UserID] = true
		}
		if !resp.HasMore {
			break
		}
		cursor = resp.Cursor
	}
	if len(seen) != 5 {
		t.Fatalf("paged %d unique users, want all 5 (ties at page boundary must not be skipped)", len(seen))
	}
}

// TestGetLikers_FallsBackToBitmapWhenIndexMissing 验证索引缺失时回退位图扫描。
func TestGetLikers_FallsBackToBitmapWhenIndexMissing(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	// 只有位图（历史数据），无 likers ZSet
	setLikeBit(t, svc, "knowpost", 3, 7)
	setLikeBit(t, svc, "knowpost", 3, 11)

	resp, err := svc.GetLikers(ctx, "knowpost", 3, "like", "", 10)
	if err != nil {
		t.Fatalf("GetLikers: %v", err)
	}
	if len(resp.Items) != 2 || resp.Items[0].UserID != 7 || resp.Items[1].UserID != 11 {
		t.Fatalf("fallback items=%+v want [7 11] (uid order)", resp.Items)
	}
	if resp.Cursor == "" || resp.Cursor[0] != 'u' {
		t.Errorf("fallback cursor=%q want u:-prefixed", resp.Cursor)
	}
}

// TestToggle_MaintainsTimeIndexAtomically 验证 toggle 同步维护时间序索引。
func TestToggle_MaintainsTimeIndexAtomically(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	if _, err := svc.Like(ctx, 42, "knowpost", "9"); err != nil {
		t.Fatalf("Like: %v", err)
	}
	zkey := likersZSetKey("like", "knowpost", "9")
	if n, _ := svc.redis.ZCard(ctx, zkey).Result(); n != 1 {
		t.Fatalf("zset card=%d want 1 after like", n)
	}
	if _, err := svc.Unlike(ctx, 42, "knowpost", "9"); err != nil {
		t.Fatalf("Unlike: %v", err)
	}
	if n, _ := svc.redis.ZCard(ctx, zkey).Result(); n != 0 {
		t.Fatalf("zset card=%d want 0 after unlike", n)
	}
}

// TestParseLikersCursor 验证游标编解码与历史兼容。
func TestParseLikersCursor(t *testing.T) {
	cases := []struct {
		in     string
		byTime bool
		ts     int64
		uid    uint64
		err    bool
	}{
		{"", false, 0, 0, false},
		{"0", false, 0, 0, false},
		{"t:100:42", true, 100, 42, false},
		{"u:42", false, 0, 42, false},
		{"12345", false, 0, 12345, false}, // 历史纯数字游标
		{"t:abc:1", false, 0, 0, true},
		{"garbage:x", false, 0, 0, true},
	}
	for _, c := range cases {
		got, err := parseLikersCursor(c.in)
		if c.err != (err != nil) {
			t.Errorf("%q: err=%v want err=%v", c.in, err, c.err)
			continue
		}
		if err == nil && (got.byTime != c.byTime || got.ts != c.ts || got.userID != c.uid) {
			t.Errorf("%q: got %+v", c.in, got)
		}
	}
}

// TestScanCache_ShortCacheFallsThrough 验证扫描缓存条数不足时回落全量扫描。
//
// 早期缺陷：缓存里恰好只剩 limit 条被当作「没有更多」，
// 刷新第一页后 hasMore 假阴性、翻页断裂。
func TestScanCache_ShortCacheFallsThrough(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	// 位图有 3 人；预置的扫描缓存只有 2 条（不足 limit+1=3）
	for _, uid := range []uint64{1, 2, 3} {
		setLikeBit(t, svc, "knowpost", 4, uid)
	}
	cacheKey := likersScanCacheKey("knowpost", 4, "like")
	svc.redis.ZAdd(ctx, cacheKey, redisZ(1, "1"), redisZ(2, "2"))

	resp, err := svc.scanBitmapForLikers(ctx, "knowpost", 4, "like", 0, 2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !resp.HasMore {
		t.Fatal("hasMore=false: short cache must fall through to a full scan")
	}
	if len(resp.Items) != 2 || resp.Items[0].UserID != 1 || resp.Items[1].UserID != 2 {
		t.Fatalf("items=%+v", resp.Items)
	}
	_ = strconv.Itoa(0)
}

// TestGetLikers_MassiveTies_DeepPagingDoesNotStall 回归：海量并列 score 深翻页不提前终止。
//
// 离线重建把历史成员统一写成 score=0，制造成百上千的并列。
// score 区间 + 固定并列冗余的旧续页方案在第 k 页要跳过 k×limit 个并列成员，
// 固定冗余覆盖不了，翻几页后整页都是已见成员 → 误判翻尽提前终止。
// rank 快路按全局排名取页，与并列规模无关。
func TestGetLikers_MassiveTies_DeepPagingDoesNotStall(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	zkey := likersZSetKey("like", "knowpost", "77")
	const total = 300
	zs := make([]redis.Z, total)
	for i := 0; i < total; i++ {
		zs[i] = redisZ(0, strconv.Itoa(10000+i)) // 全部并列 score=0
	}
	svc.redis.ZAdd(ctx, zkey, zs...)

	seen := map[uint64]bool{}
	cursor := ""
	for page := 0; page < total; page++ {
		resp, err := svc.GetLikers(ctx, "knowpost", 77, "like", cursor, 20)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, it := range resp.Items {
			if seen[it.UserID] {
				t.Fatalf("user %d returned twice", it.UserID)
			}
			seen[it.UserID] = true
		}
		if !resp.HasMore {
			break
		}
		cursor = resp.Cursor
	}
	if len(seen) != total {
		t.Fatalf("paged %d unique users, want %d (massive ties must not stall deep paging)", len(seen), total)
	}
}

// TestGetLikers_CursorMemberUnliked_FallsBackToScorePath 验证游标成员被 unlike 后仍可续页。
func TestGetLikers_CursorMemberUnliked_FallsBackToScorePath(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	zkey := likersZSetKey("like", "knowpost", "78")
	svc.redis.ZAdd(ctx, zkey, redisZ(300, "3"), redisZ(200, "2"), redisZ(100, "1"))

	resp, err := svc.GetLikers(ctx, "knowpost", 78, "like", "", 1)
	if err != nil || len(resp.Items) != 1 || resp.Items[0].UserID != 3 {
		t.Fatalf("page1: %+v err=%v", resp, err)
	}
	// 游标成员 3 被取消点赞（rank 不可得）
	svc.redis.ZRem(ctx, zkey, "3")

	resp, err = svc.GetLikers(ctx, "knowpost", 78, "like", resp.Cursor, 10)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(resp.Items) != 2 || resp.Items[0].UserID != 2 || resp.Items[1].UserID != 1 {
		t.Fatalf("score-path fallback items=%+v, want [2 1]", resp.Items)
	}
}

// TestRebuildLikersTimeIndex 验证离线重建：位图成员补进 ZSet（score=0），
// 不覆盖 toggle 已写入的真实时间；幂等重跑不重复计数。
func TestRebuildLikersTimeIndex(t *testing.T) {
	svc := newLikersTimeService(t)
	ctx := context.Background()

	// 历史数据：位图有 3 人；其中 uid=2 随后又被 toggle 写入了真实时间
	for _, uid := range []uint64{1, 2, 3} {
		setLikeBit(t, svc, "knowpost", 9, uid)
	}
	zkey := likersZSetKey("like", "knowpost", "9")
	svc.redis.ZAdd(ctx, zkey, redisZ(1700000000, "2"))

	added, err := svc.RebuildLikersTimeIndex(ctx, "knowpost", 9, "like")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if added != 2 { // uid 1、3；uid 2 已存在（NX 不覆盖）
		t.Fatalf("added=%d want 2", added)
	}
	if sc, _ := svc.redis.ZScore(ctx, zkey, "2").Result(); int64(sc) != 1700000000 {
		t.Fatalf("real timestamp overwritten: %v", sc)
	}
	if sc, _ := svc.redis.ZScore(ctx, zkey, "1").Result(); sc != 0 {
		t.Fatalf("historic member score=%v want 0", sc)
	}

	// 幂等重跑
	added, err = svc.RebuildLikersTimeIndex(ctx, "knowpost", 9, "like")
	if err != nil || added != 0 {
		t.Fatalf("rerun added=%d err=%v, want 0/nil", added, err)
	}

	// 重建后走时间序主路径：真实时间在前，历史(0)在后
	resp, err := svc.GetLikers(ctx, "knowpost", 9, "like", "", 10)
	if err != nil || len(resp.Items) != 3 || resp.Items[0].UserID != 2 {
		t.Fatalf("post-rebuild list=%+v err=%v", resp, err)
	}
}
