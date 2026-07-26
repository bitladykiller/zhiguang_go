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
