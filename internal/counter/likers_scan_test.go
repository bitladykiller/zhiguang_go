package counter

import (
	"context"
	"strconv"
	"testing"

	"go.uber.org/zap"
)

// setLikeBit 在点赞位图中把某个用户的位置为 1。
func setLikeBit(t *testing.T, svc *CounterService, entityType string, entityID, userID uint64) {
	t.Helper()
	chunk := userID / ChunkSize
	offset := userID % ChunkSize
	key := "bm:like:" + entityType + ":" + strconv.FormatUint(entityID, 10) + ":" + strconv.FormatUint(chunk, 10)
	if err := svc.redis.SetBit(context.Background(), key, int64(offset), 1).Err(); err != nil {
		t.Fatalf("setbit: %v", err)
	}
}

func newLikersTestService(t *testing.T) *CounterService {
	t.Helper()
	rdb, shutdown := startTestRedis(t)
	t.Cleanup(shutdown)
	svc := NewCounterService(rdb, nil, nil, nil, "counter-events", nil, zap.NewNop(), nil)
	return svc
}

// TestScanBitmapForLikers_BasicOrder 验证按用户 ID 升序返回。
func TestScanBitmapForLikers_BasicOrder(t *testing.T) {
	svc := newLikersTestService(t)
	for _, uid := range []uint64{3, 7, 100, 65540} { // 65540 落在第 1 个分片
		setLikeBit(t, svc, "knowpost", 1, uid)
	}

	resp, err := svc.scanBitmapForLikers(context.Background(), "knowpost", 1, "like", 0, 10)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := []uint64{3, 7, 100, 65540}
	if len(resp.Items) != len(want) {
		t.Fatalf("got %d items (%+v), want %d", len(resp.Items), resp.Items, len(want))
	}
	for i, w := range want {
		if resp.Items[i].UserID != w {
			t.Errorf("items[%d] = %d, want %d", i, resp.Items[i].UserID, w)
		}
	}
}

// TestScanBitmapForLikers_CursorIsExclusiveAndSeeks 验证游标语义与定位起点。
//
// 游标是「严格大于」语义。原实现每次都从分片 0 开始，把游标过滤放在解出 bit 之后；
// 现在直接由游标定位起始分片与起始位，深翻页不再重扫前缀。
func TestScanBitmapForLikers_CursorIsExclusiveAndSeeks(t *testing.T) {
	svc := newLikersTestService(t)
	for _, uid := range []uint64{3, 7, 100} {
		setLikeBit(t, svc, "knowpost", 2, uid)
	}

	resp, err := svc.scanBitmapForLikers(context.Background(), "knowpost", 2, "like", 7, 10)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].UserID != 100 {
		t.Fatalf("cursor=7 got %+v, want only user 100", resp.Items)
	}
}

// TestScanBitmapForLikers_CursorAtChunkBoundary 验证跨分片边界的游标处理。
func TestScanBitmapForLikers_CursorAtChunkBoundary(t *testing.T) {
	svc := newLikersTestService(t)
	// ChunkSize-1 是分片 0 的最后一位，ChunkSize 是分片 1 的第一位
	setLikeBit(t, svc, "knowpost", 3, ChunkSize-1)
	setLikeBit(t, svc, "knowpost", 3, ChunkSize)

	resp, err := svc.scanBitmapForLikers(context.Background(), "knowpost", 3, "like", ChunkSize-1, 10)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].UserID != ChunkSize {
		t.Fatalf("got %+v, want only user %d", resp.Items, uint64(ChunkSize))
	}
}

// TestScanBitmapForLikers_Pagination 验证分页与 hasMore/cursor 推进。
func TestScanBitmapForLikers_Pagination(t *testing.T) {
	svc := newLikersTestService(t)
	all := []uint64{1, 2, 3, 4, 5}
	for _, uid := range all {
		setLikeBit(t, svc, "knowpost", 4, uid)
	}

	var got []uint64
	cursor := uint64(0)
	for page := 0; page < 5; page++ {
		resp, err := svc.scanBitmapForLikers(context.Background(), "knowpost", 4, "like", cursor, 2)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, it := range resp.Items {
			got = append(got, it.UserID)
		}
		if !resp.HasMore {
			break
		}
		// 回退路径的游标形如 "u:{uid}"，解析后续扫
		cur, err := parseLikersCursor(resp.Cursor)
		if err != nil || cur.byTime {
			t.Fatalf("unexpected cursor %q (err=%v)", resp.Cursor, err)
		}
		cursor = cur.userID
	}

	if len(got) != len(all) {
		t.Fatalf("paged through %v, want %v", got, all)
	}
	for i := range all {
		if got[i] != all[i] {
			t.Fatalf("paged through %v, want %v", got, all)
		}
	}
}

// TestScanBitmapForLikers_SkipsUserZero 验证 0 号位不会被当作用户。
func TestScanBitmapForLikers_SkipsUserZero(t *testing.T) {
	svc := newLikersTestService(t)
	setLikeBit(t, svc, "knowpost", 5, 0)
	setLikeBit(t, svc, "knowpost", 5, 1)

	resp, err := svc.scanBitmapForLikers(context.Background(), "knowpost", 5, "like", 0, 10)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].UserID != 1 {
		t.Fatalf("got %+v, want only user 1 (user id 0 is not a real user)", resp.Items)
	}
}

// TestScanBitmapForLikers_RedisFailureReturnsError 验证 Redis 故障不再被当成空结果。
//
// 原实现对 redis.Nil 和真实错误一律 continue，Redis 挂掉时静默返回「没有点赞者」，
// 调用方无从分辨「确实没人点赞」与「查询失败」。
func TestScanBitmapForLikers_RedisFailureReturnsError(t *testing.T) {
	svc := newLikersTestService(t)
	setLikeBit(t, svc, "knowpost", 6, 1)

	// 关掉 Redis 连接，制造真实故障
	if err := svc.redis.Close(); err != nil {
		t.Fatalf("close redis: %v", err)
	}

	if _, err := svc.scanBitmapForLikers(context.Background(), "knowpost", 6, "like", 0, 10); err == nil {
		t.Fatal("expected an error when Redis is unavailable, got nil (failure must not look like an empty result)")
	}
}

// TestAppendLikersFromChunk_SkipsEmptyBytes 验证整字节跳过不会漏解。
func TestAppendLikersFromChunk_SkipsEmptyBytes(t *testing.T) {
	// 构造：第 0 字节全 0；第 1 字节按 SETBIT 位序（offset 0 = 0x80）
	// 置 offset 8 与 offset 11 → 掩码 0x80 | 0x10 = 0x90 → 用户 8 与 11
	bm := string([]byte{0x00, 0x90})

	items := appendLikersFromChunk(nil, bm, 0, 0, 10)
	if len(items) != 2 || items[0].UserID != 8 || items[1].UserID != 11 {
		t.Fatalf("got %+v, want users 8 and 11", items)
	}
}

// TestAppendLikersFromChunk_RespectsNeed 验证达到需要数量后立即停止。
func TestAppendLikersFromChunk_RespectsNeed(t *testing.T) {
	bm := string([]byte{0xFF}) // 用户 0..7 全部点赞
	items := appendLikersFromChunk(nil, bm, 0, 0, 3)
	if len(items) != 3 {
		t.Fatalf("got %d items, want exactly 3", len(items))
	}
}
