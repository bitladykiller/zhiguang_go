package cache

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/pkg/config"
)

func defaultHotKeyConfig() *config.HotKeyConfig {
	return &config.HotKeyConfig{
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
	}
}

func startTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return rdb, func() { rdb.Close(); mr.Close() }
}

func TestNewHotKeyDetector(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	if d == nil {
		t.Fatal("expected non-nil detector")
	}
	if d.bucketSize != 6*time.Second {
		t.Fatalf("bucketSize = %v, want 6s", d.bucketSize)
	}
	if d.flushInterval != 6*time.Second {
		t.Fatalf("flushInterval = %v, want 6s", d.flushInterval)
	}
}

func TestRecord(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	d.Record("key:a")
	d.Record("key:a")
	d.Record("key:b")

	d.mu.Lock()
	if len(d.buf) != 2 {
		t.Fatalf("buf length = %d, want 2", len(d.buf))
	}
	if d.buf["key:a"] == nil {
		t.Fatal("expected key:a in buf")
	}
	d.mu.Unlock()
}

func TestCurrentBucket(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	b := d.currentBucket()
	if b <= 0 {
		t.Fatalf("currentBucket() = %d, want > 0", b)
	}
}

func TestSnapshotAndReset_NonEmpty(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	d.Record("k1")
	d.Record("k1")
	d.Record("k2")

	snapshot := d.snapshotAndReset()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot length = %d, want 2", len(snapshot))
	}

	d.mu.Lock()
	if len(d.buf) != 0 {
		t.Fatal("expected buf reset to empty")
	}
	d.mu.Unlock()
}

func TestSnapshotAndReset_Empty(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	snapshot := d.snapshotAndReset()
	if snapshot != nil {
		t.Fatal("expected nil for empty snapshot")
	}
}

func TestCalcLevel(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	tests := []struct {
		total int64
		want  HotKeyLevel
	}{
		{0, LevelCold},
		{4, LevelCold},
		{5, LevelLow},
		{19, LevelLow},
		{20, LevelMedium},
		{49, LevelMedium},
		{50, LevelHigh},
		{100, LevelHigh},
	}
	for _, tt := range tests {
		got := d.calcLevel(tt.total)
		if got != tt.want {
			t.Errorf("calcLevel(%d) = %v, want %v", tt.total, got, tt.want)
		}
	}
}

func TestSumBucketsInWindow(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	nowBucket := d.currentBucket()
	values := map[string]string{
		strconv.FormatInt(nowBucket, 10):    "10",
		strconv.FormatInt(nowBucket-1, 10):  "5",
		strconv.FormatInt(nowBucket-9, 10):  "3",
		strconv.FormatInt(nowBucket-10, 10): "2",
		strconv.FormatInt(nowBucket+1, 10):  "99",
	}

	total := d.sumBucketsInWindow(values, nowBucket)
	want := int64(10 + 5 + 3)
	if total != want {
		t.Fatalf("sum = %d, want %d", total, want)
	}
}

func TestSumBucketsInWindow_InvalidField(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	nowBucket := d.currentBucket()
	values := map[string]string{
		"not_a_number":                   "10",
		strconv.FormatInt(nowBucket, 10): "abc",
	}

	total := d.sumBucketsInWindow(values, nowBucket)
	if total != 0 {
		t.Fatalf("sum = %d, want 0", total)
	}
}

func TestTtlForLevel(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	tests := []struct {
		level HotKeyLevel
		want  int
	}{
		{LevelCold, 60},
		{LevelLow, 60 + cfg.ExtendLowSeconds},
		{LevelMedium, 60 + cfg.ExtendMediumSeconds},
		{LevelHigh, 60 + cfg.ExtendHighSeconds},
	}
	for _, tt := range tests {
		got := d.ttlForLevel(60, tt.level)
		if got != tt.want {
			t.Errorf("ttlForLevel(60, %v) = %d, want %d", tt.level, got, tt.want)
		}
	}
}

func TestHotKeyLevel_String(t *testing.T) {
	tests := []struct {
		level HotKeyLevel
		want  string
	}{
		{LevelCold, "cold"},
		{LevelLow, "low"},
		{LevelMedium, "medium"},
		{LevelHigh, "high"},
		{HotKeyLevel(99), "unknown(99)"},
	}
	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.want {
			t.Errorf("HotKeyLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}

func TestRun_StartOnce(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.Run(ctx)
	d.Run(ctx)
}

func TestFlushOnce_EmptySnapshot(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	d.flushOnce(context.Background())
}

func TestGetLevel_FromCache(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	d.levelMu.Lock()
	d.levels["hotkey"] = LevelHigh
	d.levelMu.Unlock()

	level := d.getLevel(context.Background(), "hotkey")
	if level != LevelHigh {
		t.Fatalf("getLevel = %v, want LevelHigh", level)
	}
}

func TestGetLevel_FallbackToRedis(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	level := d.getLevel(context.Background(), "nonexistent")
	if level != LevelCold {
		t.Fatalf("getLevel = %v, want LevelCold", level)
	}
}

func TestTtlForPublic(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	d.levelMu.Lock()
	d.levels["mykey"] = LevelMedium
	d.levelMu.Unlock()

	ttl := d.TtlForPublic(context.Background(), 60, "mykey")
	want := 60 + cfg.ExtendMediumSeconds
	if ttl != want {
		t.Fatalf("TtlForPublic = %d, want %d", ttl, want)
	}
}

func TestReadLevelCache_NotFound(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	_, ok := d.readLevelCache("missing")
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

// ============================================================================
// TtlForPublicBatch：批量热度定级
// ============================================================================

func TestTtlForPublicBatch_MixedSources(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	// k1 命中本地等级缓存；k2 需回落 Redis 标记；k3 冷键。
	d.levelMu.Lock()
	d.levels["k1"] = LevelHigh
	d.levelMu.Unlock()
	if err := rdb.Set(context.Background(), hotkeyActivePrefix+"k2", "1", time.Minute).Err(); err != nil {
		t.Fatalf("seed hot mark: %v", err)
	}

	got := d.TtlForPublicBatch(context.Background(), 60, []string{"k1", "k2", "k3"})
	want := []int{
		60 + cfg.ExtendHighSeconds,
		60 + cfg.ExtendMediumSeconds,
		60,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ttls[%d] = %d, want %d", i, got[i], want[i])
		}
	}

	// 回落 Redis 得到的等级应写回本地缓存，避免下次再查。
	if level, ok := d.readLevelCache("k2"); !ok || level != LevelMedium {
		t.Errorf("k2 level cache = (%v, %v), want (LevelMedium, true)", level, ok)
	}
}

func TestTtlForPublicBatch_EmptyAndAllCached(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	if got := d.TtlForPublicBatch(context.Background(), 60, nil); len(got) != 0 {
		t.Fatalf("empty keys should yield empty result, got %v", got)
	}

	d.levelMu.Lock()
	d.levels["a"] = LevelCold
	d.levels["b"] = LevelLow
	d.levelMu.Unlock()

	got := d.TtlForPublicBatch(context.Background(), 30, []string{"a", "b"})
	if got[0] != 30 || got[1] != 30+cfg.ExtendLowSeconds {
		t.Fatalf("TtlForPublicBatch = %v", got)
	}
}

// TestTtlForPublicBatch_MatchesTtlForPublic 保证批量与单键版本定级结果一致。
func TestTtlForPublicBatch_MatchesTtlForPublic(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	keys := []string{"x", "y", "z"}
	if err := rdb.Set(context.Background(), hotkeyActivePrefix+"y", "1", time.Minute).Err(); err != nil {
		t.Fatalf("seed hot mark: %v", err)
	}

	batch := d.TtlForPublicBatch(context.Background(), 45, keys)
	for i, k := range keys {
		if single := d.TtlForPublic(context.Background(), 45, k); single != batch[i] {
			t.Errorf("key %q: batch=%d single=%d", k, batch[i], single)
		}
	}
}

// ============================================================================
// levels 生命周期：必须随窗口收敛，而非只增不减
// ============================================================================

// TestFlushOnce_ReplacesLevels 验证等级缓存按窗口整体重建。
// 历史缺陷：levels 只写不删，键空间随内容总量线性增长（内存泄漏），
// 且降温的键会永久保留旧等级、TTL 被无谓延长。
func TestFlushOnce_ReplacesLevels(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	// 上一轮遗留的等级
	d.levelMu.Lock()
	d.levels["stale-key"] = LevelHigh
	d.levelMu.Unlock()

	// 本轮只访问 fresh-key
	d.Record("fresh-key")
	d.flushOnce(context.Background())

	if _, ok := d.readLevelCache("stale-key"); ok {
		t.Error("stale-key should be dropped from levels after a flush that did not observe it")
	}
	if _, ok := d.readLevelCache("fresh-key"); !ok {
		t.Error("fresh-key should be present in levels after flush")
	}
}

// TestFlushOnce_EmptySnapshotClearsLevels 验证流量归零后等级缓存被清空。
func TestFlushOnce_EmptySnapshotClearsLevels(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	d.levelMu.Lock()
	d.levels["leftover"] = LevelHigh
	d.levelMu.Unlock()

	d.flushOnce(context.Background()) // buf 为空

	if _, ok := d.readLevelCache("leftover"); ok {
		t.Error("levels should be cleared when a flush observes no traffic")
	}
}

// TestGetLevel_RespectsMaxKeys 验证写回本地缓存时受 maxKeys 约束。
func TestGetLevel_RespectsMaxKeys(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	d.SetMaxKeys(1)

	d.levelMu.Lock()
	d.levels["occupied"] = LevelLow
	d.levelMu.Unlock()

	if err := rdb.Set(context.Background(), hotkeyActivePrefix+"newcomer", "1", time.Minute).Err(); err != nil {
		t.Fatalf("seed hot mark: %v", err)
	}

	// 仍应返回正确等级，但不得写入已满的本地缓存。
	if level := d.getLevel(context.Background(), "newcomer"); level != LevelMedium {
		t.Fatalf("getLevel = %v, want LevelMedium", level)
	}
	if _, ok := d.readLevelCache("newcomer"); ok {
		t.Error("levels is at maxKeys, newcomer must not be cached")
	}
}

// ============================================================================
// staleBucketFields / evictOldestLocked
// ============================================================================

func TestStaleBucketFields(t *testing.T) {
	cfg := defaultHotKeyConfig() // BucketCount=10, bucket=6s, flush=6s → slack=1
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)

	got := d.staleBucketFields(100)
	want := []string{"90", "89"}
	if len(got) != len(want) {
		t.Fatalf("staleBucketFields(100) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("staleBucketFields(100) = %v, want %v", got, want)
		}
	}

	// 窗口尚未滑动满一轮时无需删除任何字段。
	if got := d.staleBucketFields(3); got != nil {
		t.Errorf("staleBucketFields(3) = %v, want nil", got)
	}
}

// TestEvictOldestLocked_PrefersStaleKeys 验证淘汰优先命中已滑出窗口的键。
func TestEvictOldestLocked_PrefersStaleKeys(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	now := d.currentBucket()

	d.mu.Lock()
	// 27 个窗口内的活跃键 + 3 个早已滑出窗口的键 → 共 30 个，淘汰配额为 3。
	// 3 个过期键恰好填满配额，活跃键应当一个都不被波及。
	for i := 0; i < 27; i++ {
		d.buf["live-"+strconv.Itoa(i)] = map[int64]int64{now: 1}
	}
	staleBucket := now - int64(cfg.BucketCount) - 5
	for i := 0; i < 3; i++ {
		d.buf["stale-"+strconv.Itoa(i)] = map[int64]int64{staleBucket: 1}
	}
	d.evictOldestLocked()

	staleLeft := 0
	liveLeft := 0
	for k := range d.buf {
		if strings.HasPrefix(k, "stale-") {
			staleLeft++
		} else {
			liveLeft++
		}
	}
	d.mu.Unlock()

	if staleLeft != 0 {
		t.Errorf("stale keys left = %d, want 0 (stale keys must be evicted first)", staleLeft)
	}
	if liveLeft != 27 {
		t.Errorf("live keys left = %d, want 27 (quota filled by stale keys alone)", liveLeft)
	}
}

// TestEvictOldestLocked_AllLive 验证全部键都在窗口内时仍能腾出约 10% 空间。
func TestEvictOldestLocked_AllLive(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb, nil)
	now := d.currentBucket()

	d.mu.Lock()
	for i := 0; i < 50; i++ {
		d.buf["live-"+strconv.Itoa(i)] = map[int64]int64{now: 1}
	}
	d.evictOldestLocked()
	remaining := len(d.buf)
	d.mu.Unlock()

	if remaining != 45 {
		t.Errorf("remaining keys = %d, want 45 (10%% of 50 evicted)", remaining)
	}
}
