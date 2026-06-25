package cache

import (
	"context"
	"strconv"
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

	d := NewHotKeyDetector(cfg, rdb)
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

	d := NewHotKeyDetector(cfg, rdb)

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

	d := NewHotKeyDetector(cfg, rdb)
	b := d.currentBucket()
	if b <= 0 {
		t.Fatalf("currentBucket() = %d, want > 0", b)
	}
}

func TestSnapshotAndReset_NonEmpty(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb)
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

	d := NewHotKeyDetector(cfg, rdb)
	snapshot := d.snapshotAndReset()
	if snapshot != nil {
		t.Fatal("expected nil for empty snapshot")
	}
}

func TestCalcLevel(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb)

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

	d := NewHotKeyDetector(cfg, rdb)

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

	d := NewHotKeyDetector(cfg, rdb)

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

	d := NewHotKeyDetector(cfg, rdb)

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

	d := NewHotKeyDetector(cfg, rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.Run(ctx)
	d.Run(ctx)

	_ = d.startOnce
}

func TestFlushOnce_EmptySnapshot(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb)
	d.flushOnce(context.Background())
}

func TestGetLevel_FromCache(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb)
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

	d := NewHotKeyDetector(cfg, rdb)

	level := d.getLevel(context.Background(), "nonexistent")
	if level != LevelCold {
		t.Fatalf("getLevel = %v, want LevelCold", level)
	}
}

func TestTTLForPublic(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb)
	d.levelMu.Lock()
	d.levels["mykey"] = LevelMedium
	d.levelMu.Unlock()

	ttl := d.TTLForPublic(context.Background(), 60, "mykey")
	want := 60 + cfg.ExtendMediumSeconds
	if ttl != want {
		t.Fatalf("TTLForPublic = %d, want %d", ttl, want)
	}
}

func TestReadLevelCache_NotFound(t *testing.T) {
	cfg := defaultHotKeyConfig()
	rdb, shutdown := startTestRedis(t)
	defer shutdown()

	d := NewHotKeyDetector(cfg, rdb)
	_, ok := d.readLevelCache("missing")
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}
