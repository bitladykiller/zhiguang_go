package cache

import (
	"testing"
	"time"

	"github.com/zhiguang/app/pkg/config"
)

func newTestHotKeyDetector() *HotKeyDetector {
	cfg := &config.HotKeyConfig{
		BucketSizeSeconds:    6,
		BucketCount:          4,
		FlushIntervalSeconds: 6,
		StatTTLSeconds:       120,
		LevelLow:             10,
		LevelMedium:          20,
		LevelHigh:            30,
		ExtendLowSeconds:     20,
		ExtendMediumSeconds:  60,
		ExtendHighSeconds:    120,
		HotMarkTTLSeconds:    60,
	}

	return &HotKeyDetector{
		config:        cfg,
		buf:           make(map[string]map[int64]int64),
		levels:        make(map[string]HotKeyLevel),
		bucketSize:    time.Duration(cfg.BucketSizeSeconds) * time.Second,
		flushInterval: time.Duration(cfg.FlushIntervalSeconds) * time.Second,
		statTTL:       time.Duration(cfg.StatTTLSeconds) * time.Second,
		markTTL:       time.Duration(cfg.HotMarkTTLSeconds) * time.Second,
	}
}

func TestHotKeyDetectorSumBucketsInWindow(t *testing.T) {
	detector := newTestHotKeyDetector()

	values := map[string]string{
		"7":  "100", // 窗口外
		"8":  "5",
		"9":  "6",
		"10": "7",
		"11": "8",
		"12": "9",   // 未来桶，不应计入
		"xx": "100", // 非法字段，忽略
		"13": "yy",  // 非法计数，忽略
	}

	total := detector.sumBucketsInWindow(values, 11)
	if total != 26 {
		t.Fatalf("sumBucketsInWindow() = %d, want 26", total)
	}
}

func TestHotKeyDetectorCalcLevel(t *testing.T) {
	detector := newTestHotKeyDetector()

	tests := []struct {
		name  string
		total int64
		want  HotKeyLevel
	}{
		{name: "cold", total: 9, want: LevelCold},
		{name: "low", total: 10, want: LevelLow},
		{name: "medium", total: 20, want: LevelMedium},
		{name: "high", total: 30, want: LevelHigh},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := detector.calcLevel(tc.total)
			if got != tc.want {
				t.Fatalf("calcLevel(%d) = %v, want %v", tc.total, got, tc.want)
			}
		})
	}
}

func TestHotKeyDetectorTTLForLevel(t *testing.T) {
	detector := newTestHotKeyDetector()

	tests := []struct {
		name  string
		level HotKeyLevel
		want  int
	}{
		{name: "cold", level: LevelCold, want: 60},
		{name: "low", level: LevelLow, want: 80},
		{name: "medium", level: LevelMedium, want: 120},
		{name: "high", level: LevelHigh, want: 180},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := detector.ttlForLevel(60, tc.level)
			if got != tc.want {
				t.Fatalf("ttlForLevel(60, %v) = %d, want %d", tc.level, got, tc.want)
			}
		})
	}
}

func TestHotKeyDetectorTtlForPublicUsesLevelCache(t *testing.T) {
	detector := newTestHotKeyDetector()
	detector.levels["post:1"] = LevelHigh

	got := detector.TtlForPublic(60, "post:1")
	if got != 180 {
		t.Fatalf("TtlForPublic() = %d, want 180", got)
	}
}

func TestHotKeyDetectorSnapshotAndReset(t *testing.T) {
	detector := newTestHotKeyDetector()
	detector.buf["post:1"] = map[int64]int64{1: 2, 2: 3}

	snapshot := detector.snapshotAndReset()
	if len(snapshot) != 1 {
		t.Fatalf("len(snapshot) = %d, want 1", len(snapshot))
	}
	if got := snapshot["post:1"][2]; got != 3 {
		t.Fatalf("snapshot[post:1][2] = %d, want 3", got)
	}
	if len(detector.buf) != 0 {
		t.Fatalf("len(detector.buf) = %d, want 0", len(detector.buf))
	}
	if snapshot == nil {
		t.Fatal("snapshot is nil")
	}
}

func TestHotKeyLevelString(t *testing.T) {
	tests := []struct {
		level HotKeyLevel
		want  string
	}{
		{level: LevelCold, want: "cold"},
		{level: LevelLow, want: "low"},
		{level: LevelMedium, want: "medium"},
		{level: LevelHigh, want: "high"},
		{level: HotKeyLevel(99), want: "unknown(99)"},
	}

	for _, tc := range tests {
		if got := tc.level.String(); got != tc.want {
			t.Fatalf("String() = %q, want %q", got, tc.want)
		}
	}
}
