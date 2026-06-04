// Package cache 提供热点键识别（HotKeyDetector）与多级缓存（MultiLevelCache）能力。
//
// HotKeyDetector：使用分段滑动窗口识别高频访问键，当某个键的访问频次超过阈值时，
// 动态延长缓存 TTL，从而降低数据库压力。热度分为三级：
//   - LOW（+20s）：低热度，QPS 略高于背景水平
//   - MEDIUM（+60s）：中等热度，QPS 明显高于背景水平
//   - HIGH（+120s）：高热度，QPS 极高，会显著延长缓存以减少数据库查询
//
// MultiLevelCache：两级缓存（进程内 L1 freecache + 分布式 L2 Redis），
// 通过 singleflight 机制防止缓存击穿。
package cache

import (
	"sync"
	"time"

	"github.com/zhiguang/app/pkg/config"
)

// HotKeyDetector 使用分段滑动窗口识别高频访问键。
// 每个键的命中数会按多个时间片累计，当总命中数超过阈值后，
// 会按热度等级延长该键的缓存 TTL。
//
// WHY：如果只用一个带 TTL 的简单计数器，计数会在过期时突然归零。
// 分段窗口能让老数据逐段衰减，因此“最近访问频率”更平滑也更准确。
type HotKeyDetector struct {
	config    *config.HotKeyConfig
	segments  sync.Map // key -> *windowSegments
	windowDur time.Duration
	segDur    time.Duration
	segCount  int
}

// windowSegments 保存单个键对应的滑动窗口状态。
type windowSegments struct {
	mu        sync.Mutex
	counts    []int     // hit count per segment
	startTime time.Time // time of the first segment
}

// HotKeyLevel 表示键的热度等级。
type HotKeyLevel int

const (
	LevelCold   HotKeyLevel = 0
	LevelLow    HotKeyLevel = 1
	LevelMedium HotKeyLevel = 2
	LevelHigh   HotKeyLevel = 3
)

// NewHotKeyDetector 根据配置创建热点键探测器。
func NewHotKeyDetector(cfg *config.HotKeyConfig) *HotKeyDetector {
	windowDur := time.Duration(cfg.WindowSeconds) * time.Second
	segDur := time.Duration(cfg.SegmentSeconds) * time.Second
	segCount := cfg.WindowSeconds / cfg.SegmentSeconds
	if segCount < 1 {
		segCount = 1
	}

	return &HotKeyDetector{
		config:    cfg,
		windowDur: windowDur,
		segDur:    segDur,
		segCount:  segCount,
	}
}

// Record 在当前时间片内为指定键增加一次命中计数。
// 每次缓存访问（无论是 L1 命中还是 L2 命中）都应调用它来构建频率画像。
func (d *HotKeyDetector) Record(key string) {
	now := time.Now()
	segIdx := d.segmentIndex(now)

	val, _ := d.segments.LoadOrStore(key, &windowSegments{
		counts:    make([]int, d.segCount),
		startTime: now,
	})

	ws := val.(*windowSegments)
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// 如有必要推进窗口并清理过期时间片
	d.advanceWindow(ws, now, segIdx)

	// 累加当前时间片计数
	ws.counts[segIdx]++
}

// TtlForPublic 返回公共 feed 缓存键根据热度调整后的 TTL。
// 它会在基础 TTL 之上按热度等级做延长。
func (d *HotKeyDetector) TtlForPublic(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key), false)
}

// TtlForMine 返回“我的内容” feed 缓存键根据热度调整后的 TTL。
// 该类缓存的基础 TTL 可以不同，但延长逻辑相同。
func (d *HotKeyDetector) TtlForMine(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key), true)
}

// getLevel 根据滑动窗口中的总命中数判断某个键的热度等级。
func (d *HotKeyDetector) getLevel(key string) HotKeyLevel {
	val, ok := d.segments.Load(key)
	if !ok {
		return LevelCold
	}

	ws := val.(*windowSegments)
	ws.mu.Lock()
	defer ws.mu.Unlock()

	d.advanceWindow(ws, time.Now(), d.segmentIndex(time.Now()))

	total := 0
	for _, c := range ws.counts {
		total += c
	}

	switch {
	case total >= d.config.LevelHigh:
		return LevelHigh
	case total >= d.config.LevelMedium:
		return LevelMedium
	case total >= d.config.LevelLow:
		return LevelLow
	default:
		return LevelCold
	}
}

// ttlForLevel 把热度等级映射为最终 TTL。
func (d *HotKeyDetector) ttlForLevel(baseTTL int, level HotKeyLevel, _ bool) int {
	switch level {
	case LevelHigh:
		return baseTTL + d.config.ExtendHighSeconds
	case LevelMedium:
		return baseTTL + d.config.ExtendMediumSeconds
	case LevelLow:
		return baseTTL + d.config.ExtendLowSeconds
	default:
		return baseTTL
	}
}

// segmentIndex 把一个时间点映射到窗口中的某个时间片索引。
func (d *HotKeyDetector) segmentIndex(t time.Time) int {
	return int(t.UnixNano()/d.segDur.Nanoseconds()) % d.segCount
}

// advanceWindow 清理已经滑出窗口范围的时间片。
func (d *HotKeyDetector) advanceWindow(ws *windowSegments, now time.Time, currentSeg int) {
	windowStart := now.Add(-d.windowDur)
	windowStartSeg := d.segmentIndex(windowStart)

	elapsed := now.Sub(ws.startTime)
	if elapsed < d.windowDur && windowStartSeg == currentSeg {
		return
	}

	// 清理窗口外的时间片
	for i := 0; i < d.segCount; i++ {
		isCurrent := (i == currentSeg)
		inWindow := d.isSegmentInWindow(i, currentSeg, windowStartSeg)
		if !isCurrent && !inWindow {
			ws.counts[i] = 0
		}
	}
	ws.startTime = now
}

// isSegmentInWindow 判断某个时间片是否仍处于当前滑动窗口范围内。
func (d *HotKeyDetector) isSegmentInWindow(seg, currentSeg, windowStartSeg int) bool {
	if currentSeg >= windowStartSeg {
		return seg >= windowStartSeg && seg <= currentSeg
	}
	// 窗口在环形缓冲区上发生了回绕
	return seg >= windowStartSeg || seg <= currentSeg
}
