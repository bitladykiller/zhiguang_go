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
//
// 功能：
//   根据传入的 HotKeyConfig 初始化探测器，计算滑动窗口参数：
//   - windowDur:  整个窗口的时间跨度（例如 60 秒）
//   - segDur:     每个时间片的时间跨度（例如 10 秒）
//   - segCount:   窗口被分割成的时间片数（例如 60/10 = 6 片）
//
// 参数：
//   - cfg: 热点键配置，包含窗口大小、时间片大小、各级阈值和 TTL 延长量
//
// 返回值：
//   - *HotKeyDetector: 初始化后的探测器实例
//
// 边界情况：
//   - 如果 WindowSeconds / SegmentSeconds < 1，segCount 被强制设为 1
//     （即只有 1 个时间片，等同于无分段窗口）
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

// Record 为指定键在当前时间片内增加一次命中计数。
//
// 功能：
//  1. 根据当前时间计算所在的时间片索引（segmentIndex）。
//  2. 使用 sync.Map 查找或创建该键的 windowSegments 状态。
//  3. 加锁后调用 advanceWindow 推进滑动窗口（清理已过期的时间片）。
//  4. 在当前时间片的计数上 +1。
//
// 参数：
//   - key: 缓存键，用于区分不同对象的访问频率
//
// 函数调用说明：
//   - d.segments.LoadOrStore(key, &windowSegments{...}):
//     sync.Map 的原子操作：如果 key 存在则返回已有值，否则初始化并存储新值。
//     这避免了每次 Record 都加锁，只在首次访问时创建。
//   - val.(*windowSegments):
//     Go 类型断言，将 sync.Map 返回的 interface{} 转为具体类型。
//
// 调用时机：
//   每次缓存命中（无论 L1 还是 L2）都应调用 Record，以构建准确的频率画像。
//   注意：不要在回源加载时调用 Record，因为回源不代表"热"访问。
//
// 并发安全：
//   Record 使用 sync.Map 做全局的 key 映射管理（无锁读），
//   每个 key 的 windowSegments 内部使用 sync.Mutex 保护，
//   不同 key 之间的计数操作互不阻塞。
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
//
// 功能：
//   在基础 TTL 上按热点键当前热度等级做延长。
//   热度越高，TTL 延长越多，减少数据库回源压力。
//
// 参数：
//   - baseTTL: 缓存的基础过期时间（秒）
//   - key:     需要查询热度的缓存键
//
// 返回值：
//   - int: 调整后的最终 TTL（秒）
//
// 与 TtlForMine 的区别：
//   当前两者使用完全相同的 TTL 延长逻辑（共享 ttlForLevel 实现），
//   第二个参数 _ bool 被忽略。但接口上区分为两个方法，
//   便于将来针对不同 feed 类型制定不同的 TTL 策略。
func (d *HotKeyDetector) TtlForPublic(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key), false)
}

// TtlForMine 返回"我的内容" feed 缓存键根据热度调整后的 TTL。
//
// 功能与 TtlForPublic 相同，在基础 TTL 上按热度等级做延长。
// 分开为两个方法是为了语义清晰：公共 feed 和私有 feed 可能有不同的
// 缓存寿命策略，但当前实现共用同一段延长逻辑。
//
// 参数：
//   - baseTTL: 缓存的基础过期时间（秒）
//   - key:     需要查询热度的缓存键
//
// 返回值：
//   - int: 调整后的最终 TTL（秒）
func (d *HotKeyDetector) TtlForMine(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key), true)
}

// getLevel 根据滑动窗口中的总命中数判断某个键的热度等级。
//
// 功能：
//  1. 从 segments 映射中查找指定 key 的 windowSegments。
//  2. 如果不存在，返回 LevelCold（无热度）。
//  3. 加锁后推进滑动窗口（advanceWindow），确保计数反映最新访问趋势。
//  4. 累加所有时间片的命中数，与配置的各级阈值比较。
//  5. 按阈值从高到低判断（High > Medium > Low），返回匹配的最高等级。
//
// 参数：
//   - key: 缓存键
//
// 返回值：
//   - HotKeyLevel: LevelCold / LevelLow / LevelMedium / LevelHigh
//
// 函数调用说明：
//   - d.segments.Load(key): sync.Map 的读操作，不用加锁。
//     如果 key 不存在，返回 nil 和 false。
//
// 性能说明：
//   getLevel 在每次计算 TTL 时都会调用（对每个 key 的每次读操作），
//   因此需要保持高效。注意不要在 getLevel 内部做 IO 操作。
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

// ttlForLevel 根据热度等级计算出最终的缓存 TTL。
//
// 功能：
//   将热度等级映射为对应的 TTL 延长量，加到 baseTTL 上返回。
//   各级延长量由配置文件中的 ExtendLowSeconds / ExtendMediumSeconds / ExtendHighSeconds 指定。
//
// 参数：
//   - baseTTL: 基础 TTL（秒）
//   - level:   热度等级
//   - _:       保留参数（当前未使用），为 future 扩展预留
//
// 返回值：
//   - int: baseTTL + 对应等级的延长量。Cold 等级不加延长。
//
// 典型配置示例：
//   LOW 等级 → +20 秒
//   MEDIUM 等级 → +60 秒
//   HIGH 等级 → +120 秒
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

// segmentIndex 将时间点映射到滑动窗口中的时间片索引。
//
// 功能：
//   将 Unix 纳秒时间戳除以每个时间片的纳秒数，再对时间片总数取模，
//   得到一个循环索引（ring buffer index），用于定位该时间点在窗口中的位置。
//
// 参数：
//   - t: 需要映射的时间点
//
// 返回值：
//   - int: 时间片索引（0 到 segCount-1）
//
// 设计决策：
//   使用取模运算实现环形缓冲区，这样时间片可以循环复用，
//   无需频繁移动数组元素。索引天然对齐到时间边界，当前时间确定后
//   落入哪个时间片是确定的。
func (d *HotKeyDetector) segmentIndex(t time.Time) int {
	return int(t.UnixNano()/d.segDur.Nanoseconds()) % d.segCount
}

// advanceWindow 清理已经滑出当前滑动窗口范围的历史时间片计数。
//
// 功能：
//  1. 计算当前窗口起点的 segmentIndex（windowStartSeg）。
//  2. 如果从上次更新时间到现在没有超过一个窗口长度，
//     且当前 segmentIndex 与上次一致，则无需清理（快速返回）。
//  3. 遍历所有时间片，将不在当前窗口范围内且不等于当前时间片的计数清零。
//  4. 更新 startTime 为当前时间。
//
// 参数：
//   - ws:         窗口状态（包含各时间片的计数和时间信息）
//   - now:        当前时间
//   - currentSeg: 当前时间片索引
//
// 边界情况：
//   - 首次调用：ws.startTime 初始化为 now，elapsed == 0，跳过清理。
//   - 环形缓冲区回绕：见 isSegmentInWindow 处理跨边界的情况。
//   - 长时间不访问后首次 Record：当前时间远晚于 startTime，
//     所有旧时间片都应清零。
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
//
// 功能：
//   在环形缓冲区中，检查 seg 是否位于 [windowStartSeg, currentSeg] 范围内。
//   需要考虑环形缓冲区索引回绕的情况：
//
//   正常情况（currentSeg >= windowStartSeg）：
//     范围是 [windowStartSeg, currentSeg] 的连续区间。
//
//   回绕情况（currentSeg < windowStartSeg）：
//     窗口跨过了环形缓冲区的边界。
//     范围是 [windowStartSeg, segCount-1] ∪ [0, currentSeg]。
//
// 参数：
//   - seg:            要检查的时间片索引
//   - currentSeg:     当前时间片索引
//   - windowStartSeg: 窗口起点的对应索引
//
// 返回值：
//   - bool: true=仍在窗口内，false=已滑出窗口
//
// 设计决策：
//   使用环形缓冲区处理滑动窗口时，索引回绕是必须处理的边界情况。
//   在回绕时，"大于等于 windowStartSeg" 和 "小于等于 currentSeg"
//   这两个条件变成或者关系，而非与关系。
func (d *HotKeyDetector) isSegmentInWindow(seg, currentSeg, windowStartSeg int) bool {
	if currentSeg >= windowStartSeg {
		return seg >= windowStartSeg && seg <= currentSeg
	}
	// 窗口在环形缓冲区上发生了回绕
	return seg >= windowStartSeg || seg <= currentSeg
}
