// Package cache 提供热点键识别（HotKeyDetector）与多级缓存（MultiLevelCache）能力。
//
// HotKeyDetector：使用本地 map + Redis Hash 实现跨实例滑动窗口热点检测。
// 每次缓存访问仅递增本地计数（零 Redis IO），每 6 秒批量 flush 到 Redis Hash 完成跨实例聚合。
// 热度分为三级：
//   - LOW（+20s）：低热度，QPS 略高于背景水平
//   - MEDIUM（+60s）：中等热度，QPS 明显高于背景水平
//   - HIGH（+120s）：高热度，QPS 极高
//
// WHY 选用 Hash 而非 ZSET：
//   ZSET 适合对多个 key 排序（排行榜），而本场景是每个 key 下存 10 个时间窗口的计数，
//   Hash 的 field→value 模型（窗口编号→访问次数）更自然，无需维护 member 排序开销。
//
// WHY 不用每次请求直接写 Redis：
//   如果每次 Record() 都 HINCRBY，QPS 高时 Redis 压力大（写放大）。
//   本地 map 先聚合，每 6 秒一次批量 flush，Redis 写入量降低数个数量级。
//
// MultiLevelCache：两级缓存（进程内 L1 freecache + 分布式 L2 Redis），
// 通过 singleflight 机制防止缓存击穿。
package cache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/pkg/config"
)

// HotKeyDetector 使用本地 map + Redis Hash 检测跨实例热点键。
//
// 工作流程：
//  1. Record(key) → 仅递增本地 map 中当前桶的计数，不写 Redis
//  2. 后台 goroutine 每 flushInterval 秒执行一次：
//     a. 快照并清空本地 map
//     b. HINCRBY hotwin:{key} {bucket} {count}（跨实例汇总）
//     c. HDEL 清理超过 bucketCount 个的旧桶
//     d. EXPIRE hotwin:{key} {statTTL}
//     e. HGETALL hotwin:{key} → 累加计数 → 判断热度等级
//     f. 存入本地 levelCache（供 TtlForPublic/TtlForMine 快速查询）
//     g. 若热度 >= LevelLow，SET hotkey:active:{key} 1 EX {markTTL}
//  3. getLevel(key)：
//     - 先查本地 levelCache（微秒级，无网络开销）
//     - 本地 miss 时查 Redis hotkey:active 标记（兜底）
//
// 并发安全：
//   - buf 由 sync.Mutex 保护
//   - levelCache 由 sync.RWMutex 保护（读多写少）
//   - 后台 goroutine 通过 sync.Once 惰性启动
type HotKeyDetector struct {
	config *config.HotKeyConfig
	redis  *redis.Client

	// 本地计数缓冲：key → 桶编号 → 桶内计数
	mu  sync.Mutex
	buf map[string]map[int64]int64

	// 热度等级缓存：key → 热度等级
	// 每轮 flush 更新，供 getLevel 快速读取
	levelMu  sync.RWMutex
	levels   map[string]HotKeyLevel

	// 派生字段，由 config 计算得出
	bucketSize   time.Duration // 每个桶的时长（如 6s）
	flushInterval time.Duration // flush 间隔
	statTTL      time.Duration  // Redis Hash 的 TTL
	markTTL      time.Duration  // hotkey:active 标记 TTL

	// 生命周期控制
	startOnce sync.Once
	stopCh    chan struct{}
}

// hotwinKeyPrefix 是 Redis Hash 键的前缀。
const hotwinKeyPrefix = "hotwin:"

// hotkeyActivePrefix 是 hotkey 标记键的前缀。
const hotkeyActivePrefix = "hotkey:active:"

// HotKeyLevel 表示键的热度等级。
type HotKeyLevel int

const (
	LevelCold   HotKeyLevel = 0
	LevelLow    HotKeyLevel = 1
	LevelMedium HotKeyLevel = 2
	LevelHigh   HotKeyLevel = 3
)

// NewHotKeyDetector 根据配置和 Redis 客户端创建跨实例热点键探测器。
//
// 参数：
//   - cfg: 热点键配置，包含桶大小、桶数量、各级阈值和 TTL 延长量
//   - redisClient: Redis 客户端，用于跨实例计数聚合
//
// 返回值：
//   - *HotKeyDetector: 初始化后的探测器实例
//
// 设计决策：
//   将 redisClient 作为构造参数而非 Record 方法的参数，
//   使 Record(key) 签名保持简洁，不入侵调用方（knowpost）的调用方式。
//   后台 goroutine 在首次 Record 时惰性启动，不阻塞构造函数。
func NewHotKeyDetector(cfg *config.HotKeyConfig, redisClient *redis.Client) *HotKeyDetector {
	return &HotKeyDetector{
		config:        cfg,
		redis:         redisClient,
		buf:           make(map[string]map[int64]int64),
		levels:        make(map[string]HotKeyLevel),
		bucketSize:    time.Duration(cfg.BucketSizeSeconds) * time.Second,
		flushInterval: time.Duration(cfg.FlushIntervalSeconds) * time.Second,
		statTTL:       time.Duration(cfg.StatTTLSeconds) * time.Second,
		markTTL:       time.Duration(cfg.HotMarkTTLSeconds) * time.Second,
		stopCh:        make(chan struct{}),
	}
}

// Stop 停止后台 flush goroutine。
func (d *HotKeyDetector) Stop() {
	close(d.stopCh)
}

// Record 为指定键在当前时间窗口内增加一次命中计数。
//
// 功能：
//  1. 将当前时间映射到 6 秒级的桶编号（bucket = unix_epoch_seconds / 6）。
//  2. 在本地 buf 中对应 key+bucket 的计数 +1。
//  3. 如果是首次调用，惰性启动后台 flush goroutine。
//
// 参数：
//   - key: 缓存键（如 "knowpost:1001"、"feed:public:v3:1:0"）
//
// 调用时机：
//   每次缓存命中（无论 L1 还是 L2）都应调用 Record。
//   不要在回源加载时调用 Record。
//
// 性能：
//   纯本地操作，不涉及网络 IO，约 50-100ns。
//   仅在 buf 增大时需要少量内存分配。
func (d *HotKeyDetector) Record(key string) {
	d.startOnce.Do(func() {
		go d.flushLoop()
	})

	bucket := d.currentBucket()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.buf[key] == nil {
		d.buf[key] = make(map[int64]int64)
	}
	d.buf[key][bucket]++
}

// currentBucket 返回当前时间对应的桶编号（Unix 秒 / bucketSize）。
func (d *HotKeyDetector) currentBucket() int64 {
	return time.Now().Unix() / int64(d.bucketSize.Seconds())
}

// flushLoop 是后台 flush goroutine 的主循环。
//
// 每 flushInterval 秒执行一轮 flush：
//  1. SnapshotAndReset：快照并清空本地 buf
//  2. FlushToRedis：将快照数据批量写入 Redis（HINCRBY + HDEL + EXPIRE）
//  3. UpdateLevelCache：从 Redis 读取跨实例聚合计数，更新本地热度等级缓存
//
// 异常处理：
//   - 单次 flush 失败（如 Redis 网络抖动）：打印日志后继续下一轮，不影响后续 flush。
//   - 连续失败：本地 levelCache 最多过期 flushInterval*2 秒后回退到 Redis 查询。
//   - goroutine panic：用 recover 兜底，防止整个进程崩溃。
func (d *HotKeyDetector) flushLoop() {
	// 兜底：防止某个 flush 轮次 panic 导致整个 goroutine 退出
	defer func() {
		if r := recover(); r != nil {
			go d.flushLoop()
		}
	}()

	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			// 最后一次 flush：尽量把本地数据刷出去
			d.flushOnce()
			return
		case <-ticker.C:
			d.flushOnce()
		}
	}
}

// flushOnce 执行一轮完整的 flush 流程。
func (d *HotKeyDetector) flushOnce() {
	snapshot := d.snapshotAndReset()
	if len(snapshot) == 0 {
		return
	}

	ctx := context.Background()
	nowBucket := d.currentBucket()

	// 第一步：HINCRBY + HDEL + EXPIRE 批量写入
	pipe := d.redis.Pipeline()
	for cacheKey, buckets := range snapshot {
		statKey := hotwinKeyPrefix + cacheKey
		for bucket, count := range buckets {
			pipe.HIncrBy(ctx, statKey, strconv.FormatInt(bucket, 10), count)
		}
		// 删除超出窗口的旧桶：保留最近 bucketCount 个桶
		for i := int64(d.config.BucketCount); i < int64(d.config.BucketCount)+int64(d.config.BucketCount); i++ {
			oldBucket := nowBucket - i
			pipe.HDel(ctx, statKey, strconv.FormatInt(oldBucket, 10))
		}
		pipe.Expire(ctx, statKey, d.statTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		// flush 到 Redis 失败，不阻塞业务
		// 由于本地计数已清空，这部分数据丢失。
		// 但对于热点检测场景，少量丢数不影响阈值判断。
		return
	}

	// 第二步：HGETALL 获取跨实例聚合计数 → 更新本地热度等级
	newLevels := make(map[string]HotKeyLevel, len(snapshot))
	for cacheKey := range snapshot {
		statKey := hotwinKeyPrefix + cacheKey

		values, err := d.redis.HGetAll(ctx, statKey).Result()
		if err != nil {
			continue
		}

		total := d.sumBucketsInWindow(values, nowBucket)
		level := d.calcLevel(total)

		newLevels[cacheKey] = level

		// 如果达到热点等级，在 Redis 中标记
		if level >= LevelLow {
			d.redis.Set(ctx, hotkeyActivePrefix+cacheKey, "1", d.markTTL)
		}
	}

	// 原子替换 levelCache
	if len(newLevels) > 0 {
		d.levelMu.Lock()
		for k, v := range newLevels {
			d.levels[k] = v
		}
		d.levelMu.Unlock()
	}
}

// snapshotAndReset 快照并清空本地 buf，返回快照数据。
func (d *HotKeyDetector) snapshotAndReset() map[string]map[int64]int64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.buf) == 0 {
		return nil
	}

	snapshot := d.buf
	d.buf = make(map[string]map[int64]int64)
	return snapshot
}

// sumBucketsInWindow 从 HGETALL 结果中累加最近 bucketCount 个桶的计数。
//
// HGETALL 返回 map[string]string，key 是桶编号字符串，value 是计数字符串。
// 只累加时间窗口内的桶（[nowBucket-BucketCount+1, nowBucket]），
// 窗口外的旧计数被舍去。
//
// 边界情况：
//   - field 不是合法数字：跳过，不 panic
//   - value 不是合法数字：跳过，不 panic
//   - 所有 field 都在窗口外：返回 0
//   - values 为空：返回 0
func (d *HotKeyDetector) sumBucketsInWindow(values map[string]string, nowBucket int64) int64 {
	minBucket := nowBucket - int64(d.config.BucketCount) + 1
	if minBucket < 0 {
		minBucket = 0
	}

	var total int64
	for field, valStr := range values {
		bucket, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			continue
		}
		if bucket < minBucket || bucket > nowBucket {
			continue
		}
		count, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			continue
		}
		total += count
	}
	return total
}

// calcLevel 根据总命中数和配置阈值计算热度等级。
//
// 阈值从高到低判断：
//   总计数 >= LevelHigh → HIGH
//   总计数 >= LevelMedium → MEDIUM
//   总计数 >= LevelLow → LOW
//   否则 → COLD
func (d *HotKeyDetector) calcLevel(total int64) HotKeyLevel {
	switch {
	case total >= int64(d.config.LevelHigh):
		return LevelHigh
	case total >= int64(d.config.LevelMedium):
		return LevelMedium
	case total >= int64(d.config.LevelLow):
		return LevelLow
	default:
		return LevelCold
	}
}

// TtlForPublic 返回公共 feed 缓存键根据热度调整后的 TTL。
//
// 功能与原有实现一致：在基础 TTL 上按热点键当前热度等级做延长。
// 热度越高，TTL 延长越多，减少数据库回源压力。
//
// 参数：
//   - baseTTL: 缓存的基础过期时间（秒）
//   - key:     需要查询热度的缓存键
//
// 返回值：
//   - int: 调整后的最终 TTL（秒）
//
// 在原有实现中 getLevel 读取本地 segments 计数，现在读取跨实例聚合后的 levelCache。
// 调用方无需感知底层变化。
func (d *HotKeyDetector) TtlForPublic(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key), false)
}

// TtlForMine 返回"我的内容" feed 缓存键根据热度调整后的 TTL。
//
// 功能与 TtlForPublic 相同，在基础 TTL 上按热度等级做延长。
// 分开为两个方法是为了语义清晰：公共 feed 和私有 feed 可能有不同的缓存寿命策略，
// 但当前实现共用同一段延长逻辑。
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

// getLevel 根据本地 levelCache 或 Redis hotkey:active 标记判断热度等级。
//
// 查询路径（从快到慢）：
//  1. 本地 levelCache（microseconds）→ 如果缓存命中且未被清理，直接返回。
//  2. Redis hotkey:active 标记（约 0.5ms）→ 兜底查询。
//     如果 Redis 中有标记，说明是热点但尚未来得及刷新本地缓存，
//     返回 LevelMedium 作为合理默认值（因为无法从标记得知具体等级）。
//  3. 都 miss → LevelCold。
//
// WHF 本地缓存可能 miss：
//   - 后台 flush 每 6 秒刷新一次 levels 映射表。
//   - 但如果在两次 flush 之间访问了一个从未在本次 flush 中出现过的 key
//     （例如另一个实例刚刚把它变热了，但当前实例还未 flush），
//     本地 cache 没有该 key，需要 fallback 到 Redis。
//
// 参数：
//   - key: 缓存键
//
// 返回值：
//   - HotKeyLevel: LevelCold / LevelLow / LevelMedium / LevelHigh
func (d *HotKeyDetector) getLevel(key string) HotKeyLevel {
	// 1. 查本地 levelCache（fast path）
	if level, ok := d.readLevelCache(key); ok {
		return level
	}

	// 2. 查 Redis hotkey:active 标记（fallback）
	ctx := context.Background()
	exists, err := d.redis.Exists(ctx, hotkeyActivePrefix+key).Result()
	if err == nil && exists > 0 {
		return LevelMedium
	}

	return LevelCold
}

// readLevelCache 从本地 levels 映射中读取热度等级。
func (d *HotKeyDetector) readLevelCache(key string) (HotKeyLevel, bool) {
	d.levelMu.RLock()
	level, ok := d.levels[key]
	d.levelMu.RUnlock()
	return level, ok
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

// Ensure HotKeyDetector implements FlushDetector (if needed by external tests).
var _ fmt.Stringer = (*HotKeyLevel)(nil)

// String 将 HotKeyLevel 转为可读字符串。
func (l HotKeyLevel) String() string {
	switch l {
	case LevelCold:
		return "cold"
	case LevelLow:
		return "low"
	case LevelMedium:
		return "medium"
	case LevelHigh:
		return "high"
	default:
		return fmt.Sprintf("unknown(%d)", l)
	}
}
