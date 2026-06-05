// Package cache 提供热点键识别（HotKeyDetector）能力。
//
// HotKeyDetector：使用本地 map + Redis Hash 实现跨实例滑动窗口热点检测。
// 每次缓存访问仅递增本地计数（零 Redis IO），每 6 秒批量 flush 到 Redis Hash 完成跨实例聚合。
// 热度分为三级：
//   - LOW（+20s）：低热度，QPS 略高于背景水平
//   - MEDIUM（+60s）：中等热度，QPS 明显高于背景水平
//   - HIGH（+120s）：高热度，QPS 极高
//
// WHY 选用 Hash 而非 ZSET：
//
//	ZSET 适合对多个 key 排序（排行榜），而本场景是每个 key 下存 10 个时间窗口的计数，
//	Hash 的 field→value 模型（窗口编号→访问次数）更自然，无需维护 member 排序开销。
//
// WHY 不用每次请求直接写 Redis：
//
//	如果每次 Record() 都 HINCRBY，QPS 高时 Redis 压力大（写放大）。
//	本地 map 先聚合，每 6 秒一次批量 flush，Redis 写入量降低数个数量级。
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

// HotKeyDetector 使用本地 map + Redis Hash 检测跨实例热点键。
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
	levelMu sync.RWMutex
	levels  map[string]HotKeyLevel

	// 派生字段，由 config 计算得出
	bucketSize    time.Duration // 每个桶的时长（如 6s）
	flushInterval time.Duration // flush 间隔
	statTTL       time.Duration // Redis Hash 的 TTL
	markTTL       time.Duration // hotkey:active 标记 TTL

	// 生命周期控制
	startOnce sync.Once
}

// NewHotKeyDetector 根据配置和 Redis 客户端创建跨实例热点键探测器。
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
	}
}

// Record 为指定键在当前时间窗口内增加一次命中计数。
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
func (d *HotKeyDetector) flushLoop() {
	defer func() {
		if r := recover(); r != nil {
			go d.flushLoop()
		}
	}()

	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		d.flushOnce()
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

	pipe := d.redis.Pipeline()
	for cacheKey, buckets := range snapshot {
		statKey := hotwinKeyPrefix + cacheKey
		for bucket, count := range buckets {
			pipe.HIncrBy(ctx, statKey, strconv.FormatInt(bucket, 10), count)
		}
		for i := int64(d.config.BucketCount); i < int64(d.config.BucketCount)+int64(d.config.BucketCount); i++ {
			oldBucket := nowBucket - i
			pipe.HDel(ctx, statKey, strconv.FormatInt(oldBucket, 10))
		}
		pipe.Expire(ctx, statKey, d.statTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return
	}

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

		if level >= LevelLow {
			d.redis.Set(ctx, hotkeyActivePrefix+cacheKey, "1", d.markTTL)
		}
	}

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

// TtlForPublic 返回公共缓存键根据热度调整后的 TTL。
func (d *HotKeyDetector) TtlForPublic(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key))
}

// getLevel 根据本地 levelCache 或 Redis hotkey:active 标记判断热度等级。
func (d *HotKeyDetector) getLevel(key string) HotKeyLevel {
	if level, ok := d.readLevelCache(key); ok {
		return level
	}

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
func (d *HotKeyDetector) ttlForLevel(baseTTL int, level HotKeyLevel) int {
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