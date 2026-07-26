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
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

// hotwinKeyPrefix 是 Redis Hash 键的前缀。
const hotwinKeyPrefix = "hotwin:"

// hotkeyActivePrefix 是 hotkey 标记键的前缀。
const hotkeyActivePrefix = "hotkey:active:"

// finalFlushTimeout 是停机收尾 flush 的超时上限。
const finalFlushTimeout = 2 * time.Second

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
//   - 后台 goroutine 通过 Run(ctx) 显式启动
type HotKeyDetector struct {
	config *config.HotKeyConfig
	redis  *redis.Client
	logger *zap.Logger

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

	// 本地 map 最大键数限制
	maxKeys int

	// 生命周期控制
	startOnce sync.Once
}

// NewHotKeyDetector 根据配置和 Redis 客户端创建跨实例热点键探测器。
func NewHotKeyDetector(cfg *config.HotKeyConfig, redisClient *redis.Client, logger *zap.Logger) *HotKeyDetector {
	d := &HotKeyDetector{
		config:        cfg,
		redis:         redisClient,
		logger:        logger,
		buf:           make(map[string]map[int64]int64),
		levels:        make(map[string]HotKeyLevel),
		bucketSize:    time.Duration(cfg.BucketSizeSeconds) * time.Second,
		flushInterval: time.Duration(cfg.FlushIntervalSeconds) * time.Second,
		statTTL:       time.Duration(cfg.StatTTLSeconds) * time.Second,
		markTTL:       time.Duration(cfg.HotMarkTTLSeconds) * time.Second,
		maxKeys:       100000,
	}
	if cfg.MaxLocalKeys > 0 {
		d.maxKeys = cfg.MaxLocalKeys
	}
	if d.logger == nil {
		d.logger = zap.L()
	}
	return d
}

// Run 启动后台 flush goroutine，使用给定的 ctx 控制生命周期（**非阻塞**）。
//
// ctx 通常来自服务启动时的 root context，当 ctx 被取消时，flush goroutine 会退出。
//
// 注意：本方法立即返回，调用方无法感知 flush 循环何时真正结束。
// 若调用方需要在停机时等待 flush 收尾（例如作为 server.BackgroundRunner），
// 应改用 RunUntilDone。
func (d *HotKeyDetector) Run(ctx context.Context) {
	d.startOnce.Do(func() {
		go d.flushLoop(ctx)
	})
}

// RunUntilDone 在**当前 goroutine** 内运行 flush 循环，直到 ctx 被取消才返回。
//
// WHY 需要一个阻塞版本：
//
//	server.BackgroundRunner 的契约是「Start 阻塞至该任务生命周期结束」，
//	上层据此用 WaitGroup 判断后台任务是否已排空。
//	若 Start 内部只是 go 一个 goroutine 就返回，WaitGroup 会立刻计数归零，
//	waitBackgroundRunners 报告的「后台任务已退出」就是个假信号——
//	停机时最后一个窗口的本地计数会随进程一起消失。
//
// 返回前会执行一次最终 flush（用独立的短超时 context，因为此时 ctx 已取消），
// 把最后一个窗口的计数落到 Redis。
func (d *HotKeyDetector) RunUntilDone(ctx context.Context) {
	d.startOnce.Do(func() {
		d.flushLoop(ctx)
		d.finalFlush()
	})
}

// finalFlush 停机时的收尾 flush。
//
// 此时生命周期 ctx 已取消，因此必须新建一个不受其影响的 context；
// 超时设置得很短，避免 Redis 不可用时拖慢整体停机。
func (d *HotKeyDetector) finalFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), finalFlushTimeout)
	defer cancel()
	d.flushOnce(ctx)
}

// Record 为指定键在当前时间窗口内增加一次命中计数。
func (d *HotKeyDetector) Record(key string) {
	bucket := d.currentBucket()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.buf[key] == nil {
		if len(d.buf) >= d.maxKeys {
			d.evictOldestLocked()
		}
		d.buf[key] = make(map[int64]int64)
	}
	d.buf[key][bucket]++
}

// evictOldestLocked 在 buf 达到上限时腾出空间。持有 mu 锁时调用。
//
// 淘汰依据是每个键的**最近一次**访问所在的桶（newest），而非首次访问所在的桶：
// 按 newest 淘汰才是 LRU 语义；按 oldest 淘汰会优先干掉「持续被访问、
// 因而首次访问时间最早」的长热点键，恰好淘汰了最不该淘汰的一批。
//
// 复杂度：两趟 O(N) 扫描，不做排序。
// 该方法在 Record() 的临界区内执行，而 Record() 位于每次缓存读的热路径上，
// 原先的 O(N log N) 全量排序（N 默认上限 10 万）会在扩容瞬间造成明显的锁等待尖刺。
func (d *HotKeyDetector) evictOldestLocked() {
	if len(d.buf) == 0 {
		return
	}

	// 目标：至少腾出 10% 的空间。
	target := len(d.buf) / 10
	if target < 1 {
		target = 1
	}

	// 第一趟：淘汰已经完全滑出统计窗口的键——它们对任何窗口求和都不再有贡献。
	staleBefore := d.currentBucket() - int64(d.config.BucketCount)
	evicted := 0
	for k, buckets := range d.buf {
		newest := int64(math.MinInt64)
		for b := range buckets {
			if b > newest {
				newest = b
			}
		}
		if newest < staleBefore {
			delete(d.buf, k)
			evicted++
		}
	}
	if evicted >= target {
		return
	}

	// 第二趟：窗口内的键都还“活着”，此时已无客观优劣之分，
	// 按 Go map 的随机迭代顺序补足配额即可（O(N)，且不引入排序开销）。
	for k := range d.buf {
		if evicted >= target {
			break
		}
		delete(d.buf, k)
		evicted++
	}
}

// currentBucket 返回当前时间对应的桶编号（Unix 秒 / bucketSize）。
func (d *HotKeyDetector) currentBucket() int64 {
	return time.Now().Unix() / int64(d.bucketSize.Seconds())
}

// flushLoop 是后台 flush goroutine 的主循环。
func (d *HotKeyDetector) flushLoop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			d.logger.Error("hotkey flushLoop panic recovered", zap.Any("panic", r))
		}
	}()

	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.flushOnce(ctx)
		}
	}
}

// flushOnce 执行一轮完整的 flush 流程。
func (d *HotKeyDetector) flushOnce(ctx context.Context) {
	snapshot := d.snapshotAndReset()
	if len(snapshot) == 0 {
		// 本轮无访问：窗口内已无热点，同步清空等级缓存，
		// 否则流量停止后旧等级会永久残留（见 replaceLevels 注释）。
		d.replaceLevels(nil)
		return
	}

	nowBucket := d.currentBucket()
	staleFields := d.staleBucketFields(nowBucket)

	pipe := d.redis.Pipeline()
	for cacheKey, buckets := range snapshot {
		statKey := hotwinKeyPrefix + cacheKey
		for bucket, count := range buckets {
			pipe.HIncrBy(ctx, statKey, strconv.FormatInt(bucket, 10), count)
		}
		if len(staleFields) > 0 {
			// 一条 HDEL 携带全部待删字段。
			// 原实现对每个桶单发一条 HDEL，命令数 = 键数 × BucketCount，
			// 在万级热点键规模下每轮 flush 会产生十万级命令。
			pipe.HDel(ctx, statKey, staleFields...)
		}
		pipe.Expire(ctx, statKey, d.statTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return
	}

	newLevels := make(map[string]HotKeyLevel, len(snapshot))
	cacheKeys := make([]string, 0, len(snapshot))
	for cacheKey := range snapshot {
		cacheKeys = append(cacheKeys, cacheKey)
	}

	if len(cacheKeys) > 0 {
		pipeRead := d.redis.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, len(cacheKeys))
		for i, cacheKey := range cacheKeys {
			statKey := hotwinKeyPrefix + cacheKey
			cmds[i] = pipeRead.HGetAll(ctx, statKey)
		}
		if _, err := pipeRead.Exec(ctx); err != nil {
			d.logger.Warn("hotkey pipeRead exec failed", zap.Error(err))
		}

		pipeMark := d.redis.Pipeline()
		for i, cacheKey := range cacheKeys {
			values, err := cmds[i].Result()
			if err != nil {
				continue
			}
			total := d.sumBucketsInWindow(values, nowBucket)
			level := d.calcLevel(total)
			newLevels[cacheKey] = level
			if level >= LevelLow {
				pipeMark.Set(ctx, hotkeyActivePrefix+cacheKey, "1", d.markTTL)
			}
		}
		if _, err := pipeMark.Exec(ctx); err != nil {
			d.logger.Warn("hotkey pipeMark exec failed", zap.Error(err))
		}
	}

	d.replaceLevels(newLevels)
}

// staleBucketFields 返回本轮应从 Redis Hash 中删除的桶字段。
//
// 只删「刚滑出统计窗口」的那几个桶，而不是每轮重删整个历史区间：
// 窗口是连续向前滑动的，每个桶在其滑出的那一轮被删一次即可。
// slack 覆盖 flush 间隔大于桶长（或某轮 flush 因 Redis 抖动失败）时一次跨过多个桶的情况；
// 即便仍有遗漏，statKey 自带 TTL，且 sumBucketsInWindow 会按桶号过滤，
// 残留字段既不会撑爆内存也不会污染统计结果。
func (d *HotKeyDetector) staleBucketFields(nowBucket int64) []string {
	newestStale := nowBucket - int64(d.config.BucketCount)
	if newestStale < 0 {
		return nil
	}

	slack := int64(1)
	if bucketSecs := int64(d.bucketSize.Seconds()); bucketSecs > 0 {
		if n := int64(d.flushInterval.Seconds()) / bucketSecs; n > slack {
			slack = n
		}
	}

	oldestStale := max(newestStale-slack, 0)
	fields := make([]string, 0, newestStale-oldestStale+1)
	for b := newestStale; b >= oldestStale; b-- {
		fields = append(fields, strconv.FormatInt(b, 10))
	}
	return fields
}

// replaceLevels 用本轮窗口的统计结果整体替换等级缓存。
//
// WHY 整体替换而非增量合并：
//
//	levels 的键空间是「被访问过的缓存键」，在内容型业务里随内容总量线性增长且无上界。
//	原实现只往里写、从不删除，长期运行等同于内存泄漏；
//	同时，一个曾经上榜的键在降温后会永远保留旧等级，TTL 被无谓地持续延长。
//	整体替换让 levels 的规模始终等于「上一轮窗口内实际被访问的键数」，
//	既天然受 maxKeys 约束，也使降温的键自动退出热点集合。
func (d *HotKeyDetector) replaceLevels(levels map[string]HotKeyLevel) {
	if levels == nil {
		levels = make(map[string]HotKeyLevel)
	}
	d.levelMu.Lock()
	d.levels = levels
	d.levelMu.Unlock()
}

// SetMaxKeys 设置本地 map 上限。
func (d *HotKeyDetector) SetMaxKeys(n int) {
	if n <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.maxKeys = n
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
func (d *HotKeyDetector) TtlForPublic(ctx context.Context, baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(ctx, key))
}

// TtlForPublicBatch 批量返回一组键按热度调整后的 TTL，返回值下标与 keys 一一对应。
//
// 与逐个调用 TtlForPublic 的区别在于 Redis 访问次数：
// 本地等级缓存未命中的键会被合并成**一次** MGET，而不是每个键一次 EXISTS。
// 列表类场景（一页 Feed 几十个条目）由此把 N 次串行往返压缩为至多 1 次。
func (d *HotKeyDetector) TtlForPublicBatch(ctx context.Context, baseTTL int, keys []string) []int {
	ttls := make([]int, len(keys))
	if len(keys) == 0 {
		return ttls
	}

	// 第一趟：命中本地等级缓存的直接定级，其余记录下标待批量查询。
	missIdx := make([]int, 0, len(keys))
	markKeys := make([]string, 0, len(keys))
	d.levelMu.RLock()
	for i, key := range keys {
		if level, ok := d.levels[key]; ok {
			ttls[i] = d.ttlForLevel(baseTTL, level)
			continue
		}
		missIdx = append(missIdx, i)
		markKeys = append(markKeys, hotkeyActivePrefix+key)
	}
	d.levelMu.RUnlock()

	if len(missIdx) == 0 {
		return ttls
	}

	// 未命中的键默认视为冷键；MGET 失败时保持该默认值（fail-cold，不延长 TTL）。
	for _, i := range missIdx {
		ttls[i] = baseTTL
	}

	marks, err := d.redis.MGet(ctx, markKeys...).Result()
	if err != nil {
		d.logger.Warn("hotkey: redis MGet failed", zap.Int("keys", len(markKeys)), zap.Error(err))
		return ttls
	}

	d.levelMu.Lock()
	for n, i := range missIdx {
		if n >= len(marks) || marks[n] == nil {
			continue
		}
		ttls[i] = d.ttlForLevel(baseTTL, LevelMedium)
		if len(d.levels) < d.maxKeys {
			d.levels[keys[i]] = LevelMedium
		}
	}
	d.levelMu.Unlock()

	return ttls
}

// getLevel 根据本地 levelCache 或 Redis hotkey:active 标记判断热度等级。
func (d *HotKeyDetector) getLevel(ctx context.Context, key string) HotKeyLevel {
	if level, ok := d.readLevelCache(key); ok {
		return level
	}

	exists, err := d.redis.Exists(ctx, hotkeyActivePrefix+key).Result()
	if err != nil {
		d.logger.Warn("hotkey: redis Exists failed", zap.String("key", key), zap.Error(err))
		return LevelCold
	}
	if exists > 0 {
		// 写回本地缓存，避免下次再查 Redis。
		// 上限保护：levels 每轮 flush 会被整体替换（见 replaceLevels），
		// 这里再加一道闸，防止两轮之间被大量陌生键灌爆。
		d.levelMu.Lock()
		if len(d.levels) < d.maxKeys {
			d.levels[key] = LevelMedium
		}
		d.levelMu.Unlock()
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
