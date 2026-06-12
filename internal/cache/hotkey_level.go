package cache

import (
	"context"
	"fmt"
	"strconv"
)

// sumBucketsInWindow 从 Redis Hash 结果中累加最近窗口内的命中数。
//
// Redis Hash 的 field 是桶编号，value 是该桶内的访问次数。
// 这里只统计 [nowBucket-bucketCount+1, nowBucket] 这个滑动窗口，
// 超出窗口的数据即使尚未被 HDEL 清理，也不会参与热度判断。
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

// calcLevel 根据窗口总命中数映射热度等级。
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

// TtlForPublic 返回公共缓存键根据热度等级调整后的 TTL。
func (d *HotKeyDetector) TtlForPublic(baseTTL int, key string) int {
	return d.ttlForLevel(baseTTL, d.getLevel(key))
}

// getLevel 优先读本地缓存；缓存缺失时再降级读取 Redis 的活跃标记。
//
// Redis 标记只表示“它最近是热点”，无法区分 low/high，因此这里统一降级为
// LevelMedium。这样可以在跨实例场景下避免本机冷启动时完全丢失热点感知。
func (d *HotKeyDetector) getLevel(key string) HotKeyLevel {
	if level, ok := d.readLevelCache(key); ok {
		return level
	}
	if d.redis == nil {
		return LevelCold
	}

	ctx := context.Background()
	exists, err := d.redis.Exists(ctx, hotkeyActivePrefix+key).Result()
	if err == nil && exists > 0 {
		return LevelMedium
	}

	return LevelCold
}

// readLevelCache 从本地热度缓存读取等级。
func (d *HotKeyDetector) readLevelCache(key string) (HotKeyLevel, bool) {
	d.levelMu.RLock()
	level, ok := d.levels[key]
	d.levelMu.RUnlock()
	return level, ok
}

// ttlForLevel 根据热度等级返回最终 TTL。
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

// String 将热度等级转成便于日志输出的字符串。
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
