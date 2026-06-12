package cache

import (
	"context"
	"strconv"
	"time"
)

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
//
// 流程是：
//   - 快照并清空本地缓冲；
//   - 用 Pipeline 批量把桶计数刷到 Redis Hash；
//   - 清理窗口外旧桶；
//   - 重新计算本轮参与 key 的热度等级。
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
		for key, level := range newLevels {
			d.levels[key] = level
		}
		d.levelMu.Unlock()
	}
}

// snapshotAndReset 快照并清空本地缓冲。
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
