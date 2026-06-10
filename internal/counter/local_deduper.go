package counter

import (
	"sync"
	"time"
)

const (
	defaultCounterDedupBucketCount    = 6
	defaultCounterDedupBucketDuration = 10 * time.Second
)

// MessageIDGenerator 抽象本地雪花 ID 生成能力，便于复用应用现有生成器。
type MessageIDGenerator interface {
	NextID() uint64
}

// localCounterDeduper 使用固定时间桶做短期本地去重。
//
// 设计目标：
//   - 桶内用 map[messageID]struct{} 做 O(1) 判重。
//   - 一共 6 个桶，每 10 秒轮转一次，总窗口 60 秒。
//   - 轮转时直接清空最老桶，避免全表扫描删除。
type localCounterDeduper struct {
	mu             sync.RWMutex
	buckets        []map[uint64]struct{}
	currentBucket  int
	bucketDuration time.Duration
	lastRotate     time.Time
}

func newLocalCounterDeduper(bucketCount int, bucketDuration time.Duration) *localCounterDeduper {
	if bucketCount <= 0 {
		bucketCount = defaultCounterDedupBucketCount
	}
	if bucketDuration <= 0 {
		bucketDuration = defaultCounterDedupBucketDuration
	}

	buckets := make([]map[uint64]struct{}, bucketCount)
	for i := range buckets {
		buckets[i] = make(map[uint64]struct{})
	}

	return &localCounterDeduper{
		buckets:        buckets,
		bucketDuration: bucketDuration,
		lastRotate:     time.Now(),
	}
}

func (d *localCounterDeduper) Seen(messageID uint64) bool {
	return d.seenAt(time.Now(), messageID)
}

func (d *localCounterDeduper) Remember(messageID uint64) {
	d.rememberAt(time.Now(), messageID)
}

func (d *localCounterDeduper) seenAt(now time.Time, messageID uint64) bool {
	if d == nil || messageID == 0 {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.rotateLocked(now)

	for _, bucket := range d.buckets {
		if _, ok := bucket[messageID]; ok {
			return true
		}
	}
	return false
}

func (d *localCounterDeduper) rememberAt(now time.Time, messageID uint64) {
	if d == nil || messageID == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.rotateLocked(now)
	d.buckets[d.currentBucket][messageID] = struct{}{}
}

func (d *localCounterDeduper) rotateLocked(now time.Time) {
	if d == nil || now.Before(d.lastRotate) {
		return
	}

	steps := int(now.Sub(d.lastRotate) / d.bucketDuration)
	if steps <= 0 {
		return
	}

	if steps >= len(d.buckets) {
		for i := range d.buckets {
			clear(d.buckets[i])
		}
		d.currentBucket = (d.currentBucket + steps) % len(d.buckets)
		d.lastRotate = d.lastRotate.Add(time.Duration(steps) * d.bucketDuration)
		return
	}

	for i := 0; i < steps; i++ {
		d.currentBucket = (d.currentBucket + 1) % len(d.buckets)
		clear(d.buckets[d.currentBucket])
	}
	d.lastRotate = d.lastRotate.Add(time.Duration(steps) * d.bucketDuration)
}
