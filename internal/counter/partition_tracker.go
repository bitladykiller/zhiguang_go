package counter

import (
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// partitionTracker 管理各分区的 counterBatch，提供线程安全的批量操作。
// mu 由 AggregationConsumer 的调用方管理，方法自身不持锁。
type partitionTracker struct {
	mu      sync.Mutex
	batches map[int]*counterBatch
}

// newPartitionTracker 创建 partitionTracker 实例。
//
// 参数:
//   - batchSize: int，新建 counterBatch 的默认容量
//
// 返回值:
//   - *partitionTracker: 已初始化的分区跟踪器
func newPartitionTracker() *partitionTracker {
	return &partitionTracker{
		batches: make(map[int]*counterBatch),
	}
}

// acceptMessageAsync 将消息纳入分区 batch，在 gap 或 batch 满时返回需要异步刷出的分区列表。
// 调用方应在释放锁后使用返回的分区列表执行 flush。
func (t *partitionTracker) acceptMessageAsync(c *AggregationConsumer, msg kafka.Message) (int, bool, error) {
	if batch := t.batches[msg.Partition]; batch != nil && batch.size() > 0 && msg.Offset != batch.endOffset+1 {
		return msg.Partition, false, nil
	}

	evt, err := parseCounterEvent(msg.Value)
	if err != nil {
		if batch := t.batches[msg.Partition]; batch != nil && batch.size() > 0 {
			return msg.Partition, false, nil
		}
		return -1, false, nil
	}

	batch := t.batches[msg.Partition]
	if batch == nil {
		batch = newCounterBatch(c.cfg.batchSize)
		t.batches[msg.Partition] = batch
	}
	if err := batch.addEvent(msg, evt); err != nil {
		if batch.size() > 0 {
			return msg.Partition, false, nil
		}
		return -1, false, err
	}

	if batch.size() >= c.cfg.batchSize {
		return msg.Partition, true, nil
	}
	return -1, false, nil
}

// flushExpiredBatches 检查所有分区 batch，将超时的 batch 刷出并清理空 batch。
// 返回是否有任何 batch 被刷出。
//
// 注意：此方法会调用 flushPartitionBatch，后者内部执行网络 I/O（Redis Pipeline + Kafka commit）。
// 调用方应确保在持有 mu 锁的情况下调用，或使用 expireBatches 将实际 I/O 延迟到锁外执行。
func (t *partitionTracker) flushExpiredBatches(ctx context.Context, c *AggregationConsumer, now time.Time) bool {
	expired := make([]int, 0, len(t.batches))
	for partition, batch := range t.batches {
		if batch == nil || batch.size() == 0 {
			delete(t.batches, partition)
			continue
		}
		if !now.Before(batch.openedAt.Add(c.cfg.flushInterval)) {
			expired = append(expired, partition)
		}
	}
	if len(expired) == 0 {
		return false
	}

	sort.Ints(expired)
	for _, partition := range expired {
		t.flushPartitionBatch(ctx, c, partition)
	}
	return true
}

// expireBatches 返回所有已过期的分区编号，不做实际 flush。
// 调用方应在释放锁后使用返回的分区自行执行 flush。
func (t *partitionTracker) expireBatches(now time.Time, flushInterval time.Duration) []int {
	expired := make([]int, 0, len(t.batches))
	for partition, batch := range t.batches {
		if batch == nil || batch.size() == 0 {
			delete(t.batches, partition)
			continue
		}
		if !now.Before(batch.openedAt.Add(flushInterval)) {
			expired = append(expired, partition)
		}
	}
	return expired
}

// flushPartitionBatch 刷出指定分区的 batch。
func (t *partitionTracker) flushPartitionBatch(ctx context.Context, c *AggregationConsumer, partition int) {
	batch := t.batches[partition]
	if batch == nil || batch.size() == 0 {
		delete(t.batches, partition)
		return
	}

	c.flushAndReset(ctx, batch)
	if batch.size() == 0 {
		delete(t.batches, partition)
	}
}

// nextBatchDeadline 返回所有非空 batch 中最早的截止时间。
func nextBatchDeadline(tracker *partitionTracker, flushInterval time.Duration) (time.Time, bool) {
	var (
		deadline time.Time
		ok       bool
	)
	for _, batch := range tracker.batches {
		if batch == nil || batch.size() == 0 {
			continue
		}
		current := batch.openedAt.Add(flushInterval)
		if !ok || current.Before(deadline) {
			deadline = current
			ok = true
		}
	}
	return deadline, ok
}
