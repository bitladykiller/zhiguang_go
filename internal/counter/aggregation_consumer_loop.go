package counter

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

func (c *AggregationConsumer) consumeLoop(ctx context.Context) {
	batches := make(map[int]*counterBatch)
	defer c.flushRemainingBatches(batches)

	for {
		if flushed := c.flushExpiredBatches(ctx, batches, time.Now()); flushed {
			continue
		}

		fetchCtx := ctx
		if deadline, ok := nextBatchDeadline(batches, c.flushInterval); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				continue
			}

			var cancel context.CancelFunc
			fetchCtx, cancel = context.WithTimeout(ctx, remaining)
			msg, err := c.reader.FetchMessage(fetchCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				c.logWarn("fetch counter kafka message failed", err)
				if !sleepCounterConsumer(ctx, time.Second) {
					return
				}
				continue
			}
			if err := c.acceptMessage(ctx, batches, msg); err != nil {
				c.logWarn("accept counter kafka message failed", err)
			}
			continue
		}

		msg, err := c.reader.FetchMessage(fetchCtx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logWarn("fetch counter kafka message failed", err)
			if !sleepCounterConsumer(ctx, time.Second) {
				return
			}
			continue
		}
		if err := c.acceptMessage(ctx, batches, msg); err != nil {
			c.logWarn("accept counter kafka message failed", err)
		}
	}
}

func (c *AggregationConsumer) acceptMessage(ctx context.Context, batches map[int]*counterBatch, msg kafka.Message) error {
	if batch := batches[msg.Partition]; batch != nil && batch.size() > 0 && msg.Offset != batch.endOffset+1 {
		c.flushPartitionBatch(ctx, batches, msg.Partition)
	}

	evt, err := parseCounterEvent(msg.Value)
	if err != nil {
		if batch := batches[msg.Partition]; batch != nil && batch.size() > 0 {
			c.flushPartitionBatch(ctx, batches, msg.Partition)
		}
		c.skipMalformedMessage(ctx, msg, err)
		return nil
	}

	batch := batches[msg.Partition]
	if batch == nil {
		batch = newCounterBatch(c.batchSize)
		batches[msg.Partition] = batch
	}
	if err := batch.addEvent(msg, evt); err != nil {
		if batch.size() > 0 {
			c.flushPartitionBatch(ctx, batches, msg.Partition)
		}
		c.skipMalformedMessage(ctx, msg, err)
		return nil
	}

	if batch.size() >= c.batchSize {
		c.flushPartitionBatch(ctx, batches, msg.Partition)
	}
	return nil
}

func (c *AggregationConsumer) flushExpiredBatches(ctx context.Context, batches map[int]*counterBatch, now time.Time) bool {
	expired := make([]int, 0, len(batches))
	for partition, batch := range batches {
		if batch == nil || batch.size() == 0 {
			delete(batches, partition)
			continue
		}
		if !now.Before(batch.openedAt.Add(c.flushInterval)) {
			expired = append(expired, partition)
		}
	}
	if len(expired) == 0 {
		return false
	}

	sort.Ints(expired)
	for _, partition := range expired {
		c.flushPartitionBatch(ctx, batches, partition)
	}
	return true
}

func (c *AggregationConsumer) flushPartitionBatch(ctx context.Context, batches map[int]*counterBatch, partition int) {
	batch := batches[partition]
	if batch == nil || batch.size() == 0 {
		delete(batches, partition)
		return
	}

	c.flushAndReset(ctx, batch)
	if batch.size() == 0 {
		delete(batches, partition)
	}
}

func nextBatchDeadline(batches map[int]*counterBatch, flushInterval time.Duration) (time.Time, bool) {
	var (
		deadline time.Time
		ok       bool
	)
	for _, batch := range batches {
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

func (c *AggregationConsumer) skipMalformedMessage(ctx context.Context, msg kafka.Message, cause error) {
	c.logWarn("skip malformed counter kafka message", cause)
	if err := c.advanceAppliedOffset(ctx, msg.Partition, msg.Offset); err != nil {
		c.logWarn("advance malformed counter kafka message offset failed", err)
		return
	}
	if err := c.commitMessages(ctx, msg); err != nil {
		c.logWarn("commit malformed counter kafka message failed", err)
	}
}

func sleepCounterConsumer(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
