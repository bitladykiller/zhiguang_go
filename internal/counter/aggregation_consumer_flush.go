package counter

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func (c *AggregationConsumer) flushRemainingBatches(batches map[int]*counterBatch) {
	if len(batches) == 0 {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), counterShutdownFlushTimeout)
	defer cancel()

	partitions := make([]int, 0, len(batches))
	for partition, batch := range batches {
		if batch != nil && batch.size() > 0 {
			partitions = append(partitions, partition)
		}
	}
	sort.Ints(partitions)
	for _, partition := range partitions {
		c.flushPartitionBatch(shutdownCtx, batches, partition)
	}
}

func (c *AggregationConsumer) flushAndReset(ctx context.Context, batch *counterBatch) {
	if batch.size() == 0 {
		return
	}

	maxAttempts := c.maxFlushAttempts()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.flushBatch(ctx, batch); err != nil {
			c.logWarn(fmt.Sprintf("flush counter batch failed (attempt %d/%d)", attempt, maxAttempts), err)
			if !isCounterCommitError(err) && attempt == maxAttempts {
				recordCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				if recordErr := c.service.recordFailedKafkaMessages(recordCtx, counterFailureStageApply, batch.messages, err); recordErr != nil {
					c.logWarn("persist counter apply failure tasks failed", recordErr)
				}
				cancel()
				c.logWarn(
					fmt.Sprintf("flush counter batch exhausted retries and will defer repair task (attempts=%d, messages=%d)", maxAttempts, batch.size()),
					err,
				)
				break
			}
			if attempt == maxAttempts {
				c.logWarn(
					fmt.Sprintf("flush counter batch exhausted commit retries; redis watermark will absorb redelivery (attempts=%d, messages=%d)", maxAttempts, batch.size()),
					err,
				)
				break
			}
			if !sleepCounterConsumer(ctx, c.retryDelay()) {
				return
			}
			continue
		}
		batch.reset()
		return
	}

	batch.reset()
}

func (c *AggregationConsumer) flushBatch(ctx context.Context, batch *counterBatch) error {
	if batch == nil || batch.size() == 0 {
		return nil
	}

	if err := c.applyBatch(ctx, batch); err != nil {
		return fmt.Errorf("apply counter batch: %w", err)
	}
	if err := c.commitMessages(ctx, batch.messages...); err != nil {
		return fmt.Errorf("%w: %v", errCounterBatchCommit, err)
	}
	return nil
}

func (c *AggregationConsumer) applyBatch(ctx context.Context, batch *counterBatch) error {
	if batch == nil || batch.size() == 0 {
		return nil
	}

	appliedKey, err := c.appliedOffsetKey(batch.partition)
	if err != nil {
		return err
	}

	cntKeys, epochKeys, keyIndexes := batch.entityKeys()
	keys := make([]string, 0, len(cntKeys)*2+1)
	keys = append(keys, appliedKey)
	for i := range cntKeys {
		keys = append(keys, cntKeys[i], epochKeys[i])
	}

	args := make([]any, 0, 3+len(batch.events)*6)
	args = append(args, SchemaLen, FieldSize, len(batch.events))
	for _, event := range batch.events {
		args = append(args,
			event.offset,
			keyIndexes[CounterEntityMember(event.entityType, event.entityID)],
			event.index,
			event.delta,
			event.epoch,
			boolToLuaInt(event.usesEpoch),
		)
	}

	return applyPartitionBatchScript.Run(ctx, c.service.redis, keys, args...).Err()
}

func (c *AggregationConsumer) commitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if c != nil && c.commitFn != nil {
		return c.commitFn(ctx, msgs...)
	}
	if c != nil && c.reader != nil {
		return c.reader.CommitMessages(ctx, msgs...)
	}
	return nil
}

func (c *AggregationConsumer) appliedOffsetKey(partition int) (string, error) {
	if c == nil || c.groupID == "" || c.topic == "" {
		return "", fmt.Errorf("counter consumer applied offset scope is empty")
	}
	return AppliedOffsetKey(c.groupID, c.topic, partition), nil
}

func (c *AggregationConsumer) advanceAppliedOffset(ctx context.Context, partition int, offset int64) error {
	appliedKey, err := c.appliedOffsetKey(partition)
	if err != nil {
		return err
	}
	return advanceAppliedOffsetScript.Run(ctx, c.service.redis, []string{appliedKey}, offset).Err()
}

// forceAdvanceAppliedOffset 强制推进水位线，跳过空洞。
//
// 用途：当遇到无法处理的坏消息时，强制跳过并推进水位线，
// 避免单个坏消息阻塞整个分区。
func (c *AggregationConsumer) forceAdvanceAppliedOffset(ctx context.Context, partition int, offset int64) error {
	appliedKey, err := c.appliedOffsetKey(partition)
	if err != nil {
		return err
	}
	return forceAdvanceOffsetScript.Run(ctx, c.service.redis, []string{appliedKey}, offset).Err()
}

func isCounterCommitError(err error) bool {
	return errors.Is(err, errCounterBatchCommit)
}

func boolToLuaInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (c *AggregationConsumer) maxFlushAttempts() int {
	if c != nil && c.flushMaxAttempts > 0 {
		return c.flushMaxAttempts
	}
	return defaultCounterFlushMaxAttempts
}

func (c *AggregationConsumer) retryDelay() time.Duration {
	if c != nil && c.flushRetryDelay > 0 {
		return c.flushRetryDelay
	}
	return defaultCounterFlushRetryDelay
}

func (c *AggregationConsumer) logWarn(msg string, err error) {
	if c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}
