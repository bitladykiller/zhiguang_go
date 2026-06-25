package counter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/contextutil"
	"github.com/zhiguang/app/pkg/redislock"
)

const counterRepairLeaderLockKey = "lock:counter:repair"

const (
	defaultCounterFlushMaxAttempts = 3
	defaultCounterFlushRetryDelay  = time.Second
	defaultBatchSize               = 100
	defaultFlushInterval           = time.Second
	defaultRepairInterval          = time.Minute
)

// AggregationConsumer 消费 counter-events，并按批次把增量直接折叠到 cnt:*。
//
// 这里不再使用 Redis agg:* 中转桶，原因是当前方案把"批量聚合"放在 MQ 消费端：
//   - 同一批 Kafka 消息先在进程内做内存聚合。
//   - 到达批次大小或时间窗口后，一次性把 delta flush 到 cnt:*。
//   - 如果 publish、flush 或 offset commit 出现失败，对应实体会进入 dirty set，
//     再由 repair loop 用位图的绝对值覆盖 cnt:*。
type AggregationConsumer struct {
	reader   *kafka.Reader
	service  *CounterService
	logger   *zap.Logger
	commitFn func(ctx context.Context, msgs ...kafka.Message) error
	cfg      *consumerConfig
	tracker  *partitionTracker
	wg       sync.WaitGroup
}

// NewAggregationConsumer 创建 AggregationConsumer 实例。
//
// 参数:
//   - reader: *kafka.Reader，Kafka 消息读取器
//   - service: *CounterService，计数器服务，用于 flush 和 repair
//   - logger: *zap.Logger，日志记录器
//   - cfg: *config.CounterConfig，消费者配置（batchSize、flushInterval 等）
//
// 返回值:
//   - *AggregationConsumer: 已初始化的聚合消费者；若 reader/service/redis 为 nil 则返回 nil
func NewAggregationConsumer(
	reader *kafka.Reader,
	service *CounterService,
	logger *zap.Logger,
	cfg *config.CounterConfig,
) *AggregationConsumer {
	if reader == nil || service == nil || service.redis == nil {
		return nil
	}

	return &AggregationConsumer{
		reader:   reader,
		service:  service,
		logger:   logger,
		commitFn: reader.CommitMessages,
		cfg:      newConsumerConfig(reader, cfg),
		tracker:  newPartitionTracker(),
	}
}

// Start 启动聚合消费主循环。
// 如果启用了 repair,会先启动 repairLoop 后台协程,然后进入 consumeLoop 消费 Kafka 消息。
// 当 ctx 被取消时,reader 会关闭,方法正常退出。
func (c *AggregationConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	defer c.reader.Close()
	defer c.wg.Wait()

	if c.cfg.repairEnabled {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					zap.L().Error("repairLoop panicked", zap.Any("panic", r))
				}
			}()
			c.repairLoop(ctx)
		}()
	}

	c.consumeLoop(ctx)
}

func (c *AggregationConsumer) fetchAndAccept(ctx context.Context, fetchCtx context.Context) error {
	msg, err := c.reader.FetchMessage(fetchCtx)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("counter consumer: fetch context done: %w", err)
		}
		c.logWarn("fetch counter kafka message failed", err)
		if !contextutil.Sleep(ctx, time.Second) {
			return ctx.Err()
		}
		return nil
	}

	c.tracker.mu.Lock()
	flushPartition, _, asyncErr := c.tracker.acceptMessageAsync(c, msg)
	c.tracker.mu.Unlock()

	if asyncErr != nil {
		c.skipMalformedMessage(ctx, msg, asyncErr)
	}

	if flushPartition >= 0 {
		c.flushAndClean(ctx, flushPartition)
	}

	return nil
}

func (c *AggregationConsumer) flushAndClean(ctx context.Context, partition int) {
	c.tracker.mu.Lock()
	batch := c.tracker.batches[partition]
	if batch == nil || batch.size() == 0 {
		c.tracker.mu.Unlock()
		return
	}
	c.tracker.mu.Unlock()
	c.flushAndReset(ctx, batch)
	c.tracker.mu.Lock()
	if batch.size() == 0 {
		delete(c.tracker.batches, partition)
	}
	c.tracker.mu.Unlock()
}

func (c *AggregationConsumer) consumeLoop(ctx context.Context) {
	for {
		c.tracker.mu.Lock()
		expired := c.tracker.expireBatches(time.Now(), c.cfg.flushInterval)
		c.tracker.mu.Unlock()

		for _, partition := range expired {
			c.flushAndClean(ctx, partition)
		}

		if len(expired) > 0 {
			continue
		}

		c.tracker.mu.Lock()

		fetchCtx := ctx
		var cancel context.CancelFunc
		if deadline, ok := nextBatchDeadline(c.tracker, c.cfg.flushInterval); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.tracker.mu.Unlock()
				continue
			}
			fetchCtx, cancel = context.WithTimeout(ctx, remaining)
		}
		c.tracker.mu.Unlock()

		if err := c.fetchAndAccept(ctx, fetchCtx); err != nil {
			if cancel != nil {
				cancel()
			}
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			continue
		}
		if cancel != nil {
			cancel()
		}
	}
}

func (c *AggregationConsumer) flushAndReset(ctx context.Context, batch *counterBatch) {
	defer batch.reset()
	if batch.size() == 0 {
		return
	}

	maxAttempts := c.maxFlushAttempts()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.flushBatch(ctx, batch); err != nil {
			c.logWarn(fmt.Sprintf("flush counter batch failed (attempt %d/%d)", attempt, maxAttempts), err)
			if attempt == maxAttempts {
				c.handleFlushFailure(ctx, batch, err)
				return
			}
			if !contextutil.Sleep(ctx, c.retryDelay()) {
				return
			}
			continue
		}
		return
	}
}

func (c *AggregationConsumer) handleFlushFailure(ctx context.Context, batch *counterBatch, err error) {
	recordCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if recordErr := c.service.recordFailedKafkaMessages(recordCtx, counterFailureStageFlush, batch.messages, err); recordErr != nil {
		c.logWarn("persist counter failed messages failed", recordErr)
	}
	if markErr := c.service.markDirtyMembers(recordCtx, batch.collectDirtyMembers()); markErr != nil {
		c.logWarn("mark dirty members after exhausted retries failed", markErr)
	}
	maxAttempts := c.maxFlushAttempts()
	c.logWarn(
		fmt.Sprintf("flush counter batch exhausted retries and will drop batch (attempts=%d, messages=%d)", maxAttempts, batch.size()),
		err,
	)
}

func (c *AggregationConsumer) flushBatch(ctx context.Context, batch *counterBatch) error {
	if batch == nil || batch.size() == 0 {
		return nil
	}

	if err := c.applyBatch(ctx, batch); err != nil {
		return fmt.Errorf("apply counter batch: %w", err)
	}

	if err := c.commitMessages(ctx, batch.messages...); err != nil {
		return fmt.Errorf("commit counter batch: %w", err)
	}
	return nil
}

func (c *AggregationConsumer) applyBatch(ctx context.Context, batch *counterBatch) error {
	if batch == nil || batch.size() == 0 {
		return nil
	}

	appliedKey, err := c.appliedOffsetKey(batch.partition)
	if err != nil {
		return fmt.Errorf("counter consumer: applied offset key: %w", err)
	}

	cntKeys, keyIndexes := batch.cntKeys()
	keys := make([]string, 0, len(cntKeys)+1)
	keys = append(keys, appliedKey)
	keys = append(keys, cntKeys...)

	args := make([]any, 0, 3+len(batch.events)*4)
	args = append(args, SchemaLen, FieldSize, len(batch.events))
	for _, event := range batch.events {
		args = append(args,
			event.offset,
			keyIndexes[DirtyMember(event.entityType, event.entityID)],
			event.index,
			event.delta,
		)
	}

	return applyPartitionBatchScript.Run(ctx, c.service.redis, keys, args...).Err()
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

func (c *AggregationConsumer) commitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if c != nil && c.commitFn != nil {
		return c.commitFn(ctx, msgs...)
	}
	if c != nil && c.reader != nil {
		return c.reader.CommitMessages(ctx, msgs...)
	}
	return nil
}

// acceptMessage 将消息纳入分区 batch，在 gap 或 batch 满时触发 flush。

func (c *AggregationConsumer) appliedOffsetKey(partition int) (string, error) {
	if c == nil || c.cfg.groupID == "" || c.cfg.topic == "" {
		return "", fmt.Errorf("counter consumer applied offset scope is empty")
	}
	return AppliedOffsetKey(c.cfg.groupID, c.cfg.topic, partition), nil
}

func (c *AggregationConsumer) advanceAppliedOffset(ctx context.Context, partition int, offset int64) error {
	appliedKey, err := c.appliedOffsetKey(partition)
	if err != nil {
		return fmt.Errorf("counter consumer: applied offset key: %w", err)
	}
	return advanceAppliedOffsetScript.Run(ctx, c.service.redis, []string{appliedKey}, offset).Err()
}

func (c *AggregationConsumer) maxFlushAttempts() int {
	if c != nil && c.cfg.flushMaxAttempts > 0 {
		return c.cfg.flushMaxAttempts
	}
	return defaultCounterFlushMaxAttempts
}

func (c *AggregationConsumer) retryDelay() time.Duration {
	if c != nil && c.cfg.flushRetryDelay > 0 {
		return c.cfg.flushRetryDelay
	}
	return defaultCounterFlushRetryDelay
}

func (c *AggregationConsumer) repairLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.repairInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.repairAsLeader(ctx); err != nil {
				c.logWarn("repair dirty counters failed", err)
			}
		}
	}
}

func (c *AggregationConsumer) repairAsLeader(ctx context.Context) error {
	lock, locked, err := redislock.TryAcquire(ctx, c.service.redis, counterRepairLeaderLockKey, counterRepairLockOptions())
	if err != nil {
		return fmt.Errorf("counter consumer: acquire repair lock: %w", err)
	}
	if !locked {
		return nil
	}
	defer lock.Release()

	if err := c.repairDirtyMembers(ctx); err != nil {
		return fmt.Errorf("counter consumer: repair dirty members: %w", err)
	}
	return nil
}

func (c *AggregationConsumer) repairDirtyMembers(ctx context.Context) error {
	members, err := c.listDirtyMembers(ctx, c.cfg.repairBatch)
	if err != nil || len(members) == 0 {
		return err
	}

	var firstErr error
	for _, member := range members {
		if err := c.repairDirtyMember(ctx, member); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *AggregationConsumer) listDirtyMembers(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	members := make([]string, 0, limit)
	var cursor uint64
	for len(members) < limit {
		items, next, err := c.service.redis.SScan(ctx, DirtySetKey(), cursor, "", int64(limit-len(members))).Result()
		if err != nil {
			return nil, fmt.Errorf("scan dirty members: %w", err)
		}
		members = append(members, items...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return members, nil
}

func (c *AggregationConsumer) repairDirtyMember(ctx context.Context, member string) error {
	entityType, entityID, err := ParseDirtyMember(member)
	if err != nil {
		if clearErr := c.service.clearDirtyMembers(ctx, []string{member}); clearErr != nil {
			return fmt.Errorf("drop invalid dirty member: %w", clearErr)
		}
		return nil
	}

	lockKey := fmt.Sprintf("lock:sds-rebuild:%s:%s", entityType, entityID)
	lock, locked, err := redislock.TryAcquire(ctx, c.service.redis, lockKey, c.service.rebuildLockOptions)
	if err != nil {
		return fmt.Errorf("repair dirty member: acquire lock: %w", err)
	}
	if !locked {
		return nil
	}
	defer lock.Release()

	raw, err := c.service.buildSnapshotFromBitmap(ctx, entityType, entityID)
	if err != nil {
		return fmt.Errorf("repair dirty member: build snapshot: %w", err)
	}
	if err := c.service.redis.Set(ctx, SdsKey(entityType, entityID), raw, 0).Err(); err != nil {
		return fmt.Errorf("repair dirty member: set sds: %w", err)
	}
	if err := c.service.clearDirtyMembers(ctx, []string{member}); err != nil {
		return fmt.Errorf("repair dirty member: clear dirty: %w", err)
	}
	c.service.resetBackoff(ctx, entityType, entityID)
	return nil
}

func (c *AggregationConsumer) logWarn(msg string, err error) {
	if c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}
