package counter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
)

// AggregationConsumer 消费 counter-events，并按批次把增量直接折叠到 cnt:*。
//
// 这里不再使用 Redis agg:* 中转桶，原因是当前方案把“批量聚合”放在 MQ 消费端：
//   - 同一批 Kafka 消息先在进程内做内存聚合。
//   - 到达批次大小或时间窗口后，一次性把 delta flush 到 cnt:*。
//   - 如果 publish、flush 或 offset commit 出现失败，对应实体会进入 dirty set，
//     再由 repair loop 用位图的绝对值覆盖 cnt:*。
type AggregationConsumer struct {
	reader           *kafka.Reader
	service          *CounterService
	logger           *zap.Logger
	commitFn         func(ctx context.Context, msgs ...kafka.Message) error
	groupID          string
	topic            string
	batchSize        int
	flushInterval    time.Duration
	flushRetryDelay  time.Duration
	flushMaxAttempts int
	repairEnabled    bool
	repairInterval   time.Duration
	repairBatch      int
}

func NewAggregationConsumer(
	reader *kafka.Reader,
	service *CounterService,
	logger *zap.Logger,
	cfg *config.CounterConfig,
) *AggregationConsumer {
	if reader == nil || service == nil || service.redis == nil {
		return nil
	}

	batchSize := 100
	flushInterval := time.Second
	repairEnabled := false
	repairInterval := time.Minute
	repairBatch := batchSize

	if cfg != nil {
		if cfg.Consumer.BatchSize > 0 {
			batchSize = cfg.Consumer.BatchSize
		}
		if cfg.Consumer.FlushIntervalMs > 0 {
			flushInterval = time.Duration(cfg.Consumer.FlushIntervalMs) * time.Millisecond
		}
		repairEnabled = cfg.Repair.Enabled
		if cfg.Repair.IntervalMs > 0 {
			repairInterval = time.Duration(cfg.Repair.IntervalMs) * time.Millisecond
		}
		if cfg.Repair.BatchSize > 0 {
			repairBatch = cfg.Repair.BatchSize
		}
	}

	readerCfg := reader.Config()

	return &AggregationConsumer{
		reader:           reader,
		service:          service,
		logger:           logger,
		commitFn:         reader.CommitMessages,
		groupID:          readerCfg.GroupID,
		topic:            readerCfg.Topic,
		batchSize:        batchSize,
		flushInterval:    flushInterval,
		flushRetryDelay:  defaultCounterFlushRetryDelay,
		flushMaxAttempts: defaultCounterFlushMaxAttempts,
		repairEnabled:    repairEnabled,
		repairInterval:   repairInterval,
		repairBatch:      repairBatch,
	}
}

func (c *AggregationConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	defer c.reader.Close()

	if c.repairEnabled {
		go c.repairLoop(ctx)
	}

	c.consumeLoop(ctx)
}

func (c *AggregationConsumer) consumeLoop(ctx context.Context) {
	batches := make(map[int]*counterBatch)

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
				if !contextutil.Sleep(ctx, time.Second) {
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
			if !contextutil.Sleep(ctx, time.Second) {
				return
			}
			continue
		}
		if err := c.acceptMessage(ctx, batches, msg); err != nil {
			c.logWarn("accept counter kafka message failed", err)
		}
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
			if attempt == maxAttempts {
				recordCtx, cancel := context.WithTimeout(ctx, time.Second)
				if recordErr := c.service.recordFailedKafkaMessages(recordCtx, counterFailureStageFlush, batch.messages, err); recordErr != nil {
					c.logWarn("persist counter failed messages failed", recordErr)
				}
				if markErr := c.service.markDirtyMembers(recordCtx, batch.collectDirtyMembers()); markErr != nil {
					c.logWarn("mark dirty members after exhausted retries failed", markErr)
				}
				cancel()
				c.logWarn(
					fmt.Sprintf("flush counter batch exhausted retries and will drop batch (attempts=%d, messages=%d)", maxAttempts, batch.size()),
					err,
				)
				break
			}
			if !contextutil.Sleep(ctx, c.retryDelay()) {
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
		return err
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

func (c *AggregationConsumer) repairLoop(ctx context.Context) {
	ticker := time.NewTicker(c.repairInterval)
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
		return err
	}
	if !locked {
		return nil
	}
	defer lock.Release()

	return c.repairDirtyMembers(ctx)
}

func (c *AggregationConsumer) repairDirtyMembers(ctx context.Context) error {
	members, err := c.listDirtyMembers(ctx, c.repairBatch)
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
			return nil, err
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
		return err
	}
	if !locked {
		return nil
	}
	defer lock.Release()

	raw, err := c.service.buildSnapshotFromBitmap(ctx, entityType, entityID)
	if err != nil {
		return err
	}
	if err := c.service.redis.Set(ctx, SdsKey(entityType, entityID), raw, 0).Err(); err != nil {
		return err
	}
	if err := c.service.clearDirtyMembers(ctx, []string{member}); err != nil {
		return err
	}
	c.service.resetBackoff(ctx, entityType, entityID)
	return nil
}

func (c *AggregationConsumer) logWarn(msg string, err error) {
	if c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}

type counterBatch struct {
	partition   int
	openedAt    time.Time
	startOffset int64
	endOffset   int64
	messages    []kafka.Message
	events      []counterBatchEvent
	entities    map[string]struct{}
}

type counterBatchEvent struct {
	offset     int64
	entityType string
	entityID   string
	index      int
	delta      int
}

func newCounterBatch(capacity int) *counterBatch {
	if capacity <= 0 {
		capacity = 1
	}
	return &counterBatch{
		partition: -1,
		messages:  make([]kafka.Message, 0, capacity),
		events:    make([]counterBatchEvent, 0, capacity),
		entities:  make(map[string]struct{}, capacity),
	}
}

func (b *counterBatch) add(msg kafka.Message) error {
	evt, err := parseCounterEvent(msg.Value)
	if err != nil {
		return err
	}
	return b.addEvent(msg, evt)
}

func (b *counterBatch) addEvent(msg kafka.Message, evt CounterEvent) error {
	if evt.EntityType == "" || evt.EntityID == "" {
		return fmt.Errorf("counter event missing entity: %+v", evt)
	}
	if evt.Index < 0 || evt.Index >= SchemaLen {
		return fmt.Errorf("counter event index out of range: %d", evt.Index)
	}
	if evt.Delta == 0 {
		return fmt.Errorf("counter event delta is zero")
	}

	if b.size() == 0 {
		b.partition = msg.Partition
		b.openedAt = time.Now()
		b.startOffset = msg.Offset
		b.endOffset = msg.Offset
	} else {
		if msg.Partition != b.partition {
			return fmt.Errorf("counter batch partition mismatch: got=%d want=%d", msg.Partition, b.partition)
		}
		if msg.Offset != b.endOffset+1 {
			return fmt.Errorf("counter batch offset gap: partition=%d got=%d want=%d", msg.Partition, msg.Offset, b.endOffset+1)
		}
		b.endOffset = msg.Offset
	}

	b.messages = append(b.messages, msg)
	b.events = append(b.events, counterBatchEvent{
		offset:     msg.Offset,
		entityType: evt.EntityType,
		entityID:   evt.EntityID,
		index:      evt.Index,
		delta:      evt.Delta,
	})
	b.entities[DirtyMember(evt.EntityType, evt.EntityID)] = struct{}{}
	return nil
}

func (b *counterBatch) size() int {
	if b == nil {
		return 0
	}
	return len(b.messages)
}

func (b *counterBatch) collectDirtyMembers() []string {
	members := make([]string, 0, len(b.entities))
	for member := range b.entities {
		members = append(members, member)
	}
	return members
}

func (b *counterBatch) cntKeys() ([]string, map[string]int) {
	members := b.collectDirtyMembers()
	sort.Strings(members)

	keys := make([]string, 0, len(members))
	indexes := make(map[string]int, len(members))
	for i, member := range members {
		entityType, entityID, err := ParseDirtyMember(member)
		if err != nil {
			continue
		}
		keys = append(keys, SdsKey(entityType, entityID))
		indexes[member] = i
	}
	return keys, indexes
}

func (b *counterBatch) reset() {
	if b == nil {
		return
	}
	b.partition = -1
	b.openedAt = time.Time{}
	b.startOffset = 0
	b.endOffset = 0
	b.messages = b.messages[:0]
	b.events = b.events[:0]
	clear(b.entities)
}

func parseCounterEvent(value []byte) (CounterEvent, error) {
	var evt CounterEvent
	if err := json.Unmarshal(value, &evt); err != nil {
		return CounterEvent{}, err
	}
	return evt, nil
}

