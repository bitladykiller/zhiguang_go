package counter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
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

	return &AggregationConsumer{
		reader:           reader,
		service:          service,
		logger:           logger,
		commitFn:         reader.CommitMessages,
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
	batch := newCounterBatch(c.batchSize)
	var openedAt time.Time

	for {
		if batch.size() == 0 {
			msg, err := c.reader.FetchMessage(ctx)
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

			if err := batch.add(msg); err != nil {
				c.skipMalformedMessage(ctx, msg, err)
				continue
			}
			openedAt = time.Now()
			if batch.size() >= c.batchSize {
				c.flushAndReset(ctx, batch)
			}
			continue
		}

		remaining := time.Until(openedAt.Add(c.flushInterval))
		if remaining <= 0 {
			c.flushAndReset(ctx, batch)
			continue
		}

		fetchCtx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := c.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				c.flushAndReset(ctx, batch)
				continue
			}
			c.logWarn("fetch counter kafka message failed", err)
			if !sleepCounterConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := batch.add(msg); err != nil {
			c.skipMalformedMessage(ctx, msg, err)
			continue
		}
		if batch.size() >= c.batchSize {
			c.flushAndReset(ctx, batch)
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
				recordCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				if recordErr := c.service.recordFailedKafkaMessages(recordCtx, counterFailureStageFlush, batch.messages, err); recordErr != nil {
					c.logWarn("persist counter failed messages failed", recordErr)
				}
				cancel()
				c.logWarn(
					fmt.Sprintf("flush counter batch exhausted retries and will drop batch (attempts=%d, messages=%d)", maxAttempts, batch.size()),
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

	if !batch.dirtyMarked {
		batch.dirtyMembers = batch.collectDirtyMembers()
		if err := c.service.markDirtyMembers(ctx, batch.dirtyMembers); err != nil {
			return fmt.Errorf("mark dirty members: %w", err)
		}
		batch.dirtyMarked = true
	}

	if !batch.applied {
		if err := c.applyBatch(ctx, batch); err != nil {
			return fmt.Errorf("apply counter batch: %w", err)
		}
		batch.applied = true
	}

	if err := c.commitMessages(ctx, batch.messages...); err != nil {
		return fmt.Errorf("commit counter batch: %w", err)
	}

	if err := c.service.clearDirtyMembers(ctx, batch.dirtyMembers); err != nil {
		c.logWarn("clear dirty members failed after commit", err)
	}
	return nil
}

func (c *AggregationConsumer) applyBatch(ctx context.Context, batch *counterBatch) error {
	pipe := c.service.redis.Pipeline()
	cmds := make([]*redis.Cmd, 0, batch.commandCount())

	for _, entity := range batch.entities {
		cntKey := SdsKey(entity.entityType, entity.entityID)
		for idx, delta := range entity.deltas {
			if delta == 0 {
				continue
			}
			cmds = append(cmds, pipe.Eval(
				ctx,
				INCR_SDS_FIELD_LUA,
				[]string{cntKey},
				SchemaLen,
				FieldSize,
				idx+1,
				delta,
			))
		}
	}

	if len(cmds) == 0 {
		return nil
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (c *AggregationConsumer) skipMalformedMessage(ctx context.Context, msg kafka.Message, cause error) {
	c.logWarn("skip malformed counter kafka message", cause)
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
	messages     []kafka.Message
	entities     map[string]*counterBatchEntity
	dirtyMembers []string
	dirtyMarked  bool
	applied      bool
}

type counterBatchEntity struct {
	entityType string
	entityID   string
	deltas     map[int]int64
}

func newCounterBatch(capacity int) *counterBatch {
	if capacity <= 0 {
		capacity = 1
	}
	return &counterBatch{
		messages: make([]kafka.Message, 0, capacity),
		entities: make(map[string]*counterBatchEntity, capacity),
	}
}

func (b *counterBatch) add(msg kafka.Message) error {
	var evt CounterEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return err
	}
	if evt.EntityType == "" || evt.EntityID == "" {
		return fmt.Errorf("counter event missing entity: %+v", evt)
	}
	if evt.Index < 0 || evt.Index >= SchemaLen {
		return fmt.Errorf("counter event index out of range: %d", evt.Index)
	}
	if evt.Delta == 0 {
		return fmt.Errorf("counter event delta is zero")
	}

	b.messages = append(b.messages, msg)

	key := DirtyMember(evt.EntityType, evt.EntityID)
	entity := b.entities[key]
	if entity == nil {
		entity = &counterBatchEntity{
			entityType: evt.EntityType,
			entityID:   evt.EntityID,
			deltas:     make(map[int]int64, 2),
		}
		b.entities[key] = entity
	}
	entity.deltas[evt.Index] += int64(evt.Delta)
	if entity.deltas[evt.Index] == 0 {
		delete(entity.deltas, evt.Index)
	}
	return nil
}

func (b *counterBatch) size() int {
	if b == nil {
		return 0
	}
	return len(b.messages)
}

func (b *counterBatch) commandCount() int {
	total := 0
	for _, entity := range b.entities {
		total += len(entity.deltas)
	}
	return total
}

func (b *counterBatch) collectDirtyMembers() []string {
	members := make([]string, 0, len(b.entities))
	for member := range b.entities {
		members = append(members, member)
	}
	return members
}

func (b *counterBatch) reset() {
	if b == nil {
		return
	}
	b.messages = b.messages[:0]
	clear(b.entities)
	b.dirtyMembers = nil
	b.dirtyMarked = false
	b.applied = false
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
