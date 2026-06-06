package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/redislock"
)

const decrAggFieldLua = `
local key = KEYS[1]
local field = ARGV[1]
local delta = tonumber(ARGV[2])
local v = redis.call('HINCRBY', key, field, -delta)
if v == 0 then
  redis.call('HDEL', key, field)
end
return v
`

const aggregationFlushLeaderLockKey = "lock:counter:aggregation:flush"

// AggregationConsumer 消费 counter-events，并周期性折叠到 SDS。
type AggregationConsumer struct {
	reader        *kafka.Reader
	redis         *redis.Client
	logger        *zap.Logger
	flushInterval time.Duration
}

func NewAggregationConsumer(reader *kafka.Reader, redisClient *redis.Client, logger *zap.Logger) *AggregationConsumer {
	if reader == nil || redisClient == nil {
		return nil
	}
	return &AggregationConsumer{
		reader:        reader,
		redis:         redisClient,
		logger:        logger,
		flushInterval: time.Second,
	}
}

func (c *AggregationConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}
	defer c.reader.Close()

	go c.flushLoop(ctx)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if c.logger != nil {
				c.logger.Warn("fetch counter kafka message failed", zap.Error(err))
			}
			if !sleepCounterConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.handleMessage(ctx, msg.Value); err != nil {
			if c.logger != nil {
				c.logger.Warn("process counter kafka message failed", zap.Error(err))
			}
			if !sleepCounterConsumer(ctx, time.Second) {
				return
			}
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil && c.logger != nil {
			c.logger.Warn("commit counter kafka message failed", zap.Error(err))
		}
	}
}

func (c *AggregationConsumer) handleMessage(ctx context.Context, value []byte) error {
	var evt CounterEvent
	if err := json.Unmarshal(value, &evt); err != nil {
		return err
	}
	aggKey := AggKey(evt.EntityType, evt.EntityID)
	field := strconv.Itoa(evt.Index)
	return c.redis.HIncrBy(ctx, aggKey, field, int64(evt.Delta)).Err()
}

func (c *AggregationConsumer) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.flushAsLeader(ctx); err != nil && c.logger != nil {
				c.logger.Warn("flush counter aggregation failed", zap.Error(err))
			}
		}
	}
}

// flushAsLeader 使用全局 leader 锁保证同一时刻只有一个实例执行 agg:* 扫描与折叠。
//
// WHY 这里使用“全局 flush 锁”而不是“每个 aggKey 单独加锁”：
//   - 多实例下最明显的问题是每个实例都会独立 SCAN agg:*，Redis 扫描开销会被线性放大。
//   - 让每一轮 flush 只有一个 leader 执行，既保证正确性，也减少 Redis 背景扫描压力。
func (c *AggregationConsumer) flushAsLeader(ctx context.Context) error {
	lock, locked, err := redislock.TryAcquire(ctx, c.redis, aggregationFlushLeaderLockKey, aggregationFlushLockOptions())
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}
	defer lock.Release()
	return c.flush(ctx)
}

func (c *AggregationConsumer) flush(ctx context.Context) error {
	var cursor uint64
	pattern := "agg:*"
	for {
		keys, next, err := c.redis.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := c.flushAggregation(ctx, key); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func (c *AggregationConsumer) flushAggregation(ctx context.Context, aggKey string) error {
	entries, err := c.redis.HGetAll(ctx, aggKey).Result()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		_ = c.redis.Del(ctx, aggKey).Err()
		return nil
	}

	entityType, entityID, err := parseAggKey(aggKey)
	if err != nil {
		return err
	}
	cntKey := SdsKey(entityType, entityID)

	for field, rawDelta := range entries {
		idx, err := strconv.Atoi(field)
		if err != nil || idx < 0 {
			continue
		}
		delta, err := strconv.ParseInt(rawDelta, 10, 64)
		if err != nil || delta == 0 {
			continue
		}

		if err := c.redis.Eval(ctx, INCR_SDS_FIELD_LUA, []string{cntKey}, SchemaLen, FieldSize, idx+1, delta).Err(); err != nil {
			return err
		}
		if err := c.redis.Eval(ctx, decrAggFieldLua, []string{aggKey}, field, delta).Err(); err != nil {
			return err
		}
	}

	size, err := c.redis.HLen(ctx, aggKey).Result()
	if err == nil && size == 0 {
		_ = c.redis.Del(ctx, aggKey).Err()
	}
	return nil
}

func parseAggKey(key string) (string, string, error) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("invalid aggregation key: %s", key)
	}
	return parts[1], parts[2], nil
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
