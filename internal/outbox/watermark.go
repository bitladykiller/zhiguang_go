package outbox

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const advanceConsumerOffsetLua = `
local appliedKey = KEYS[1]
local target = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', appliedKey) or '-1')

if target <= current then
  return current
end

if current >= 0 and target ~= current + 1 then
  return redis.error_reply('outbox offset gap')
end

redis.call('SET', appliedKey, target)
return target
`

// advanceConsumerOffsetScript 使用 Lua 原子推进共享水位线。
//
// 约束是：
//   - 如果 target 小于等于当前值，说明只是重复投递，直接返回当前水位线
//   - 如果 target 不是 current + 1，说明同分区处理出现空洞，返回错误
//   - 只有严格顺序的下一个 offset 才允许推进
var advanceConsumerOffsetScript = redis.NewScript(advanceConsumerOffsetLua)

// WatermarkTracker 管理 consumer-group/topic 维度的共享 offset 水位线。
//
// 这是 outbox 消费端的主幂等机制。它不记录“某个事件 ID 是否处理过”，
// 而是记录“某个 consumer group 在某个 topic/partition 上已经成功处理到哪里”。
// 对 Kafka 这种分区内有序的日志，这比单纯存事件 ID 更贴近实际投递模型。
type WatermarkTracker struct {
	redis   *redis.Client
	groupID string
	topic   string
}

// NewWatermarkTracker 创建 outbox 消费水位线跟踪器。
//
// 当 redis 或作用域信息不完整时返回 nil，调用方会退化为不启用共享水位线。
func NewWatermarkTracker(redisClient *redis.Client, groupID, topic string) *WatermarkTracker {
	if redisClient == nil || groupID == "" || topic == "" {
		return nil
	}
	return &WatermarkTracker{
		redis:   redisClient,
		groupID: groupID,
		topic:   topic,
	}
}

// LastApplied 返回指定分区当前已经成功处理的最大 offset。
func (t *WatermarkTracker) LastApplied(ctx context.Context, partition int) (int64, error) {
	key, err := t.appliedOffsetKey(partition)
	if err != nil {
		return 0, err
	}

	val, err := t.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}

	offset, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse outbox applied offset: %w", err)
	}
	return offset, nil
}

// Advance 原子推进指定分区的共享水位线。
//
// 这个动作只能在副作用已经成功完成后调用。
// 一旦推进成功，即使随后 Kafka commit 失败，重复投递也会在下次消费时被跳过。
func (t *WatermarkTracker) Advance(ctx context.Context, partition int, offset int64) error {
	key, err := t.appliedOffsetKey(partition)
	if err != nil {
		return err
	}
	return advanceConsumerOffsetScript.Run(ctx, t.redis, []string{key}, offset).Err()
}

func (t *WatermarkTracker) appliedOffsetKey(partition int) (string, error) {
	if t == nil || t.redis == nil || t.groupID == "" || t.topic == "" {
		return "", fmt.Errorf("outbox consumer applied offset scope is empty")
	}
	return AppliedOffsetKey(t.groupID, t.topic, partition), nil
}

// AppliedOffsetKey 返回 outbox 共享水位线键名。
//
// 键名把 consumer group、topic、partition 全部编码进去，避免不同消费者作用域互相污染。
func AppliedOffsetKey(groupID, topic string, partition int) string {
	return fmt.Sprintf("outbox:applied-offset:%s:%s:%d", groupID, topic, partition)
}
