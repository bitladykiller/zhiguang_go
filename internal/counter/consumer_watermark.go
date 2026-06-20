package counter

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// APPLY_PARTITION_BATCH_LUA 按 partition 内的 offset 顺序原子应用一批计数事件。
//
// 设计目标：
//   - 使用共享水位线 applied_offset 实现跨实例幂等。
//   - 允许一批消息中出现"前缀已应用、后缀未应用"的部分重放。
//   - 只有 offset 推进时才更新 cnt:*，offset <= current 时跳过已应用消息。
//   - 检查 rebuilding 标记，若修复中则跳过该实体的 HINCRBY，防止覆盖重建值。
//
// KEYS:
//   - KEYS[1]：applied_offset key
//   - KEYS[2..entityCount+1]：本批次涉及到的 cnt:* Redis Hash 键
//   - KEYS[entityCount+2..2*entityCount+1]：rebuilding 标记键（与 cnt:* 一一对应）
//
// ARGV:
//   - ARGV[1]：eventCount
//   - ARGV[2]：entityCount
//   - 随后每 4 个参数为一个事件：
//   - offset, keyIndex, field, delta
//
// 返回值：
//   - 返回应用后的最新水位线（即 max applied offset）。
const APPLY_PARTITION_BATCH_LUA = `
local appliedKey = KEYS[1]
local eventCount = tonumber(ARGV[1])
local entityCount = tonumber(ARGV[2])

local current = tonumber(redis.call('GET', appliedKey) or '-1')
local lastApplied = current
local pos = 3

for i = 1, eventCount do
  local offset = tonumber(ARGV[pos]); pos = pos + 1
  local keyIndex = tonumber(ARGV[pos]); pos = pos + 1
  local field = ARGV[pos]; pos = pos + 1
  local delta = tonumber(ARGV[pos]); pos = pos + 1

  if offset <= current then
    -- 这条消息已经在之前的 flush 中成功落过 Redis，直接跳过。
  else
    if lastApplied >= 0 and offset <= lastApplied then
      return redis.error_reply('counter offset gap')
    end

    local cntKey = KEYS[keyIndex + 2]
    -- 检查 rebuilding 标记（cnt: 之后的 rebuild keys 从 entityCount + 2 开始）
    local rebuildKey = KEYS[keyIndex + 2 + entityCount]
    if redis.call('EXISTS', rebuildKey) == 0 then
      -- 实体未被 rebuild 锁定，使用 HINCRBY 原子递增
      redis.call('HINCRBY', cntKey, field, delta)
      local val = redis.call('HGET', cntKey, field)
      if tonumber(val) < 0 then
        redis.call('HSET', cntKey, field, 0)
      end
    end
    -- 若实体正在被 repair，跳过增量 flush 防止覆盖重建绝对值

    lastApplied = offset
  end
end

if lastApplied > current then
  redis.call('SET', appliedKey, lastApplied)
end

return lastApplied
`

// ADVANCE_APPLIED_OFFSET_LUA 在不修改 cnt:* 的前提下推进共享水位线。
//
// 用途：
//   - 对于无法解析、字段非法等"明确要丢弃"的消息，也要推进 partition 水位线，
//     否则后续合法消息会因为 offset 空洞永远无法应用。
//
// KEYS[1]：applied_offset key
// ARGV[1]：目标 offset
const ADVANCE_APPLIED_OFFSET_LUA = `
local appliedKey = KEYS[1]
local target = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', appliedKey) or '-1')

if target <= current then
  return current
end

redis.call('SET', appliedKey, target)
return target
`

var (
	applyPartitionBatchScript  = redis.NewScript(APPLY_PARTITION_BATCH_LUA)
	advanceAppliedOffsetScript = redis.NewScript(ADVANCE_APPLIED_OFFSET_LUA)
)

func AppliedOffsetKey(groupID, topic string, partition int) string {
	return fmt.Sprintf("counter:applied-offset:%s:%s:%d", groupID, topic, partition)
}
