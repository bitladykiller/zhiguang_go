package counter

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// APPLY_PARTITION_BATCH_LUA 按 partition 内的 offset 顺序原子应用一批计数事件。
//
// 设计目标：
//   - 使用共享水位线 applied_offset 实现跨实例幂等。
//   - 允许一批消息中出现“前缀已应用、后缀未应用”的部分重放。
//   - 只有 offset 连续推进时才更新 cnt:*，避免跨 offset 空洞误推进。
//
// KEYS:
//   - KEYS[1]：applied_offset key
//   - KEYS[2..n]：本批次涉及到的 cnt:* 键
//
// ARGV:
//   - ARGV[1]：schemaLen
//   - ARGV[2]：fieldSize
//   - ARGV[3]：eventCount
//   - 随后每 4 个参数为一个事件：
//   - offset, keyIndex, idx, delta
//
// 返回值：
//   - 返回应用后的最新水位线（即 max applied offset）。
const APPLY_PARTITION_BATCH_LUA = `
local appliedKey = KEYS[1]
local schemaLen = tonumber(ARGV[1])
local fieldSize = tonumber(ARGV[2])
local eventCount = tonumber(ARGV[3])

local function read32be(s, off)
  local b = {string.byte(s, off + 1, off + 4)}
  local n = 0
  for i = 1, 4 do n = n * 256 + b[i] end
  return n
end

local function write32be(n)
  local t = {}
  for i = 4, 1, -1 do
    t[i] = n % 256
    n = math.floor(n / 256)
  end
  return string.char(unpack(t))
end

local current = tonumber(redis.call('GET', appliedKey) or '-1')
local lastApplied = current
local pos = 4

for i = 1, eventCount do
  local offset = tonumber(ARGV[pos]); pos = pos + 1
  local keyIndex = tonumber(ARGV[pos]); pos = pos + 1
  local idx = tonumber(ARGV[pos]); pos = pos + 1
  local delta = tonumber(ARGV[pos]); pos = pos + 1

  if offset <= current then
    -- 这条消息已经在之前的 flush 中成功落过 Redis，直接跳过。
  else
    if lastApplied >= 0 and offset ~= lastApplied + 1 then
      return redis.error_reply('counter offset gap')
    end

    local cntKey = KEYS[keyIndex + 2]
    local cnt = redis.call('GET', cntKey)
    if not cnt then
      cnt = string.rep(string.char(0), schemaLen * fieldSize)
    end

    local off = idx * fieldSize
    local v = read32be(cnt, off) + delta
    if v < 0 then v = 0 end
    local seg = write32be(v)
    cnt = string.sub(cnt, 1, off) .. seg .. string.sub(cnt, off + fieldSize + 1)
    redis.call('SET', cntKey, cnt)

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
//   - 对于无法解析、字段非法等“明确要丢弃”的消息，也要推进 partition 水位线，
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

if current >= 0 and target ~= current + 1 then
  return redis.error_reply('counter offset gap')
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
