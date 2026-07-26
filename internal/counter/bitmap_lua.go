package counter

import (
	"github.com/redis/go-redis/v9"
)

// toggleLua 以原子方式切换位图中的单个位，并返回状态是否发生变化。
//
// 功能：如果操作是 "add"，当前位为 0 则设为 1 并返回 1；已为 1 则返回 0。
// 如果操作是 "remove"，当前位为 1 则设为 0 并返回 1；已为 0 则返回 0。
// 无效的操作（非 add/remove）返回 -1。
//
// KEYS[1]：位图键（bm:{metric}:{entityType}:{entityID}:{chunk}）
// KEYS[2]：时间序索引键（likers:{metric}:{entityType}:{entityID}，ZSet）
// ARGV[1]：位偏移（用户 ID 在分片内的位置）
// ARGV[2]：操作类型（"add" 或 "remove"）
// ARGV[3]：用户 ID（ZSet member）
// ARGV[4]：当前 Unix 秒（ZSet score）
//
// WHY 在同一段 Lua 里维护两个结构：
//
//	Bitmap 是状态真值（O(1) 判定、极省内存），但只能按 userID 枚举；
//	「谁赞了我」的产品语义是按**时间**倒序，需要 ZSet(score=时间) 做索引。
//	两个结构必须原子同步——分两次调用会在崩溃窗口留下真值与索引不一致。
//
// 返回值：1=状态发生变化，0=无变化，-1=未知操作
const toggleLua = `
local bmKey = KEYS[1]
local zKey = KEYS[2]
local offset = tonumber(ARGV[1])
local op = ARGV[2]
local member = ARGV[3]
local now = ARGV[4]
local prev = redis.call('GETBIT', bmKey, offset)
if op == 'add' then
  if prev == 1 then return 0 end
  redis.call('SETBIT', bmKey, offset, 1)
  redis.call('ZADD', zKey, now, member)
  return 1
elseif op == 'remove' then
  if prev == 0 then return 0 end
  redis.call('SETBIT', bmKey, offset, 0)
  redis.call('ZREM', zKey, member)
  return 1
end
return -1
`

// rateLimitLua 原子递增限流计数器并设置过期时间。
//
// 解决 INCR + 条件 EXPIRE 的竞态条件：
//
//	如果 INCR 和 EXPIRE 分开发送，两个并发请求都可能在 INCR 后看到 val > 1，
//	从而都跳过 EXPIRE，导致限流键永不过期。
//	Lua 脚本在 Redis 中原子执行，保证 INCR 和 EXPIRE 不可分割。
//
// KEYS[1]：限流键（rl:sds-rebuild:{entityType}:{entityID}）
// ARGV[1]：过期时间（秒）
//
// 返回值：递增后的计数值
const rateLimitLua = `
local val = redis.call('INCR', KEYS[1])
if val == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return val
`

var rateLimitScript = redis.NewScript(rateLimitLua)
