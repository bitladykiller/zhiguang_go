package counter

import "github.com/redis/go-redis/v9"

// TOGGLE_LUA 以原子方式切换位图中的单个位，并返回状态是否发生变化。
//
// 功能：如果操作是 "add"，当前位为 0 则设为 1 并返回 1；已为 1 则返回 0。
// 如果操作是 "remove"，当前位为 1 则设为 0 并返回 1；已为 0 则返回 0。
// 无效的操作（非 add/remove）返回 -1。
//
// KEYS[1]：位图键（bm:{metric}:{entityType}:{entityID}:{chunk}）
// ARGV[1]：位偏移（用户 ID 在分片内的位置）
// ARGV[2]：操作类型（"add" 或 "remove"）
//
// 返回值：1=状态发生变化，0=无变化，-1=未知操作
const TOGGLE_LUA = `
local bmKey = KEYS[1]
local offset = tonumber(ARGV[1])
local op = ARGV[2]
local prev = redis.call('GETBIT', bmKey, offset)
if op == 'add' then
  if prev == 1 then return 0 end
  redis.call('SETBIT', bmKey, offset, 1)
  return 1
elseif op == 'remove' then
  if prev == 0 then return 0 end
  redis.call('SETBIT', bmKey, offset, 0)
  return 1
end
return -1
`

// INCR_SDS_FIELD_LUA 原子递增指定 SDS 槽位。
//
// 功能：
//  1. 读取 Redis 中 SDS 键的二进制值。
//  2. 如果键不存在，初始化为全 0（长度为 schemaLen × fieldSize）。
//  3. 在指定槽位（idx）上增加 delta（可为正数或负数）。
//  4. 如果结果 < 0 则截断为 0（不允许负数计数）。
//  5. 写回 Redis。
//
// KEYS[1]：SDS 键（cnt:{entityType}:{entityID}）
// ARGV[1]：schemaLen（5，指标个数）
// ARGV[2]：fieldSize（4，每个指标占的字节数）
// ARGV[3]：idx（Lua 中槽位索引，从 1 开始，对应 Go 的 idx+1）
// ARGV[4]：delta（增量，+1 或 -1）
//
// Lua 辅助函数说明：
//   - read32be(s, off): 从字符串 s 的 off 偏移处读取 4 字节大端 uint32
//     通过 string.byte 逐字节取出，手动拼装为整数
//   - write32be(n): 将整数 n 编码为 4 字节大端字符串
//     通过取模运算逐字节分解，用 string.char 组装
//   - string.sub(s, 1, off): 取字符串从开头到 off 的子串
//   - string.sub(s, off+fieldSize+1): 取字符串从 off+fieldSize+1 到末尾的子串
//     两者拼接起来替换掉中间的 fieldSize 字节，实现定点写入
const INCR_SDS_FIELD_LUA = `
local cntKey = KEYS[1]
local schemaLen = tonumber(ARGV[1])
local fieldSize = tonumber(ARGV[2])
local idx = tonumber(ARGV[3])
local delta = tonumber(ARGV[4])
local function read32be(s, off)
  local b = {string.byte(s, off+1, off+4)}
  local n = 0
  for i=1,4 do n = n * 256 + b[i] end
  return n
end
local function write32be(n)
  local t = {}
  for i=4,1,-1 do t[i] = n % 256; n = math.floor(n/256) end
  return string.char(unpack(t))
end
local cnt = redis.call('GET', cntKey)
if not cnt then cnt = string.rep(string.char(0), schemaLen * fieldSize) end
local off = (idx - 1) * fieldSize
local v = read32be(cnt, off) + delta
if v < 0 then v = 0 end
local seg = write32be(v)
cnt = string.sub(cnt, 1, off) .. seg .. string.sub(cnt, off+fieldSize+1)
redis.call('SET', cntKey, cnt)
return 1
`

// RATE_LIMIT_LUA 原子递增限流计数器并设置过期时间。
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
const RATE_LIMIT_LUA = `
local val = redis.call('INCR', KEYS[1])
if val == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return val
`

var rateLimitScript = redis.NewScript(RATE_LIMIT_LUA)
