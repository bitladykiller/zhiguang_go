package counter

import "github.com/redis/go-redis/v9"

// TOGGLE_LUA 以原子方式切换位图中的单个位，并返回状态是否发生变化。
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

// SET_SDS_FIELD_LUA 按绝对值覆写 SDS 中的单个槽位。
const SET_SDS_FIELD_LUA = `
local cntKey = KEYS[1]
local schemaLen = tonumber(ARGV[1])
local fieldSize = tonumber(ARGV[2])
local idx = tonumber(ARGV[3])
local value = tonumber(ARGV[4])
local function write32be(n)
  local t = {}
  for i=4,1,-1 do t[i] = n % 256; n = math.floor(n/256) end
  return string.char(unpack(t))
end
local cnt = redis.call('GET', cntKey)
if not cnt then cnt = string.rep(string.char(0), schemaLen * fieldSize) end
local off = (idx - 1) * fieldSize
local seg = write32be(value)
cnt = string.sub(cnt, 1, off) .. seg .. string.sub(cnt, off+fieldSize+1)
redis.call('SET', cntKey, cnt)
return 1
`

// RATE_LIMIT_LUA 原子递增限流计数器并设置过期时间。
const RATE_LIMIT_LUA = `
local val = redis.call('INCR', KEYS[1])
if val == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return val
`

var rateLimitScript = redis.NewScript(RATE_LIMIT_LUA)
