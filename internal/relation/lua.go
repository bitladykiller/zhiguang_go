package relation

// TOKEN_BUCKET_LUA 实现一个通用令牌桶限流器。
// ARGV[1] = capacity, ARGV[2] = rate, ARGV[3] = 当前时间（毫秒时间戳）。
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local last = redis.call('HGET', key, 'last')
local tokens = redis.call('HGET', key, 'tokens')
if not last then last = now; tokens = capacity end
local elapsed = tonumber(now) - tonumber(last)
local add = elapsed * rate
tokens = math.min(capacity, tonumber(tokens) + add)
if tokens < 1 then
  redis.call('HSET', key, 'last', now)
  redis.call('HSET', key, 'tokens', tokens)
  return 0
end
tokens = tokens - 1
redis.call('HSET', key, 'last', now)
redis.call('HSET', key, 'tokens', tokens)
redis.call('PEXPIRE', key, 60000)
return 1
`
