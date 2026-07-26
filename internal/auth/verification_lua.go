package auth

import "github.com/redis/go-redis/v9"

// incrAndExpireScript 原子递增键值并在首次递增时设置过期时间。
//
// 解决 INCR + 条件 EXPIRE 的竞态条件：
//
//	两个并发请求都 INCR 后看到 val > 1，都跳过 EXPIRE，导致键永不过期。
//	Lua 脚本在 Redis 中原子执行，保证 INCR 和 EXPIRE 不可分割。
//
// KEYS[1]：目标键
// ARGV[1]：过期时间（秒）
//
// 返回值：递增后的值
var incrAndExpireScript = redis.NewScript(`
local val = redis.call('INCR', KEYS[1])
if val == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return val
`)

// verifyAndCountScript 原子检查尝试次数 + 递增 + 获取验证码。
//
// 解决 Verify 函数中的 TOCTOU 竞态条件：
//
//	原实现先 GET 尝试次数再条件 INCR，两个并发请求可以同时通过上限检查。
//	Lua 脚本将检查和递增合为原子操作，杜绝并发绕过。
//
// KEYS[1]：尝试次数键（vc:attempts:{scene}:{identifier}）
// KEYS[2]：验证码键（vc:code:{scene}:{identifier}）
// ARGV[1]：最大尝试次数
// ARGV[2]：尝试次数键过期时间（秒）
//
// 返回值：
//
//	[1] 尝试次数（递增后），-1 表示已超限
//	[2] 验证码字符串，空字符串表示不存在
var verifyAndCountScript = redis.NewScript(`
local attemptKey = KEYS[1]
local codeKey = KEYS[2]
local maxAttempts = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local attempts = tonumber(redis.call('GET', attemptKey) or '0')
if attempts >= maxAttempts then
    return {-1, ''}
end

local val = redis.call('INCR', attemptKey)
if val == 1 then
    redis.call('EXPIRE', attemptKey, ttl)
end

local code = redis.call('GET', codeKey)
if code then
    return {val, code}
end
return {val, ''}
`)
