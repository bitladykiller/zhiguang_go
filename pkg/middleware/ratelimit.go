// Package middleware 中的 RateLimiter 提供基于 Redis ZSET 的分布式滑动窗口限流。
//
// 窗口模型：
//
//	每个 IP 对应一个 ZSET，成员是「一次请求」，score 是该请求发生的毫秒时间戳。
//	判定时先用 ZREMRANGEBYSCORE 剔除窗口外的旧请求，再用 ZCARD 得到窗口内请求数。
//
// WHY score 取自 Redis 服务端 TIME 而非应用进程时钟：
//
//	多实例部署时各进程时钟存在偏差，用本地时间会让窗口边界抖动。
//	统一以 Redis 的时间为准，窗口在所有实例上语义一致。
//
// WHY 成员必须逐请求唯一（见 nextRateLimitMember）：
//
//	ZADD 对相同 member 是「更新 score」而非「新增元素」。
//	若把时间戳同时当作 score 和 member，同一毫秒内的并发请求会互相覆盖，
//	ZCARD 永远只记 1，突发流量可以完全绕过限流——正是限流最需要拦住的场景。
//	因此 member 由「进程唯一前缀 + 进程内自增序号」生成，保证跨实例、跨请求不重复。
package middleware

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/config"
)

// rateLimitScript 原子地执行「清理过期成员 → 统计 → 放行或拒绝」。
//
//	KEYS[1] = 该 IP 的滑动窗口 ZSET
//	ARGV[1] = 窗口长度（毫秒）
//	ARGV[2] = 窗口内允许的最大请求数
//	ARGV[3] = 本次请求的唯一成员标识
//
// 返回 {allowed, remaining}：allowed 为 1 表示放行，remaining 是放行后窗口内的剩余配额。
// 拒绝时 remaining 恒为 0。
var rateLimitScript = redis.NewScript(`
local key = KEYS[1]
local windowMs = tonumber(ARGV[1])
local maxRequests = tonumber(ARGV[2])
local member = ARGV[3]

local now = redis.call('TIME')
local currentMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local windowStart = currentMs - windowMs

redis.call('ZREMRANGEBYSCORE', key, 0, windowStart)
local count = redis.call('ZCARD', key)

if count >= maxRequests then
  -- 仍然续期，避免持续打满的 key 因过期而重置窗口
  redis.call('PEXPIRE', key, windowMs)
  return {0, 0}
end

redis.call('ZADD', key, currentMs, member)
redis.call('PEXPIRE', key, windowMs)
return {1, maxRequests - count - 1}
`)

// rateLimitMemberPrefix 是进程启动时生成的随机前缀。
//
// 多个实例共享同一个 Redis ZSET，仅靠进程内自增序号无法避免跨实例撞号，
// 因此用随机前缀把各实例的成员空间隔开。
var rateLimitMemberPrefix = newRateLimitMemberPrefix()

// rateLimitSeq 是进程内单调递增的请求序号。
var rateLimitSeq atomic.Uint64

func newRateLimitMemberPrefix() string {
	var b [6]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand 不可用时退化为纳秒时间戳，仍能保证同一实例内唯一。
		return strconv.FormatInt(time.Now().UnixNano(), 36) + ":"
	}
	return base64.RawURLEncoding.EncodeToString(b[:]) + ":"
}

// nextRateLimitMember 返回本次请求在 ZSET 中的唯一成员标识。
func nextRateLimitMember() string {
	return rateLimitMemberPrefix + strconv.FormatUint(rateLimitSeq.Add(1), 36)
}

// RateLimiter 是按客户端 IP 限流的 Gin 中间件工厂。
type RateLimiter struct {
	redisClient *redis.Client
	cfg         config.RateLimitConfig
	logger      *zap.Logger
}

// NewRateLimiter 创建限流器。redisClient 为 nil 或配置未启用时，中间件退化为直接放行。
func NewRateLimiter(redisClient *redis.Client, cfg config.RateLimitConfig, logger *zap.Logger) *RateLimiter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &RateLimiter{
		redisClient: redisClient,
		cfg:         cfg,
		logger:      logger,
	}
}

// Middleware 返回限流中间件。
//
// 判定顺序：封禁名单 → 滑动窗口。Redis 故障时 fail-open（放行并告警），
// 避免限流组件自身的可用性问题演变成整站不可用。
func (rl *RateLimiter) Middleware() gin.HandlerFunc {
	if !rl.cfg.Enabled || rl.redisClient == nil {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	return func(c *gin.Context) {
		ip := rl.clientIP(c)
		ctx := c.Request.Context()

		banKey := "ratelimit:ban:" + ip
		banned, err := rl.redisClient.Exists(ctx, banKey).Result()
		if err == nil && banned > 0 {
			rl.reject(c)
			return
		}

		key := "ratelimit:" + ip
		res, err := rateLimitScript.Run(
			ctx, rl.redisClient, []string{key},
			rl.cfg.WindowMs, rl.cfg.PerIP, nextRateLimitMember(),
		).Int64Slice()
		if err != nil || len(res) != 2 {
			rl.logger.Warn("rate limit script failed", zap.String("ip", ip), zap.Error(err))
			c.Next()
			return
		}

		if res[0] == 0 {
			rl.ban(ctx, banKey, ip)
			rl.reject(c)
			return
		}

		c.Header("X-RateLimit-Limit", strconv.Itoa(rl.cfg.PerIP))
		c.Header("X-RateLimit-Remaining", strconv.FormatInt(res[1], 10))
		c.Next()
	}
}

// clientIP 解析客户端 IP，ClientIP() 为空时从 RemoteAddr 兜底。
//
// 用 net.SplitHostPort 而非按 ":" 切分：IPv6 地址本身含冒号，
// 按冒号切分会把 "[::1]:8080" 截成 "[",导致所有 IPv6 客户端共用一个限流桶。
func (rl *RateLimiter) clientIP(c *gin.Context) string {
	if ip := c.ClientIP(); ip != "" {
		return ip
	}
	if host, _, err := net.SplitHostPort(c.Request.RemoteAddr); err == nil {
		return host
	}
	return c.Request.RemoteAddr
}

// ban 在配置了封禁时长时把该 IP 加入封禁名单。
func (rl *RateLimiter) ban(ctx context.Context, banKey, ip string) {
	if rl.cfg.BanDurationMs <= 0 {
		return
	}
	banDuration := time.Duration(rl.cfg.BanDurationMs) * time.Millisecond
	if err := rl.redisClient.Set(ctx, banKey, "1", banDuration).Err(); err != nil {
		rl.logger.Warn("failed to set ban key", zap.String("ip", ip), zap.Error(err))
	}
}

// reject 统一输出 429 响应。
func (rl *RateLimiter) reject(c *gin.Context) {
	c.Header("X-RateLimit-Limit", strconv.Itoa(rl.cfg.PerIP))
	c.Header("X-RateLimit-Remaining", "0")
	if rl.cfg.WindowMs > 0 {
		retryAfter := (rl.cfg.WindowMs + 999) / 1000
		c.Header("Retry-After", strconv.Itoa(retryAfter))
	}
	c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
		"code":    429,
		"message": "too many requests, please try again later",
	})
}
