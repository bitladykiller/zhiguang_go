package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/zhiguang/app/pkg/config"
	"go.uber.org/zap"
)

var rateLimitIncrScript = redis.NewScript(`
local key = KEYS[1]
local windowMs = tonumber(ARGV[1])
local maxRequests = tonumber(ARGV[2])
local now = redis.call('TIME')
local currentMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local windowStart = currentMs - windowMs

redis.call('ZREMRANGEBYSCORE', key, 0, windowStart)

local count = redis.call('ZCARD', key)

if count >= maxRequests then
  return 0
end

redis.call('ZADD', key, currentMs, currentMs)
redis.call('PEXPIRE', key, windowMs)
return 1
`)

type RateLimiter struct {
	redisClient *redis.Client
	cfg         config.RateLimitConfig
	logger      *zap.Logger
}

func NewRateLimiter(redisClient *redis.Client, cfg config.RateLimitConfig, logger *zap.Logger) *RateLimiter {
	return &RateLimiter{
		redisClient: redisClient,
		cfg:         cfg,
		logger:      logger,
	}
}

func (rl *RateLimiter) Middleware() gin.HandlerFunc {
	if !rl.cfg.Enabled || rl.redisClient == nil {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	return func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "" {
			ip = strings.Split(c.Request.RemoteAddr, ":")[0]
		}

		banKey := "ratelimit:ban:" + ip
		banned, err := rl.redisClient.Exists(c.Request.Context(), banKey).Result()
		if err == nil && banned > 0 {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    429,
				"message": "too many requests, please try again later",
			})
			return
		}

		key := "ratelimit:" + ip
		allowed, err := rateLimitIncrScript.Run(c.Request.Context(), rl.redisClient, []string{key}, rl.cfg.WindowMs, rl.cfg.PerIP).Int()
		if err != nil {
			rl.logger.Warn("rate limit script failed", zap.String("ip", ip), zap.Error(err))
			c.Next()
			return
		}

		if allowed == 0 {
			if rl.cfg.BanDurationMs > 0 {
				banDuration := time.Duration(rl.cfg.BanDurationMs) * time.Millisecond
				if setErr := rl.redisClient.Set(c.Request.Context(), banKey, "1", banDuration).Err(); setErr != nil {
					rl.logger.Warn("failed to set ban key", zap.String("ip", ip), zap.Error(setErr))
				}
			}

			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    429,
				"message": "too many requests, please try again later",
			})
			return
		}

		remaining := rl.cfg.PerIP - 1
		remainingStr := strconv.Itoa(remaining)
		c.Header("X-RateLimit-Remaining", remainingStr)

		c.Next()
	}
}