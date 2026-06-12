package config

import (
	"strings"
	"time"
)

// applyDefaults 为核心配置填充保守默认值。
//
// 这里主要处理两类默认值：
//   - 基础设施连接与连接池
//   - Counter / Cache / Auth 等核心模块的运行参数
//
// 对可选能力（ES、LLM、OSS）则不强塞默认值，避免制造“看似可用、实际错误”的假象。
func (c *Config) applyDefaults() {
	applyServerDefaults(&c.Server)
	applyDatabaseDefaults(&c.Database)
	applyRedisDefaults(&c.Redis)
	applyKafkaDefaults(&c.Kafka)
	applyAuthDefaults(&c.Auth)
	applyCanalDefaults(&c.Canal)
	applyCounterDefaults(&c.Counter)
	applyCacheDefaults(&c.Cache)
	applyLLMDefaults(&c.LLM)
}

func applyServerDefaults(cfg *ServerConfig) {
	if cfg.Port == 0 {
		cfg.Port = 8080
	}
	if strings.TrimSpace(cfg.Mode) == "" {
		cfg.Mode = "debug"
	}
}

func applyDatabaseDefaults(cfg *DatabaseConfig) {
	if cfg.Port == 0 {
		cfg.Port = 3306
	}
	if strings.TrimSpace(cfg.Charset) == "" {
		cfg.Charset = "utf8mb4"
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 50
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 10
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		cfg.MaxIdleConns = cfg.MaxOpenConns
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 3600
	}
}

func applyRedisDefaults(cfg *RedisConfig) {
	if cfg.Port == 0 {
		cfg.Port = 6379
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 20
	}
}

func applyKafkaDefaults(cfg *KafkaConfig) {
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "counter-agg"
	}
	if cfg.Topics.CounterEvents == "" {
		cfg.Topics.CounterEvents = "counter-events"
	}
}

func applyAuthDefaults(cfg *AuthConfig) {
	if cfg.JWT.AccessTokenTTL <= 0 {
		cfg.JWT.AccessTokenTTL = 15 * time.Minute
	}
	if cfg.JWT.RefreshTokenTTL <= 0 {
		cfg.JWT.RefreshTokenTTL = 168 * time.Hour
	}
	if cfg.Verification.CodeLength <= 0 {
		cfg.Verification.CodeLength = 6
	}
	if cfg.Verification.TTL <= 0 {
		cfg.Verification.TTL = 5 * time.Minute
	}
	if cfg.Verification.MaxAttempts <= 0 {
		cfg.Verification.MaxAttempts = 5
	}
	if cfg.Verification.SendInterval <= 0 {
		cfg.Verification.SendInterval = 60 * time.Second
	}
	if cfg.Verification.DailyLimit <= 0 {
		cfg.Verification.DailyLimit = 10
	}
	applyAuthLockDefaults(&cfg.Verification.Lock)
	applyAuthLockDefaults(&cfg.Refresh.Lock)
	if cfg.Password.BcryptCost <= 0 {
		cfg.Password.BcryptCost = 12
	}
	if cfg.Password.MinLength <= 0 {
		cfg.Password.MinLength = 8
	}
}

func applyCanalDefaults(cfg *CanalConfig) {
	if cfg.Port == 0 {
		cfg.Port = 11111
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.IntervalMs <= 0 {
		cfg.IntervalMs = 1000
	}
}

func applyCounterDefaults(cfg *CounterConfig) {
	if cfg.Consumer.BatchSize <= 0 {
		cfg.Consumer.BatchSize = 100
	}
	if cfg.Consumer.FlushIntervalMs <= 0 {
		cfg.Consumer.FlushIntervalMs = 1000
	}
	if cfg.Repair.IntervalMs <= 0 {
		cfg.Repair.IntervalMs = 60000
	}
	if cfg.Repair.BatchSize <= 0 {
		cfg.Repair.BatchSize = 100
	}
	if cfg.Repair.CleanupIntervalMs <= 0 {
		cfg.Repair.CleanupIntervalMs = 3600000
	}
	if cfg.Repair.CleanupBatchSize <= 0 {
		cfg.Repair.CleanupBatchSize = 500
	}
	if cfg.Repair.DoneRetentionHours <= 0 {
		cfg.Repair.DoneRetentionHours = 168
	}
	if cfg.Rebuild.Lock.TTLMs <= 0 {
		cfg.Rebuild.Lock.TTLMs = 5000
	}
	if cfg.Rebuild.Lock.WatchdogMs <= 0 {
		cfg.Rebuild.Lock.WatchdogMs = 30000
	}
	if cfg.Rebuild.Rate.Permits <= 0 {
		cfg.Rebuild.Rate.Permits = 3
	}
	if cfg.Rebuild.Rate.WindowSeconds <= 0 {
		cfg.Rebuild.Rate.WindowSeconds = 10
	}
	if cfg.Rebuild.Backoff.BaseMs <= 0 {
		cfg.Rebuild.Backoff.BaseMs = 500
	}
	if cfg.Rebuild.Backoff.MaxMs <= 0 {
		cfg.Rebuild.Backoff.MaxMs = 30000
	}
}

func applyCacheDefaults(cfg *CacheConfig) {
	if cfg.L2.PublicCfg.TTLSeconds <= 0 {
		cfg.L2.PublicCfg.TTLSeconds = 15
	}
	if cfg.L2.PublicCfg.MaxSize <= 0 {
		cfg.L2.PublicCfg.MaxSize = 1000
	}
	if cfg.L2.MineCfg.TTLSeconds <= 0 {
		cfg.L2.MineCfg.TTLSeconds = 10
	}
	if cfg.L2.MineCfg.MaxSize <= 0 {
		cfg.L2.MineCfg.MaxSize = 1000
	}
	if cfg.HotKey.BucketSizeSeconds <= 0 {
		cfg.HotKey.BucketSizeSeconds = 6
	}
	if cfg.HotKey.BucketCount <= 0 {
		cfg.HotKey.BucketCount = 10
	}
	if cfg.HotKey.FlushIntervalSeconds <= 0 {
		cfg.HotKey.FlushIntervalSeconds = 6
	}
	if cfg.HotKey.StatTTLSeconds <= 0 {
		cfg.HotKey.StatTTLSeconds = 120
	}
	if cfg.HotKey.LevelLow <= 0 {
		cfg.HotKey.LevelLow = 50
	}
	if cfg.HotKey.LevelMedium <= 0 {
		cfg.HotKey.LevelMedium = 200
	}
	if cfg.HotKey.LevelHigh <= 0 {
		cfg.HotKey.LevelHigh = 500
	}
	if cfg.HotKey.ExtendLowSeconds <= 0 {
		cfg.HotKey.ExtendLowSeconds = 20
	}
	if cfg.HotKey.ExtendMediumSeconds <= 0 {
		cfg.HotKey.ExtendMediumSeconds = 60
	}
	if cfg.HotKey.ExtendHighSeconds <= 0 {
		cfg.HotKey.ExtendHighSeconds = 120
	}
	if cfg.HotKey.HotMarkTTLSeconds <= 0 {
		cfg.HotKey.HotMarkTTLSeconds = 60
	}
}

func applyLLMDefaults(cfg *LLMConfig) {
	if cfg.OpenAI.Dimensions <= 0 {
		cfg.OpenAI.Dimensions = 1536
	}
}

func applyAuthLockDefaults(lock *AuthLockConfig) {
	if lock.TTLMs <= 0 {
		lock.TTLMs = 5000
	}
	if lock.WatchdogMs <= 0 {
		lock.WatchdogMs = 15000
	}
	if lock.RetryIntervalMs <= 0 {
		lock.RetryIntervalMs = 100
	}
}
