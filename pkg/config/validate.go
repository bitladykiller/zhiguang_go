package config

import (
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Validate 校验启动所需的核心配置。
//
// 约束原则：
//   - 核心链路配置必须完整，否则直接启动失败
//   - 可选能力允许缺失，由 bootstrap 层统一降级
func (c *Config) Validate() error {
	if err := validateServerConfig(c.Server); err != nil {
		return err
	}
	if err := validateDatabaseConfig(c.Database); err != nil {
		return err
	}
	if err := validateRedisConfig(c.Redis); err != nil {
		return err
	}
	if err := validateIDGeneratorConfig(c.IDGenerator); err != nil {
		return err
	}
	if err := validateKafkaConfig(c.Kafka); err != nil {
		return err
	}
	if err := validateAuthConfig(c.Auth); err != nil {
		return err
	}
	if err := validateCanalConfig(c.Canal); err != nil {
		return err
	}
	if err := validateCounterConfig(c.Counter); err != nil {
		return err
	}
	if err := validateCacheConfig(c.Cache); err != nil {
		return err
	}
	return nil
}

func validateServerConfig(cfg ServerConfig) error {
	if err := validatePort("server.port", cfg.Port); err != nil {
		return err
	}
	switch cfg.Mode {
	case "debug", "release", "test":
		return nil
	default:
		return fmt.Errorf("server.mode must be one of debug/release/test")
	}
}

func validateDatabaseConfig(cfg DatabaseConfig) error {
	if strings.TrimSpace(cfg.Host) == "" {
		return fmt.Errorf("database.host is required")
	}
	if err := validatePort("database.port", cfg.Port); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.User) == "" {
		return fmt.Errorf("database.user is required")
	}
	if strings.TrimSpace(cfg.Name) == "" {
		return fmt.Errorf("database.name is required")
	}
	if cfg.MaxOpenConns <= 0 {
		return fmt.Errorf("database.max_open_conns must be > 0")
	}
	if cfg.MaxIdleConns < 0 {
		return fmt.Errorf("database.max_idle_conns must be >= 0")
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		return fmt.Errorf("database.max_idle_conns must be <= max_open_conns")
	}
	if cfg.ConnMaxLifetime <= 0 {
		return fmt.Errorf("database.conn_max_lifetime must be > 0")
	}
	return nil
}

func validateRedisConfig(cfg RedisConfig) error {
	if err := validatePort("redis.port", cfg.Port); err != nil {
		return err
	}
	if cfg.DB < 0 {
		return fmt.Errorf("redis.db must be >= 0")
	}
	if cfg.PoolSize <= 0 {
		return fmt.Errorf("redis.pool_size must be > 0")
	}
	return nil
}

func validateIDGeneratorConfig(cfg IDGeneratorConfig) error {
	if cfg.MachineID < 0 || cfg.MachineID > 31 {
		return fmt.Errorf("id_generator.machine_id must be in [0,31]")
	}
	if cfg.WorkerID < 0 || cfg.WorkerID > 31 {
		return fmt.Errorf("id_generator.worker_id must be in [0,31]")
	}
	return nil
}

func validateKafkaConfig(cfg KafkaConfig) error {
	if len(cfg.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	for i, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("kafka.brokers[%d] must not be empty", i)
		}
	}
	if strings.TrimSpace(cfg.ConsumerGroup) == "" {
		return fmt.Errorf("kafka.consumer_group is required")
	}
	if strings.TrimSpace(cfg.Topics.CounterEvents) == "" {
		return fmt.Errorf("kafka.topics.counter_events is required")
	}
	return nil
}

func validateAuthConfig(cfg AuthConfig) error {
	if strings.TrimSpace(cfg.JWT.Issuer) == "" {
		return fmt.Errorf("auth.jwt.issuer is required")
	}
	if strings.TrimSpace(cfg.JWT.KeyID) == "" {
		return fmt.Errorf("auth.jwt.key_id is required")
	}
	if strings.TrimSpace(cfg.JWT.PrivateKeyPath) == "" {
		return fmt.Errorf("auth.jwt.private_key_path is required")
	}
	if strings.TrimSpace(cfg.JWT.PublicKeyPath) == "" {
		return fmt.Errorf("auth.jwt.public_key_path is required")
	}
	if cfg.JWT.AccessTokenTTL <= 0 {
		return fmt.Errorf("auth.jwt.access_token_ttl must be > 0")
	}
	if cfg.JWT.RefreshTokenTTL <= 0 {
		return fmt.Errorf("auth.jwt.refresh_token_ttl must be > 0")
	}
	if cfg.Verification.CodeLength <= 0 {
		return fmt.Errorf("auth.verification.code_length must be > 0")
	}
	if cfg.Verification.TTL <= 0 {
		return fmt.Errorf("auth.verification.ttl must be > 0")
	}
	if cfg.Verification.MaxAttempts <= 0 {
		return fmt.Errorf("auth.verification.max_attempts must be > 0")
	}
	if cfg.Verification.SendInterval <= 0 {
		return fmt.Errorf("auth.verification.send_interval must be > 0")
	}
	if cfg.Verification.DailyLimit <= 0 {
		return fmt.Errorf("auth.verification.daily_limit must be > 0")
	}
	if err := validateAuthLock("auth.verification.lock", cfg.Verification.Lock); err != nil {
		return err
	}
	if err := validateAuthLock("auth.refresh.lock", cfg.Refresh.Lock); err != nil {
		return err
	}
	if cfg.Password.MinLength <= 0 {
		return fmt.Errorf("auth.password.min_length must be > 0")
	}
	if cfg.Password.BcryptCost < bcrypt.MinCost || cfg.Password.BcryptCost > bcrypt.MaxCost {
		return fmt.Errorf("auth.password.bcrypt_cost must be in [%d,%d]", bcrypt.MinCost, bcrypt.MaxCost)
	}
	return nil
}

func validateCanalConfig(cfg CanalConfig) error {
	if cfg.Enabled {
		if strings.TrimSpace(cfg.Host) == "" {
			return fmt.Errorf("canal.host is required when canal.enabled=true")
		}
		if err := validatePort("canal.port", cfg.Port); err != nil {
			return err
		}
	}
	if cfg.BatchSize <= 0 {
		return fmt.Errorf("canal.batch_size must be > 0")
	}
	if cfg.IntervalMs <= 0 {
		return fmt.Errorf("canal.interval_ms must be > 0")
	}
	return nil
}

func validateCounterConfig(cfg CounterConfig) error {
	if cfg.Consumer.BatchSize <= 0 {
		return fmt.Errorf("counter.consumer.batch_size must be > 0")
	}
	if cfg.Consumer.FlushIntervalMs <= 0 {
		return fmt.Errorf("counter.consumer.flush_interval_ms must be > 0")
	}
	if cfg.Repair.IntervalMs <= 0 {
		return fmt.Errorf("counter.repair.interval_ms must be > 0")
	}
	if cfg.Repair.BatchSize <= 0 {
		return fmt.Errorf("counter.repair.batch_size must be > 0")
	}
	if cfg.Repair.CleanupIntervalMs <= 0 {
		return fmt.Errorf("counter.repair.cleanup_interval_ms must be > 0")
	}
	if cfg.Repair.CleanupBatchSize <= 0 {
		return fmt.Errorf("counter.repair.cleanup_batch_size must be > 0")
	}
	if cfg.Repair.DoneRetentionHours <= 0 {
		return fmt.Errorf("counter.repair.done_retention_hours must be > 0")
	}
	if cfg.Rebuild.Lock.TTLMs <= 0 || cfg.Rebuild.Lock.WatchdogMs <= 0 {
		return fmt.Errorf("counter.rebuild.lock ttl/watchdog must be > 0")
	}
	if cfg.Rebuild.Rate.Permits <= 0 || cfg.Rebuild.Rate.WindowSeconds <= 0 {
		return fmt.Errorf("counter.rebuild.rate permits/window_seconds must be > 0")
	}
	if cfg.Rebuild.Backoff.BaseMs <= 0 || cfg.Rebuild.Backoff.MaxMs <= 0 {
		return fmt.Errorf("counter.rebuild.backoff base_ms/max_ms must be > 0")
	}
	if cfg.Rebuild.Backoff.BaseMs > cfg.Rebuild.Backoff.MaxMs {
		return fmt.Errorf("counter.rebuild.backoff.base_ms must be <= max_ms")
	}
	return nil
}

func validateCacheConfig(cfg CacheConfig) error {
	if cfg.L2.PublicCfg.TTLSeconds <= 0 || cfg.L2.PublicCfg.MaxSize <= 0 {
		return fmt.Errorf("cache.l2.public_cfg ttl_seconds/max_size must be > 0")
	}
	if cfg.L2.MineCfg.TTLSeconds <= 0 || cfg.L2.MineCfg.MaxSize <= 0 {
		return fmt.Errorf("cache.l2.mine_cfg ttl_seconds/max_size must be > 0")
	}
	if cfg.HotKey.BucketSizeSeconds <= 0 ||
		cfg.HotKey.BucketCount <= 0 ||
		cfg.HotKey.FlushIntervalSeconds <= 0 ||
		cfg.HotKey.StatTTLSeconds <= 0 ||
		cfg.HotKey.HotMarkTTLSeconds <= 0 {
		return fmt.Errorf("cache.hotkey bucket/flush/ttl settings must be > 0")
	}
	return nil
}

func validateAuthLock(name string, lock AuthLockConfig) error {
	if lock.TTLMs <= 0 {
		return fmt.Errorf("%s.ttl_ms must be > 0", name)
	}
	if lock.WatchdogMs <= 0 {
		return fmt.Errorf("%s.watchdog_ms must be > 0", name)
	}
	if lock.RetryIntervalMs <= 0 {
		return fmt.Errorf("%s.retry_interval_ms must be > 0", name)
	}
	return nil
}
