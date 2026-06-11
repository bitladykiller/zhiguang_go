// Package config 提供基于 YAML 的配置加载能力。
// 所有配置会在启动时通过 LoadConfig() 一次性读取并反序列化到 Config 结构体，
// 再通过应用装配流程传递给各个服务模块。
//
// 配置设计原则：
//   - 所有配置字段都定义了 yaml tag，与 config.yaml / config-local.yaml 一一对应。
//   - 可选依赖（搜索、LLM、OSS）配置不完整时不会阻止服务启动，
//     而是由调用方自行检测并降级（返回 503）。
//   - itoa 不使用 strconv.Itoa 是为了最小化启动依赖链。
//
// 使用方式：
//
//	cfg, err := config.LoadConfig("config/config-local.yaml")
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// Config 是根配置结构体，与 config.yaml 的层级结构一一对应。
type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Database      DatabaseConfig      `yaml:"database"`
	Redis         RedisConfig         `yaml:"redis"`
	IDGenerator   IDGeneratorConfig   `yaml:"id_generator"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch"`
	Auth          AuthConfig          `yaml:"auth"`
	OSS           OSSConfig           `yaml:"oss"`
	Canal         CanalConfig         `yaml:"canal"`
	Counter       CounterConfig       `yaml:"counter"`
	Cache         CacheConfig         `yaml:"cache"`
	LLM           LLMConfig           `yaml:"llm"`
}

// ServerConfig 控制 HTTP 服务监听配置。
type ServerConfig struct {
	Port int    `yaml:"port"` // default: 8080
	Mode string `yaml:"mode"` // "debug", "release", or "test"
}

// DatabaseConfig 配置 MySQL 连接池。
type DatabaseConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Name            string `yaml:"name"`
	Charset         string `yaml:"charset"`           // default: utf8mb4
	MaxOpenConns    int    `yaml:"max_open_conns"`    // max open connections
	MaxIdleConns    int    `yaml:"max_idle_conns"`    // max idle connections
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"` // max connection lifetime in seconds
}

// DSN 根据配置字段拼装 MySQL 的数据源连接串。
//
// 功能：
//
//	将 DatabaseConfig 中的 Host、Port、User、Password、Name、Charset 等字段
//	拼装为 MySQL DSN 格式的字符串。
//
// 返回值：
//   - string: 格式为 "user:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
//
// 注意：
//   - 密码中包含特殊字符（如 @、: 等）可能造成连接串解析错误，
//     但当前实现不做 URL 编码处理。
//   - parseTime=True 告诉 MySQL 驱动程序将 DATE/DATETIME 类型自动解析为
//     Go 的 time.Time 类型而非字符串。
//   - loc=Local 使用本地时区解析时间。
func (c *DatabaseConfig) DSN() string {
	return c.User + ":" + c.Password + "@tcp(" + c.Host + ":" +
		itoa(c.Port) + ")/" + c.Name + "?charset=" + c.Charset + "&parseTime=True&loc=Local"
}

// RedisConfig 配置 Redis 连接参数。
type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`        // Redis database number (0-15)
	PoolSize int    `yaml:"pool_size"` // connection pool size
}

// IDGeneratorConfig 配置本地雪花 ID 生成器。
//
// 当前约定把 snowflake 的 10 位 node id 拆成：
//   - 5 位 machine_id
//   - 5 位 worker_id
//
// 多实例部署时，必须保证不同实例的 machine_id + worker_id 组合唯一。
type IDGeneratorConfig struct {
	MachineID int `yaml:"machine_id"`
	WorkerID  int `yaml:"worker_id"`
}

// Addr 返回 host:port 形式的 Redis 地址。
//
// 功能：
//
//	将 Host 和 Port 组合为标准 Redis 连接地址格式。
//
// 返回值：
//   - string: 格式为 "host:port"
//
// 注意：
//
//	如果 Host 是域名（如 "redis.example.com"），直接拼接；
//	如果 Host 是空字符串，返回 ":port"（go-redis 会尝试连接本地）。
func (c *RedisConfig) Addr() string {
	return c.Host + ":" + itoa(c.Port)
}

// KafkaConfig 配置 Kafka 生产者与消费者。
type KafkaConfig struct {
	Brokers       []string          `yaml:"brokers"`
	ConsumerGroup string            `yaml:"consumer_group"`
	Topics        KafkaTopicsConfig `yaml:"topics"`
}

// KafkaTopicsConfig 将业务 topic 名称映射为实际 Kafka topic 标识。
type KafkaTopicsConfig struct {
	CounterEvents string `yaml:"counter_events"`
}

// ElasticsearchConfig 配置 Elasticsearch 集群连接信息。
type ElasticsearchConfig struct {
	URIs      []string `yaml:"uris"`
	IndexName string   `yaml:"index_name"` // primary search index
}

// AuthConfig 聚合所有鉴权相关配置。
type AuthConfig struct {
	JWT          JWTConfig          `yaml:"jwt"`
	Verification VerificationConfig `yaml:"verification"`
	Refresh      RefreshConfig      `yaml:"refresh"`
	Password     PasswordConfig     `yaml:"password"`
}

// JWTConfig 配置 JWT 签名与过期时间。
type JWTConfig struct {
	Issuer          string        `yaml:"issuer"`
	KeyID           string        `yaml:"key_id"`
	PrivateKeyPath  string        `yaml:"private_key_path"`
	PublicKeyPath   string        `yaml:"public_key_path"`
	AccessTokenTTL  time.Duration `yaml:"access_token_ttl"`
	RefreshTokenTTL time.Duration `yaml:"refresh_token_ttl"`
}

// VerificationConfig 控制验证码相关行为。
type VerificationConfig struct {
	CodeLength   int            `yaml:"code_length"`
	TTL          time.Duration  `yaml:"ttl"`
	MaxAttempts  int            `yaml:"max_attempts"`
	SendInterval time.Duration  `yaml:"send_interval"`
	DailyLimit   int            `yaml:"daily_limit"`
	Lock         AuthLockConfig `yaml:"lock"`
}

// PasswordConfig 约束密码强度策略。
type PasswordConfig struct {
	BcryptCost int `yaml:"bcrypt_cost"`
	MinLength  int `yaml:"min_length"`
}

// RefreshConfig 配置 refresh token 轮换相关行为。
type RefreshConfig struct {
	Lock AuthLockConfig `yaml:"lock"`
}

// AuthLockConfig 统一描述鉴权域分布式锁参数。
type AuthLockConfig struct {
	TTLMs           int `yaml:"ttl_ms"`
	WatchdogMs      int `yaml:"watchdog_ms"`
	RetryIntervalMs int `yaml:"retry_interval_ms"`
}

// OSSConfig 配置阿里云 OSS 对象存储。
type OSSConfig struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`
	Bucket          string `yaml:"bucket"`
	PublicDomain    string `yaml:"public_domain"`
	Folder          string `yaml:"folder"`
}

// CanalConfig 配置阿里 Canal 的 MySQL binlog 订阅。
type CanalConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	Destination string `yaml:"destination"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	Filter      string `yaml:"filter"`
	BatchSize   int    `yaml:"batch_size"`
	IntervalMs  int    `yaml:"interval_ms"`
}

// CounterConfig 配置计数消费、补偿与 SDS 重建行为。
type CounterConfig struct {
	Consumer ConsumerConfig `yaml:"consumer"`
	Repair   RepairConfig   `yaml:"repair"`
	Rebuild  RebuildConfig  `yaml:"rebuild"`
}

// ConsumerConfig 控制计数 MQ 消费端的批量聚合行为。
type ConsumerConfig struct {
	BatchSize       int `yaml:"batch_size"`
	FlushIntervalMs int `yaml:"flush_interval_ms"`
}

// RepairConfig 控制失败任务补偿与历史记录清理行为。
type RepairConfig struct {
	Enabled            bool `yaml:"enabled"`
	IntervalMs         int  `yaml:"interval_ms"`
	BatchSize          int  `yaml:"batch_size"`
	CleanupIntervalMs  int  `yaml:"cleanup_interval_ms"`
	CleanupBatchSize   int  `yaml:"cleanup_batch_size"`
	DoneRetentionHours int  `yaml:"done_retention_hours"`
}

// RebuildConfig 控制 SDS 重建过程中的限流策略。
type RebuildConfig struct {
	Enabled bool              `yaml:"enabled"`
	Lock    RebuildLockConfig `yaml:"lock"`
	Rate    RebuildRateConfig `yaml:"rate"`
	Backoff BackoffConfig     `yaml:"backoff"`
}

// RebuildLockConfig 配置重建操作使用的分布式锁参数。
type RebuildLockConfig struct {
	TTLMs      int `yaml:"ttl_ms"`
	WatchdogMs int `yaml:"watchdog_ms"`
}

// RebuildRateConfig 限制单位时间窗口内允许发生的重建次数。
type RebuildRateConfig struct {
	Permits       int `yaml:"permits"`
	WindowSeconds int `yaml:"window_seconds"`
}

// BackoffConfig 控制重建失败后的指数退避策略。
type BackoffConfig struct {
	BaseMs int `yaml:"base_ms"`
	MaxMs  int `yaml:"max_ms"`
}

// CacheConfig 配置多级缓存系统。
type CacheConfig struct {
	L2     L2CacheConfig `yaml:"l2"`
	HotKey HotKeyConfig  `yaml:"hotkey"`
}

// L2CacheConfig 保存不同 feed 类型对应的二级缓存配置。
type L2CacheConfig struct {
	PublicCfg CacheItemConfig `yaml:"public_cfg"`
	MineCfg   CacheItemConfig `yaml:"mine_cfg"`
}

// CacheItemConfig 定义单个缓存实例的 TTL 和最大容量。
type CacheItemConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
	MaxSize    int `yaml:"max_size"`
}

// HotKeyConfig 控制热点键识别与 TTL 延长行为。
//
// 设计说明：
// HotKeyDetector 使用本地 map + Redis Hash 实现滑动窗口热点检测。
// 本地 map 在每次缓存访问时计数（无 Redis IO），
// 每 BucketSizeSeconds 秒 flush 到 Redis Hash 完成跨实例聚合。
// Redis Hash 的 field 是 6 秒窗口编号，value 是窗口内访问次数。
// 判断 hotkey 时，HGETALL 该哈希并累加最近 BucketCount 个窗口的值。
//
// 建议配置（6s 窗口 × 10 = 60s 滑动窗口）：
//
//	BucketSizeSeconds: 6         # 每个窗口大小
//	BucketCount: 10               # 窗口数量（总窗口 = 6s × 10 = 60s）
//	FlushIntervalSeconds: 6       # flush 间隔，与 BucketSizeSeconds 一致
//	StatTTLSeconds: 120           # Redis Hash 的 TTL（略大于窗口总时长）
//	HotMarkTTLSeconds: 60         # hotkey:active 标记的 TTL
//
// 阈值说明（基于 60s 窗口的全局总访问次数）：
//
//	LevelLow(50):   0.83 QPS 以上 → TTL +20s
//	LevelMedium(200):  3.3 QPS 以上 → TTL +60s
//	LevelHigh(500):   8.3 QPS 以上 → TTL +120s
type HotKeyConfig struct {
	BucketSizeSeconds    int `yaml:"bucket_size_seconds"`    // 每个时间窗口的秒数（建议 6）
	BucketCount          int `yaml:"bucket_count"`           // 窗口数量（建议 10，总窗口 = 6×10=60s）
	FlushIntervalSeconds int `yaml:"flush_interval_seconds"` // flush 到 Redis 的间隔（建议 6）
	StatTTLSeconds       int `yaml:"stat_ttl_seconds"`       // Redis Hash 的 TTL（建议 120）
	LevelLow             int `yaml:"level_low"`              // LOW 热度阈值
	LevelMedium          int `yaml:"level_medium"`           // MEDIUM 热度阈值
	LevelHigh            int `yaml:"level_high"`             // HIGH 热度阈值
	ExtendLowSeconds     int `yaml:"extend_low_seconds"`     // LOW 等级 TTL 延长量（秒）
	ExtendMediumSeconds  int `yaml:"extend_medium_seconds"`  // MEDIUM 等级 TTL 延长量（秒）
	ExtendHighSeconds    int `yaml:"extend_high_seconds"`    // HIGH 等级 TTL 延长量（秒）
	HotMarkTTLSeconds    int `yaml:"hot_mark_ttl_seconds"`   // hotkey:active 标记的 TTL（建议 60）
}

// LLMConfig 配置 AI 模型连接信息。
type LLMConfig struct {
	DeepSeek DeepSeekConfig `yaml:"deepseek"`
	OpenAI   OpenAIConfig   `yaml:"openai"`
}

// DeepSeekConfig 配置 DeepSeek 对话模型 API。
type DeepSeekConfig struct {
	APIKey      string  `yaml:"api_key"`
	BaseURL     string  `yaml:"base_url"`
	Model       string  `yaml:"model"`
	Temperature float64 `yaml:"temperature"`
}

// OpenAIConfig 配置兼容 OpenAI 协议的 API（用于向量嵌入）。
type OpenAIConfig struct {
	BaseURL        string `yaml:"base_url"`
	APIKey         string `yaml:"api_key"`
	EmbeddingModel string `yaml:"embedding_model"`
	Dimensions     int    `yaml:"dimensions"`
}

// LoadConfig 从指定路径读取 YAML 配置，补默认值并做启动期校验。
//
// 启动时统一完成这三件事，可以把很多“运行到一半才暴露”的配置问题前移到启动阶段。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return cfg, nil
}

// applyDefaults 为核心配置填充保守默认值。
//
// 这里主要处理两类默认值：
//   - 基础设施连接与连接池
//   - Counter / Cache / Auth 等核心模块的运行参数
//
// 对可选能力（ES、LLM、OSS）则不强塞默认值，避免制造“看似可用、实际错误”的假象。
func (c *Config) applyDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if strings.TrimSpace(c.Server.Mode) == "" {
		c.Server.Mode = "debug"
	}

	if c.Database.Port == 0 {
		c.Database.Port = 3306
	}
	if strings.TrimSpace(c.Database.Charset) == "" {
		c.Database.Charset = "utf8mb4"
	}
	if c.Database.MaxOpenConns <= 0 {
		c.Database.MaxOpenConns = 50
	}
	if c.Database.MaxIdleConns <= 0 {
		c.Database.MaxIdleConns = 10
	}
	if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
		c.Database.MaxIdleConns = c.Database.MaxOpenConns
	}
	if c.Database.ConnMaxLifetime <= 0 {
		c.Database.ConnMaxLifetime = 3600
	}

	if c.Redis.Port == 0 {
		c.Redis.Port = 6379
	}
	if c.Redis.PoolSize <= 0 {
		c.Redis.PoolSize = 20
	}

	if c.Kafka.ConsumerGroup == "" {
		c.Kafka.ConsumerGroup = "counter-agg"
	}
	if c.Kafka.Topics.CounterEvents == "" {
		c.Kafka.Topics.CounterEvents = "counter-events"
	}

	if c.Auth.JWT.AccessTokenTTL <= 0 {
		c.Auth.JWT.AccessTokenTTL = 15 * time.Minute
	}
	if c.Auth.JWT.RefreshTokenTTL <= 0 {
		c.Auth.JWT.RefreshTokenTTL = 168 * time.Hour
	}
	if c.Auth.Verification.CodeLength <= 0 {
		c.Auth.Verification.CodeLength = 6
	}
	if c.Auth.Verification.TTL <= 0 {
		c.Auth.Verification.TTL = 5 * time.Minute
	}
	if c.Auth.Verification.MaxAttempts <= 0 {
		c.Auth.Verification.MaxAttempts = 5
	}
	if c.Auth.Verification.SendInterval <= 0 {
		c.Auth.Verification.SendInterval = 60 * time.Second
	}
	if c.Auth.Verification.DailyLimit <= 0 {
		c.Auth.Verification.DailyLimit = 10
	}
	applyAuthLockDefaults(&c.Auth.Verification.Lock)
	applyAuthLockDefaults(&c.Auth.Refresh.Lock)
	if c.Auth.Password.BcryptCost <= 0 {
		c.Auth.Password.BcryptCost = 12
	}
	if c.Auth.Password.MinLength <= 0 {
		c.Auth.Password.MinLength = 8
	}

	if c.Canal.Port == 0 {
		c.Canal.Port = 11111
	}
	if c.Canal.BatchSize <= 0 {
		c.Canal.BatchSize = 1000
	}
	if c.Canal.IntervalMs <= 0 {
		c.Canal.IntervalMs = 1000
	}

	if c.Counter.Consumer.BatchSize <= 0 {
		c.Counter.Consumer.BatchSize = 100
	}
	if c.Counter.Consumer.FlushIntervalMs <= 0 {
		c.Counter.Consumer.FlushIntervalMs = 1000
	}
	if c.Counter.Repair.IntervalMs <= 0 {
		c.Counter.Repair.IntervalMs = 60000
	}
	if c.Counter.Repair.BatchSize <= 0 {
		c.Counter.Repair.BatchSize = 100
	}
	if c.Counter.Repair.CleanupIntervalMs <= 0 {
		c.Counter.Repair.CleanupIntervalMs = 3600000
	}
	if c.Counter.Repair.CleanupBatchSize <= 0 {
		c.Counter.Repair.CleanupBatchSize = 500
	}
	if c.Counter.Repair.DoneRetentionHours <= 0 {
		c.Counter.Repair.DoneRetentionHours = 168
	}
	if c.Counter.Rebuild.Lock.TTLMs <= 0 {
		c.Counter.Rebuild.Lock.TTLMs = 5000
	}
	if c.Counter.Rebuild.Lock.WatchdogMs <= 0 {
		c.Counter.Rebuild.Lock.WatchdogMs = 30000
	}
	if c.Counter.Rebuild.Rate.Permits <= 0 {
		c.Counter.Rebuild.Rate.Permits = 3
	}
	if c.Counter.Rebuild.Rate.WindowSeconds <= 0 {
		c.Counter.Rebuild.Rate.WindowSeconds = 10
	}
	if c.Counter.Rebuild.Backoff.BaseMs <= 0 {
		c.Counter.Rebuild.Backoff.BaseMs = 500
	}
	if c.Counter.Rebuild.Backoff.MaxMs <= 0 {
		c.Counter.Rebuild.Backoff.MaxMs = 30000
	}

	if c.Cache.L2.PublicCfg.TTLSeconds <= 0 {
		c.Cache.L2.PublicCfg.TTLSeconds = 15
	}
	if c.Cache.L2.PublicCfg.MaxSize <= 0 {
		c.Cache.L2.PublicCfg.MaxSize = 1000
	}
	if c.Cache.L2.MineCfg.TTLSeconds <= 0 {
		c.Cache.L2.MineCfg.TTLSeconds = 10
	}
	if c.Cache.L2.MineCfg.MaxSize <= 0 {
		c.Cache.L2.MineCfg.MaxSize = 1000
	}
	if c.Cache.HotKey.BucketSizeSeconds <= 0 {
		c.Cache.HotKey.BucketSizeSeconds = 6
	}
	if c.Cache.HotKey.BucketCount <= 0 {
		c.Cache.HotKey.BucketCount = 10
	}
	if c.Cache.HotKey.FlushIntervalSeconds <= 0 {
		c.Cache.HotKey.FlushIntervalSeconds = 6
	}
	if c.Cache.HotKey.StatTTLSeconds <= 0 {
		c.Cache.HotKey.StatTTLSeconds = 120
	}
	if c.Cache.HotKey.LevelLow <= 0 {
		c.Cache.HotKey.LevelLow = 50
	}
	if c.Cache.HotKey.LevelMedium <= 0 {
		c.Cache.HotKey.LevelMedium = 200
	}
	if c.Cache.HotKey.LevelHigh <= 0 {
		c.Cache.HotKey.LevelHigh = 500
	}
	if c.Cache.HotKey.ExtendLowSeconds <= 0 {
		c.Cache.HotKey.ExtendLowSeconds = 20
	}
	if c.Cache.HotKey.ExtendMediumSeconds <= 0 {
		c.Cache.HotKey.ExtendMediumSeconds = 60
	}
	if c.Cache.HotKey.ExtendHighSeconds <= 0 {
		c.Cache.HotKey.ExtendHighSeconds = 120
	}
	if c.Cache.HotKey.HotMarkTTLSeconds <= 0 {
		c.Cache.HotKey.HotMarkTTLSeconds = 60
	}

	if c.LLM.OpenAI.Dimensions <= 0 {
		c.LLM.OpenAI.Dimensions = 1536
	}
}

// Validate 校验启动所需的核心配置。
//
// 约束原则：
//   - 核心链路配置必须完整，否则直接启动失败
//   - 可选能力允许缺失，由 bootstrap 层统一降级
func (c *Config) Validate() error {
	if err := validatePort("server.port", c.Server.Port); err != nil {
		return err
	}
	switch c.Server.Mode {
	case "debug", "release", "test":
	default:
		return fmt.Errorf("server.mode must be one of debug/release/test")
	}

	if strings.TrimSpace(c.Database.Host) == "" {
		return fmt.Errorf("database.host is required")
	}
	if err := validatePort("database.port", c.Database.Port); err != nil {
		return err
	}
	if strings.TrimSpace(c.Database.User) == "" {
		return fmt.Errorf("database.user is required")
	}
	if strings.TrimSpace(c.Database.Name) == "" {
		return fmt.Errorf("database.name is required")
	}
	if c.Database.MaxOpenConns <= 0 {
		return fmt.Errorf("database.max_open_conns must be > 0")
	}
	if c.Database.MaxIdleConns < 0 {
		return fmt.Errorf("database.max_idle_conns must be >= 0")
	}
	if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
		return fmt.Errorf("database.max_idle_conns must be <= max_open_conns")
	}
	if c.Database.ConnMaxLifetime <= 0 {
		return fmt.Errorf("database.conn_max_lifetime must be > 0")
	}

	if err := validatePort("redis.port", c.Redis.Port); err != nil {
		return err
	}
	if c.Redis.DB < 0 {
		return fmt.Errorf("redis.db must be >= 0")
	}
	if c.Redis.PoolSize <= 0 {
		return fmt.Errorf("redis.pool_size must be > 0")
	}

	if c.IDGenerator.MachineID < 0 || c.IDGenerator.MachineID > 31 {
		return fmt.Errorf("id_generator.machine_id must be in [0,31]")
	}
	if c.IDGenerator.WorkerID < 0 || c.IDGenerator.WorkerID > 31 {
		return fmt.Errorf("id_generator.worker_id must be in [0,31]")
	}

	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	for i, broker := range c.Kafka.Brokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("kafka.brokers[%d] must not be empty", i)
		}
	}
	if strings.TrimSpace(c.Kafka.ConsumerGroup) == "" {
		return fmt.Errorf("kafka.consumer_group is required")
	}
	if strings.TrimSpace(c.Kafka.Topics.CounterEvents) == "" {
		return fmt.Errorf("kafka.topics.counter_events is required")
	}

	if strings.TrimSpace(c.Auth.JWT.Issuer) == "" {
		return fmt.Errorf("auth.jwt.issuer is required")
	}
	if strings.TrimSpace(c.Auth.JWT.KeyID) == "" {
		return fmt.Errorf("auth.jwt.key_id is required")
	}
	if strings.TrimSpace(c.Auth.JWT.PrivateKeyPath) == "" {
		return fmt.Errorf("auth.jwt.private_key_path is required")
	}
	if strings.TrimSpace(c.Auth.JWT.PublicKeyPath) == "" {
		return fmt.Errorf("auth.jwt.public_key_path is required")
	}
	if c.Auth.JWT.AccessTokenTTL <= 0 {
		return fmt.Errorf("auth.jwt.access_token_ttl must be > 0")
	}
	if c.Auth.JWT.RefreshTokenTTL <= 0 {
		return fmt.Errorf("auth.jwt.refresh_token_ttl must be > 0")
	}
	if c.Auth.Verification.CodeLength <= 0 {
		return fmt.Errorf("auth.verification.code_length must be > 0")
	}
	if c.Auth.Verification.TTL <= 0 {
		return fmt.Errorf("auth.verification.ttl must be > 0")
	}
	if c.Auth.Verification.MaxAttempts <= 0 {
		return fmt.Errorf("auth.verification.max_attempts must be > 0")
	}
	if c.Auth.Verification.SendInterval <= 0 {
		return fmt.Errorf("auth.verification.send_interval must be > 0")
	}
	if c.Auth.Verification.DailyLimit <= 0 {
		return fmt.Errorf("auth.verification.daily_limit must be > 0")
	}
	if err := validateAuthLock("auth.verification.lock", c.Auth.Verification.Lock); err != nil {
		return err
	}
	if err := validateAuthLock("auth.refresh.lock", c.Auth.Refresh.Lock); err != nil {
		return err
	}
	if c.Auth.Password.MinLength <= 0 {
		return fmt.Errorf("auth.password.min_length must be > 0")
	}
	if c.Auth.Password.BcryptCost < bcrypt.MinCost || c.Auth.Password.BcryptCost > bcrypt.MaxCost {
		return fmt.Errorf("auth.password.bcrypt_cost must be in [%d,%d]", bcrypt.MinCost, bcrypt.MaxCost)
	}

	if c.Canal.Enabled {
		if strings.TrimSpace(c.Canal.Host) == "" {
			return fmt.Errorf("canal.host is required when canal.enabled=true")
		}
		if err := validatePort("canal.port", c.Canal.Port); err != nil {
			return err
		}
	}
	if c.Canal.BatchSize <= 0 {
		return fmt.Errorf("canal.batch_size must be > 0")
	}
	if c.Canal.IntervalMs <= 0 {
		return fmt.Errorf("canal.interval_ms must be > 0")
	}

	if c.Counter.Consumer.BatchSize <= 0 {
		return fmt.Errorf("counter.consumer.batch_size must be > 0")
	}
	if c.Counter.Consumer.FlushIntervalMs <= 0 {
		return fmt.Errorf("counter.consumer.flush_interval_ms must be > 0")
	}
	if c.Counter.Repair.IntervalMs <= 0 {
		return fmt.Errorf("counter.repair.interval_ms must be > 0")
	}
	if c.Counter.Repair.BatchSize <= 0 {
		return fmt.Errorf("counter.repair.batch_size must be > 0")
	}
	if c.Counter.Repair.CleanupIntervalMs <= 0 {
		return fmt.Errorf("counter.repair.cleanup_interval_ms must be > 0")
	}
	if c.Counter.Repair.CleanupBatchSize <= 0 {
		return fmt.Errorf("counter.repair.cleanup_batch_size must be > 0")
	}
	if c.Counter.Repair.DoneRetentionHours <= 0 {
		return fmt.Errorf("counter.repair.done_retention_hours must be > 0")
	}
	if c.Counter.Rebuild.Lock.TTLMs <= 0 || c.Counter.Rebuild.Lock.WatchdogMs <= 0 {
		return fmt.Errorf("counter.rebuild.lock ttl/watchdog must be > 0")
	}
	if c.Counter.Rebuild.Rate.Permits <= 0 || c.Counter.Rebuild.Rate.WindowSeconds <= 0 {
		return fmt.Errorf("counter.rebuild.rate permits/window_seconds must be > 0")
	}
	if c.Counter.Rebuild.Backoff.BaseMs <= 0 || c.Counter.Rebuild.Backoff.MaxMs <= 0 {
		return fmt.Errorf("counter.rebuild.backoff base_ms/max_ms must be > 0")
	}
	if c.Counter.Rebuild.Backoff.BaseMs > c.Counter.Rebuild.Backoff.MaxMs {
		return fmt.Errorf("counter.rebuild.backoff.base_ms must be <= max_ms")
	}

	if c.Cache.L2.PublicCfg.TTLSeconds <= 0 || c.Cache.L2.PublicCfg.MaxSize <= 0 {
		return fmt.Errorf("cache.l2.public_cfg ttl_seconds/max_size must be > 0")
	}
	if c.Cache.L2.MineCfg.TTLSeconds <= 0 || c.Cache.L2.MineCfg.MaxSize <= 0 {
		return fmt.Errorf("cache.l2.mine_cfg ttl_seconds/max_size must be > 0")
	}
	if c.Cache.HotKey.BucketSizeSeconds <= 0 ||
		c.Cache.HotKey.BucketCount <= 0 ||
		c.Cache.HotKey.FlushIntervalSeconds <= 0 ||
		c.Cache.HotKey.StatTTLSeconds <= 0 ||
		c.Cache.HotKey.HotMarkTTLSeconds <= 0 {
		return fmt.Errorf("cache.hotkey bucket/flush/ttl settings must be > 0")
	}

	return nil
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

func validatePort(name string, port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("%s must be in [1,65535]", name)
	}
	return nil
}

// itoa 在不引入 strconv 的前提下把 int 转成字符串。
//
// 功能：
//
//	将整数 n 通过除 10 取余的方式逐位分解，然后拼接为字符串。
//	支持负数和零。
//
// 参数：
//   - n: 待转换的整数
//
// 返回值：
//   - string: 整数的十进制字符串表示
//
// WHY 不使用 strconv.Itoa：
//
//	官方说明是在启动路径上减少一个标准库依赖能略微缩短编译时间。
//	该函数仅在 DSN() 和 Addr() 中被调用，性能不敏感，
//	因此自实现的开销可以忽略。
//
// 边界情况：
//   - n == 0 → 返回 "0"
//   - n < 0 → 返回 "-" + 绝对值的字符串（如 -42 → "-42"）
//   - n == math.MinInt → 取绝对值会溢出，但该函数仅在端口号上使用，
//     端口号始终为正数，因此不会有负值极端情况。
//
// 实现说明：
//
//	使用 [20]byte 固定长度数组作为缓冲区（最大 int64 十进制 19 位 + 负号），
//	从尾部往前填充，最后切片转换为字符串。这比多次字符串拼接更高效。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
