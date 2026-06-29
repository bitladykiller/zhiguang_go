// Package config 提供基于 YAML 的配置加载能力。
// 所有配置会在启动时通过 LoadConfig() 一次性读取并反序列化到 Config 结构体，
// 再通过应用装配流程传递给各个服务模块。
//
// 配置设计原则：
//   - 所有配置字段都定义了 yaml tag，与 config.yaml / config-local.yaml 一一对应。
//   - 可选依赖（搜索、LLM、OSS）配置不完整时不会阻止服务启动，
//     而是由调用方自行检测并降级（返回 503）。
//
// 使用方式：
//
//	cfg, err := config.LoadConfig("config/config-local.yaml")
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

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
	OSS           OssConfig           `yaml:"oss"`
	Canal         CanalConfig         `yaml:"canal"`
	Counter       CounterConfig       `yaml:"counter"`
	Cache         CacheConfig         `yaml:"cache"`
	LLM           LLMConfig           `yaml:"llm"`
	Relation      RelationConfig      `yaml:"relation"`
	Prometheus    PrometheusConfig    `yaml:"prometheus"`
	KnowPost      KnowPostConfig      `yaml:"knowpost"`
	Bootstrap     BootstrapConfig     `yaml:"bootstrap"`
}

const (
	DefaultServerPort              = 8080
	DefaultHTTPRequestTimeoutMs    = 30000
	DefaultCounterPublishTimeoutMs = 3000
	DefaultBackoffKeyTTLMinutes    = 120
	DefaultRebuildScanCount        = 100
	DefaultRebuildConcurrency      = 5
	DefaultLikersCacheMaxSize      = 500
	DefaultLikersCacheTTLMinutes   = 5
	DefaultTokenBucketPExpireMs    = 60000
	DefaultRelationL1CacheSizeMB   = 10
	DefaultRelationL1CacheTTL      = 600
	DefaultFillL1Limit             = 500
	DefaultFallbackExhaustedTTLMin = 10
	DefaultRebuildRateWindowSec    = 60
	DefaultRebuildRatePermits      = 5
	DefaultRebuildRetryInterval    = 50
)

// ServerConfig 控制 HTTP 服务监听配置。
type ServerConfig struct {
	Port                 int             `yaml:"port"`                 // default: 8080
	Mode                 string          `yaml:"mode"`                 // "debug", "release", or "test"
	RequestTimeoutMs     int             `yaml:"request_timeout_ms"`   // default: 30000
	CorsAllowedOrigins   []string        `yaml:"cors_allowed_origins"` // CORS 允许的来源，空时默认 ["*"]
	RateLimit            RateLimitConfig `yaml:"rate_limit"`
}

// HTTPRequestTimeout 返回全局 HTTP 请求超时；未配置或非法时使用默认值。
func (s ServerConfig) HTTPRequestTimeout() time.Duration {
	if s.RequestTimeoutMs <= 0 {
		return time.Duration(DefaultHTTPRequestTimeoutMs) * time.Millisecond
	}
	return time.Duration(s.RequestTimeoutMs) * time.Millisecond
}

// DatabaseConfig 配置 MySQL 连接池。
type DatabaseConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Name            string `yaml:"name"`
	Charset         string `yaml:"charset"`            // default: utf8mb4
	MaxOpenConns    int    `yaml:"max_open_conns"`     // 最大打开连接数
	MaxIdleConns    int    `yaml:"max_idle_conns"`     // 最大空闲连接数
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"`  // 连接最大生命周期（秒）
	ConnMaxIdleTime int    `yaml:"conn_max_idle_time"` // 空闲连接最大生命周期（秒）
	DialTimeoutMs   int    `yaml:"dial_timeout_ms"`    // 连接超时（毫秒）
	ReadTimeoutMs   int    `yaml:"read_timeout_ms"`    // 读超时（毫秒）
	WriteTimeoutMs  int    `yaml:"write_timeout_ms"`    // 写超时（毫秒）
}

// DSN 根据配置字段拼装 MySQL 的数据源连接串。
func (c *DatabaseConfig) DSN() string {
	dsn := c.User + ":" + url.QueryEscape(c.Password) + "@tcp(" + c.Host + ":" +
		strconv.Itoa(c.Port) + ")/" + c.Name + "?charset=" + c.Charset + "&parseTime=True&loc=Local"

	if c.DialTimeoutMs > 0 {
		dsn += "&timeout=" + strconv.Itoa(c.DialTimeoutMs) + "ms"
	}
	if c.ReadTimeoutMs > 0 {
		dsn += "&readTimeout=" + strconv.Itoa(c.ReadTimeoutMs) + "ms"
	}
	if c.WriteTimeoutMs > 0 {
		dsn += "&writeTimeout=" + strconv.Itoa(c.WriteTimeoutMs) + "ms"
	}

	return dsn
}

// RedisConfig 配置 Redis 连接参数。
type RedisConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Password        string `yaml:"password"`
	RequirePass     bool   `yaml:"require_pass"`
	DB              int    `yaml:"db"`                // Redis database number (0-15)
	PoolSize        int    `yaml:"pool_size"`         // connection pool size
	MinIdleConns    int    `yaml:"min_idle_conns"`    // 最小空闲连接数
	MaxRetries      int    `yaml:"max_retries"`       // 最大重试次数
	DialTimeoutMs   int    `yaml:"dial_timeout_ms"`   // 连接超时（毫秒）
	ReadTimeoutMs   int    `yaml:"read_timeout_ms"`   // 读超时（毫秒）
	WriteTimeoutMs  int    `yaml:"write_timeout_ms"`  // 写超时（毫秒）
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"` // 连接最大生命周期（秒）
}

// IDGeneratorConfig 配置本地雪花 ID 生成器。
type IDGeneratorConfig struct {
	MachineID int `yaml:"machine_id"`
	WorkerID  int `yaml:"worker_id"`
}

// Addr 返回 host:port 形式的 Redis 地址。
func (c *RedisConfig) Addr() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}

// KafkaConfig 配置 Kafka 生产者与消费者。
type KafkaConfig struct {
	Brokers        []string          `yaml:"brokers"`
	ConsumerGroup  string            `yaml:"consumer_group"`
	Topics         KafkaTopicsConfig `yaml:"topics"`
	WriteTimeoutMs int               `yaml:"write_timeout_ms"` // 写超时（毫秒）
	ReadTimeoutMs  int               `yaml:"read_timeout_ms"`  // 读超时（毫秒）
	MaxAttempts    int               `yaml:"max_attempts"`     // 最大重试次数
}

// KafkaTopicsConfig 将业务 topic 名称映射为实际 Kafka topic 标识。
type KafkaTopicsConfig struct {
	CounterEvents string `yaml:"counter_events"`
}

// ElasticsearchConfig 配置 Elasticsearch 集群连接信息。
type ElasticsearchConfig struct {
	Enabled    *bool    `yaml:"enabled"`     // 显式功能开关，nil 表示跟随配置完整性判断
	URIs       []string `yaml:"uris"`
	IndexName  string   `yaml:"index_name"`  // primary search index
	MaxRetries int      `yaml:"max_retries"` // 最大重试次数
}

// AuthConfig 聚合所有鉴权相关配置。
type AuthConfig struct {
	Jwt          JwtConfig          `yaml:"jwt"`
	Verification VerificationConfig `yaml:"verification"`
	Refresh      RefreshConfig      `yaml:"refresh"`
	Password     PasswordConfig     `yaml:"password"`
}

// JwtConfig 配置 JWT 签名与过期时间。
type JwtConfig struct {
	Issuer          string        `yaml:"issuer"`
	KeyID           string        `yaml:"key_id"`
	PrivateKeyPath  string        `yaml:"private_key_path"`
	PublicKeyPath   string        `yaml:"public_key_path"`
	AccessTokenTTL  time.Duration `yaml:"access_token_ttl"`
	RefreshTokenTTL time.Duration `yaml:"refresh_token_ttl"`
}

// VerificationConfig 控制验证码相关行为。
type VerificationConfig struct {
	CodeLength        int            `yaml:"code_length"`
	TTL               time.Duration  `yaml:"ttl"`
	MaxAttempts       int            `yaml:"max_attempts"`
	SendInterval      time.Duration  `yaml:"send_interval"`
	DailyLimit        int            `yaml:"daily_limit"`
	OperationTimeoutMs int           `yaml:"operation_timeout_ms"`
	Lock              AuthLockConfig `yaml:"lock"`
}

// PasswordConfig 约束密码强度策略。
type PasswordConfig struct {
	BcryptCost int `yaml:"bcrypt_cost"`
	MinLength  int `yaml:"min_length"`
}

// RefreshConfig 配置 refresh token 轮换相关行为。
type RefreshConfig struct {
	Lock               AuthLockConfig `yaml:"lock"`
	OperationTimeoutMs int            `yaml:"operation_timeout_ms"`
}

// AuthLockConfig 统一描述鉴权域分布式锁参数。
type AuthLockConfig struct {
	TTLMs           int `yaml:"ttl_ms"`
	WatchdogMs      int `yaml:"watchdog_ms"`
	RetryIntervalMs int `yaml:"retry_interval_ms"`
}

// OssConfig 配置阿里云 OSS 对象存储。
type OssConfig struct {
	Enabled         *bool  `yaml:"enabled"`          // 显式功能开关，nil 表示跟随配置完整性判断
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`
	Bucket          string `yaml:"bucket"`
	PublicDomain    string `yaml:"public_domain"`
	Folder          string `yaml:"folder"`
	PresignExpiryMs int    `yaml:"presign_expiry_ms"` // 预签名 URL 过期时间（毫秒），默认 600000 (10分钟)
}

// CanalConfig 配置阿里 Canal 的 MySQL binlog 订阅。
type CanalConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Destination     string `yaml:"destination"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	Filter          string `yaml:"filter"`
	BatchSize       int    `yaml:"batch_size"`
	IntervalMs      int    `yaml:"interval_ms"`
	SocketTimeoutMs int    `yaml:"socket_timeout_ms"` // Socket 超时（毫秒），默认 60000
	IdleTimeoutMs   int    `yaml:"idle_timeout_ms"`   // 空闲超时（毫秒），默认 3600000
}

// CounterConfig 配置 SDS 计数器重建行为。
type CounterConfig struct {
	Consumer         ConsumerConfig `yaml:"consumer"`
	Repair           RepairConfig   `yaml:"repair"`
	Rebuild          RebuildConfig  `yaml:"rebuild"`
	PublishTimeoutMs int            `yaml:"publish_timeout_ms"` // 异步发布 Kafka 超时，默认 3000
}

// PublishTimeout 返回计数事件异步发布的超时时间。
func (c CounterConfig) PublishTimeout() time.Duration {
	if c.PublishTimeoutMs <= 0 {
		return time.Duration(DefaultCounterPublishTimeoutMs) * time.Millisecond
	}
	return time.Duration(c.PublishTimeoutMs) * time.Millisecond
}

// ConsumerConfig 控制计数 MQ 消费端的批量聚合行为。
type ConsumerConfig struct {
	BatchSize       int `yaml:"batch_size"`
	FlushIntervalMs int `yaml:"flush_interval_ms"`
}

// RepairConfig 控制 dirty set 修复任务行为。
type RepairConfig struct {
	Enabled    bool `yaml:"enabled"`
	IntervalMs int  `yaml:"interval_ms"`
	BatchSize  int  `yaml:"batch_size"`
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
	TTLSeconds        int `yaml:"ttl_seconds"`
	MaxSize           int `yaml:"max_size"`
	FreeCacheDefaultMB int `yaml:"free_cache_default_mb"`
}

// HotKeyConfig 控制热点键识别与 TTL 延长行为。
//
// 设计说明：
// HotKeyDetector 使用本地 map + Redis Hash 实现滑动窗口热点检测。
// 本地 map 在每次缓存访问时计数（无 Redis IO），
// 每 BucketSizeSeconds 秒 flush 到 Redis Hash 完成跨实例聚合。
// Redis Hash 的 field 是 6 秒窗口编号，value 是窗口内访问次数。
// 判断 hotkey 时，HGETALL 该哈希并累加最近 BucketCount 个窗口的值。
type HotKeyConfig struct {
	BucketSizeSeconds    int `yaml:"bucket_size_seconds"`     // 每个时间窗口的秒数（建议 6）
	BucketCount          int `yaml:"bucket_count"`            // 窗口数量（建议 10，总窗口 = 6×10=60s）
	FlushIntervalSeconds int `yaml:"flush_interval_seconds"`  // flush 到 Redis 的间隔（建议 6）
	StatTTLSeconds       int `yaml:"stat_ttl_seconds"`        // Redis Hash 的 TTL（建议 120）
	LevelLow             int `yaml:"level_low"`               // LOW 热度阈值
	LevelMedium          int `yaml:"level_medium"`            // MEDIUM 热度阈值
	LevelHigh            int `yaml:"level_high"`              // HIGH 热度阈值
	ExtendLowSeconds     int `yaml:"extend_low_seconds"`      // LOW 等级 TTL 延长量（秒）
	ExtendMediumSeconds  int `yaml:"extend_medium_seconds"`   // MEDIUM 等级 TTL 延长量（秒）
	ExtendHighSeconds    int `yaml:"extend_high_seconds"`     // HIGH 等级 TTL 延长量（秒）
	HotMarkTTLSeconds    int `yaml:"hot_mark_ttl_seconds"`    // hotkey:active 标记的 TTL（建议 60）
	MaxLocalKeys         int `yaml:"max_local_keys"`          // 本地 map 最大键数限制，0 表示使用默认值 100000
}

// LLMConfig 配置 AI 模型连接信息。
type LLMConfig struct {
	Enabled       *bool          `yaml:"enabled"`     // 显式功能开关，nil 表示跟随配置完整性判断
	DeepSeek      DeepSeekConfig `yaml:"deepseek"`
	OpenAI        OpenAIConfig   `yaml:"openai"`
	TimeoutMs     int            `yaml:"timeout_ms"`      // HTTP 客户端超时（毫秒），默认 30000
	MaxContentLen int            `yaml:"max_content_len"`  // 内容截断长度，默认 2000
	MaxTokens     int            `yaml:"max_tokens"`       // 生成最大 token 数，默认 100
	SystemPrompt  string         `yaml:"system_prompt"`    // 系统提示词
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

// RelationConfig 配置关系服务。
type RelationConfig struct {
	BigVThreshold int                              `yaml:"big_v_threshold"`
	TokenBucket   RelationTokenBucketConfig        `yaml:"token_bucket"`
	CacheTTL      int                              `yaml:"cache_ttl"`
	ZSetWarmLimit int                              `yaml:"zset_warm_limit"`
	CacheLock     RelationCacheLockConfig           `yaml:"cache_lock"`
	InvalidateLock RelationInvalidateLockConfig    `yaml:"invalidate_lock"`
	L1Cache       RelationL1CacheConfig            `yaml:"l1_cache"`
	Fallback      RelationFallbackConfig           `yaml:"fallback"`
	MaxOffset     int                              `yaml:"max_offset"`
}

// RelationTokenBucketConfig 配置令牌桶限流。
type RelationTokenBucketConfig struct {
	Capacity   int `yaml:"capacity"`
	Rate       int `yaml:"rate"`
	PExpireMs  int `yaml:"pexpire_ms"`
}

// RelationCacheLockConfig 配置关系列表缓存锁参数。
type RelationCacheLockConfig struct {
	TTLMs           int `yaml:"ttl_ms"`
	WatchdogMs      int `yaml:"watchdog_ms"`
	OpTimeoutMs     int `yaml:"op_timeout_ms"`
	RetryIntervalMs int `yaml:"retry_interval_ms"`
}

// RelationInvalidateLockConfig 配置缓存失效锁参数。
type RelationInvalidateLockConfig struct {
	WaitLimitMs int `yaml:"wait_limit_ms"`
}

// RelationL1CacheConfig 配置关系 L1 缓存参数。
type RelationL1CacheConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
	FillLimit  int `yaml:"fill_limit"`
}

// RelationFallbackConfig 配置关系降级参数。
type RelationFallbackConfig struct {
	ExhaustedTTLMinutes int `yaml:"exhausted_ttl_minutes"`
}

// RateLimitConfig 配置每个 IP 的滑动窗口限流参数。
type RateLimitConfig struct {
	Enabled       bool `yaml:"enabled"`
	PerIP         int  `yaml:"per_ip"`          // 每个 IP 在窗口内允许的最大请求数
	WindowMs      int  `yaml:"window_ms"`       // 滑动窗口大小（毫秒）
	BanDurationMs int  `yaml:"ban_duration_ms"` // 超过限制后的封禁时长（毫秒）
}

type PrometheusConfig struct {
	Enabled bool `yaml:"enabled"`
}

// KnowPostConfig 配置知文模块。
type KnowPostConfig struct {
	DetailCache KnowPostDetailCacheConfig `yaml:"detail_cache"`
	FeedCache   KnowPostFeedCacheConfig   `yaml:"feed_cache"`
}

// KnowPostDetailCacheConfig 配置知文详情缓存。
type KnowPostDetailCacheConfig struct {
	L1TTLSeconds int `yaml:"l1_ttl_seconds"`
	NullTTLBase  int `yaml:"null_ttl_base"`
	NullJitter   int `yaml:"null_jitter"`
	L2TTLBase    int `yaml:"l2_ttl_base"`
	L2Jitter     int `yaml:"l2_jitter"`
	TTLLow       int `yaml:"ttl_low"`
	TTLMedium    int `yaml:"ttl_medium"`
	TTLHigh      int `yaml:"ttl_high"`
}

// KnowPostFeedCacheConfig 配置知文 Feed 缓存。
type KnowPostFeedCacheConfig struct {
	SafeSize         int `yaml:"safe_size"`
	L1TTLSeconds     int `yaml:"l1_ttl_seconds"`
	L2IDListTTLBase  int `yaml:"l2_id_list_ttl_base"`
	L2IDListJitter   int `yaml:"l2_id_list_jitter"`
	L2HasMoreTTLBase int `yaml:"l2_has_more_ttl_base"`
	L2HasMoreJitter  int `yaml:"l2_has_more_jitter"`
	L2ItemTTLBase    int `yaml:"l2_item_ttl_base"`
	L2ItemJitter     int `yaml:"l2_item_jitter"`
	L2MineTTLBase    int `yaml:"l2_mine_ttl_base"`
	L2MineJitter     int `yaml:"l2_mine_jitter"`
	L1MineTTLSeconds int `yaml:"l1_mine_ttl_seconds"`
	ExtendTTLBase    int `yaml:"extend_ttl_base"`
	TTLLow           int `yaml:"ttl_low"`
	TTLMedium        int `yaml:"ttl_medium"`
	TTLHigh          int `yaml:"ttl_high"`
}

// BootstrapConfig 配置 bootstrap 模块的 runner 间隔时间。
type BootstrapConfig struct {
	RelationOutboxIntervalMs int `yaml:"relation_outbox_interval_ms"`
}

// Validate 校验配置中的关键字段是否合法。
func (c *Config) ApplyDefaults() {
	if c.Server.Port <= 0 {
		c.Server.Port = DefaultServerPort
	}
	if c.Auth.Password.BcryptCost <= 0 {
		c.Auth.Password.BcryptCost = 12
	}
	if c.Auth.Password.MinLength <= 0 {
		c.Auth.Password.MinLength = 8
	}

	// KnowPost defaults
	if c.KnowPost.DetailCache.L1TTLSeconds <= 0 {
		c.KnowPost.DetailCache.L1TTLSeconds = 60
	}
	if c.KnowPost.DetailCache.NullTTLBase <= 0 {
		c.KnowPost.DetailCache.NullTTLBase = 30
	}
	if c.KnowPost.DetailCache.NullJitter <= 0 {
		c.KnowPost.DetailCache.NullJitter = 31
	}
	if c.KnowPost.DetailCache.L2TTLBase <= 0 {
		c.KnowPost.DetailCache.L2TTLBase = 60
	}
	if c.KnowPost.DetailCache.L2Jitter <= 0 {
		c.KnowPost.DetailCache.L2Jitter = 31
	}
	if c.KnowPost.DetailCache.TTLLow <= 0 {
		c.KnowPost.DetailCache.TTLLow = 30
	}
	if c.KnowPost.DetailCache.TTLMedium <= 0 {
		c.KnowPost.DetailCache.TTLMedium = 60
	}
	if c.KnowPost.DetailCache.TTLHigh <= 0 {
		c.KnowPost.DetailCache.TTLHigh = 300
	}

	// Feed defaults
	if c.KnowPost.FeedCache.SafeSize <= 0 {
		c.KnowPost.FeedCache.SafeSize = 50
	}
	if c.KnowPost.FeedCache.L1TTLSeconds <= 0 {
		c.KnowPost.FeedCache.L1TTLSeconds = 15
	}
	if c.KnowPost.FeedCache.L2IDListTTLBase <= 0 {
		c.KnowPost.FeedCache.L2IDListTTLBase = 60
	}
	if c.KnowPost.FeedCache.L2IDListJitter <= 0 {
		c.KnowPost.FeedCache.L2IDListJitter = 31
	}
	if c.KnowPost.FeedCache.L2HasMoreTTLBase <= 0 {
		c.KnowPost.FeedCache.L2HasMoreTTLBase = 10
	}
	if c.KnowPost.FeedCache.L2HasMoreJitter <= 0 {
		c.KnowPost.FeedCache.L2HasMoreJitter = 11
	}
	if c.KnowPost.FeedCache.L2ItemTTLBase <= 0 {
		c.KnowPost.FeedCache.L2ItemTTLBase = 60
	}
	if c.KnowPost.FeedCache.L2ItemJitter <= 0 {
		c.KnowPost.FeedCache.L2ItemJitter = 31
	}
	if c.KnowPost.FeedCache.L2MineTTLBase <= 0 {
		c.KnowPost.FeedCache.L2MineTTLBase = 30
	}
	if c.KnowPost.FeedCache.L2MineJitter <= 0 {
		c.KnowPost.FeedCache.L2MineJitter = 21
	}
	if c.KnowPost.FeedCache.L1MineTTLSeconds <= 0 {
		c.KnowPost.FeedCache.L1MineTTLSeconds = 30
	}
	if c.KnowPost.FeedCache.ExtendTTLBase <= 0 {
		c.KnowPost.FeedCache.ExtendTTLBase = 60
	}
	if c.KnowPost.FeedCache.TTLLow <= 0 {
		c.KnowPost.FeedCache.TTLLow = 30
	}
	if c.KnowPost.FeedCache.TTLMedium <= 0 {
		c.KnowPost.FeedCache.TTLMedium = 60
	}
	if c.KnowPost.FeedCache.TTLHigh <= 0 {
		c.KnowPost.FeedCache.TTLHigh = 300
	}

	// Relation defaults
	if c.Relation.BigVThreshold <= 0 {
		c.Relation.BigVThreshold = 500
	}
	if c.Relation.ZSetWarmLimit <= 0 {
		c.Relation.ZSetWarmLimit = 2000
	}
	if c.Relation.CacheLock.TTLMs <= 0 {
		c.Relation.CacheLock.TTLMs = 5000
	}
	if c.Relation.CacheLock.OpTimeoutMs <= 0 {
		c.Relation.CacheLock.OpTimeoutMs = 1000
	}
	if c.Relation.CacheLock.RetryIntervalMs <= 0 {
		c.Relation.CacheLock.RetryIntervalMs = 50
	}
	if c.Relation.InvalidateLock.WaitLimitMs <= 0 {
		c.Relation.InvalidateLock.WaitLimitMs = 2000
	}
	if c.Relation.L1Cache.TTLSeconds <= 0 {
		c.Relation.L1Cache.TTLSeconds = 600
	}
	if c.Relation.L1Cache.FillLimit <= 0 {
		c.Relation.L1Cache.FillLimit = 500
	}
	if c.Relation.Fallback.ExhaustedTTLMinutes <= 0 {
		c.Relation.Fallback.ExhaustedTTLMinutes = 10
	}
	if c.Relation.TokenBucket.PExpireMs <= 0 {
		c.Relation.TokenBucket.PExpireMs = 60000
	}

	// Bootstrap defaults
	if c.Bootstrap.RelationOutboxIntervalMs <= 0 {
		c.Bootstrap.RelationOutboxIntervalMs = 1000
	}
}

func (c *Config) Validate() error {
	var errs []string

	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		errs = append(errs, "server.port must be between 1 and 65535")
	}
	if c.Database.DSN() == "" {
		errs = append(errs, "database.dsn is required")
	}
	if c.Redis.Addr() == "" {
		errs = append(errs, "redis.addr is required")
	}
	if c.Server.RateLimit.Enabled && (c.Server.RateLimit.PerIP <= 0 || c.Server.RateLimit.WindowMs <= 0) {
		errs = append(errs, "rate_limit: per_ip and window_ms must be positive when enabled")
	}
	if c.Auth.Jwt.PrivateKeyPath == "" {
		errs = append(errs, "auth.jwt.private_key_path is required")
	}
	if c.Auth.Jwt.PublicKeyPath == "" {
		errs = append(errs, "auth.jwt.public_key_path is required")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}

	if c.Canal.Enabled && (c.Canal.Username == "" || c.Canal.Password == "") {
		errs = append(errs, "canal: username and password are required when enabled")
	}

	if c.Redis.RequirePass && c.Redis.Password == "" {
		errs = append(errs, "redis: require_pass is true but password is empty")
	}
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, "kafka: at least one broker is required")
	}
	if c.Cache.HotKey.BucketCount <= 0 {
		errs = append(errs, "hotkey: bucket_count must be > 0")
	}
	if len(c.Elasticsearch.URIs) == 0 {
		errs = append(errs, "elasticsearch: uris is required")
	}
	if c.OSS.Endpoint != "" || c.OSS.Bucket != "" || c.OSS.AccessKeyID != "" || c.OSS.AccessKeySecret != "" {
		if c.OSS.Endpoint == "" {
			errs = append(errs, "oss: endpoint is required when oss is configured")
		}
		if c.OSS.Bucket == "" {
			errs = append(errs, "oss: bucket is required when oss is configured")
		}
		if c.OSS.AccessKeyID == "" {
			errs = append(errs, "oss: access_key_id is required when oss is configured")
		}
		if c.OSS.AccessKeySecret == "" {
			errs = append(errs, "oss: access_key_secret is required when oss is configured")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// LoadConfig 从指定路径读取 YAML 配置文件并解析为 Config 结构体。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
