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
	"os"
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
	Charset         string `yaml:"charset"`            // default: utf8mb4
	MaxOpenConns    int    `yaml:"max_open_conns"`     // max open connections
	MaxIdleConns    int    `yaml:"max_idle_conns"`     // max idle connections
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"`  // max connection lifetime in seconds
	ConnMaxIdleTime int    `yaml:"conn_max_idle_time"` // max idle connection time in seconds
	DialTimeoutMs   int    `yaml:"dial_timeout_ms"`    // 连接超时（毫秒）
	ReadTimeoutMs   int    `yaml:"read_timeout_ms"`    // 读超时（毫秒）
	WriteTimeoutMs  int    `yaml:"write_timeout_ms"`   // 写超时（毫秒）
}

// DSN 根据配置字段拼装 MySQL 的数据源连接串。
func (c *DatabaseConfig) DSN() string {
	dsn := c.User + ":" + c.Password + "@tcp(" + c.Host + ":" +
		itoa(c.Port) + ")/" + c.Name + "?charset=" + c.Charset + "&parseTime=True&loc=Local"

	if c.DialTimeoutMs > 0 {
		dsn += "&timeout=" + itoa(c.DialTimeoutMs) + "ms"
	}
	if c.ReadTimeoutMs > 0 {
		dsn += "&readTimeout=" + itoa(c.ReadTimeoutMs) + "ms"
	}
	if c.WriteTimeoutMs > 0 {
		dsn += "&writeTimeout=" + itoa(c.WriteTimeoutMs) + "ms"
	}

	return dsn
}

// RedisConfig 配置 Redis 连接参数。
type RedisConfig struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Password        string `yaml:"password"`
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
func (c *RedisConfig) Addr() string {
	return c.Host + ":" + itoa(c.Port)
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
	Enabled    *bool    `yaml:"enabled"` // 显式功能开关，nil 表示跟随配置完整性判断
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
	CodeLength         int            `yaml:"code_length"`
	TTL                time.Duration  `yaml:"ttl"`
	MaxAttempts        int            `yaml:"max_attempts"`
	SendInterval       time.Duration  `yaml:"send_interval"`
	DailyLimit         int            `yaml:"daily_limit"`
	OperationTimeoutMs int            `yaml:"operation_timeout_ms"`
	Lock               AuthLockConfig `yaml:"lock"`
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
	Enabled         *bool  `yaml:"enabled"` // 显式功能开关，nil 表示跟随配置完整性判断
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
	Consumer ConsumerConfig `yaml:"consumer"`
	Repair   RepairConfig   `yaml:"repair"`
	Rebuild  RebuildConfig  `yaml:"rebuild"`
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
	TTLSeconds         int `yaml:"ttl_seconds"`
	MaxSize            int `yaml:"max_size"`
	FreeCacheDefaultMB int `yaml:"free_cache_default_mb"`
}

// HotKeyConfig 控制热点键识别与 TTL 延长行为。
type HotKeyConfig struct {
	BucketSizeSeconds    int `yaml:"bucket_size_seconds"`
	BucketCount          int `yaml:"bucket_count"`
	FlushIntervalSeconds int `yaml:"flush_interval_seconds"`
	StatTTLSeconds       int `yaml:"stat_ttl_seconds"`
	LevelLow             int `yaml:"level_low"`
	LevelMedium          int `yaml:"level_medium"`
	LevelHigh            int `yaml:"level_high"`
	ExtendLowSeconds     int `yaml:"extend_low_seconds"`
	ExtendMediumSeconds  int `yaml:"extend_medium_seconds"`
	ExtendHighSeconds    int `yaml:"extend_high_seconds"`
	HotMarkTTLSeconds    int `yaml:"hot_mark_ttl_seconds"`
}

// LLMConfig 配置 AI 模型连接信息。
type LLMConfig struct {
	Enabled   *bool          `yaml:"enabled"` // 显式功能开关，nil 表示跟随配置完整性判断
	DeepSeek  DeepSeekConfig `yaml:"deepseek"`
	OpenAI    OpenAIConfig   `yaml:"openai"`
	TimeoutMs int            `yaml:"timeout_ms"` // HTTP 客户端超时（毫秒），默认 30000
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
	BigVThreshold int                       `yaml:"big_v_threshold"`
	TokenBucket   RelationTokenBucketConfig `yaml:"token_bucket"`
	CacheTTL      int                       `yaml:"cache_ttl"`
}

// RelationTokenBucketConfig 配置令牌桶限流。
type RelationTokenBucketConfig struct {
	Capacity int `yaml:"capacity"`
	Rate     int `yaml:"rate"`
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

// itoa 在不引入 strconv 的前提下把 int 转成字符串。
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
