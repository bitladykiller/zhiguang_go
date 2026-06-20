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
	Relation   RelationConfig    `yaml:"relation"`
	Prometheus PrometheusConfig `yaml:"prometheus"`
}

const (
	DefaultServerPort              = 8080
	DefaultHTTPRequestTimeoutMs    = 30000
	DefaultCounterPublishTimeoutMs = 3000
)

// ServerConfig 控制 HTTP 服务监听配置。
type ServerConfig struct {
	Port                 int             `yaml:"port"` // default: 8080
	Mode                 string          `yaml:"mode"` // "debug", "release", or "test"
	RequestTimeoutMs     int             `yaml:"request_timeout_ms"` // default: 30000
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
	Charset         string `yaml:"charset"`           // default: utf8mb4
	MaxOpenConns    int    `yaml:"max_open_conns"`    // max open connections
	MaxIdleConns    int    `yaml:"max_idle_conns"`    // max idle connections
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"` // max connection lifetime in seconds
	ConnMaxIdleTime int    `yaml:"conn_max_idle_time"` // max idle connection time in seconds
	DialTimeoutMs   int    `yaml:"dial_timeout_ms"`   // 连接超时（毫秒）
	ReadTimeoutMs   int    `yaml:"read_timeout_ms"`   // 读超时（毫秒）
	WriteTimeoutMs  int    `yaml:"write_timeout_ms"`  // 写超时（毫秒）
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
//   - 超时参数（dial_timeout_ms、read_timeout_ms、write_timeout_ms）会添加到 DSN 参数中。
func (c *DatabaseConfig) DSN() string {
	dsn := c.User + ":" + url.QueryEscape(c.Password) + "@tcp(" + c.Host + ":" +
		strconv.Itoa(c.Port) + ")/" + c.Name + "?charset=" + c.Charset + "&parseTime=True&loc=Local"

	// 添加超时参数
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
	Enabled    *bool    `yaml:"enabled"`    // 显式功能开关，nil 表示跟随配置完整性判断
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
	TTLSeconds int `yaml:"ttl_seconds"`
	MaxSize    int `yaml:"max_size"`
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
	MaxLocalKeys         int `yaml:"max_local_keys"`          // 本地 map 最大键数限制，0 表示使用默认值 100000
}

// LLMConfig 配置 AI 模型连接信息。
type LLMConfig struct {
	Enabled       *bool          `yaml:"enabled"`    // 显式功能开关，nil 表示跟随配置完整性判断
	DeepSeek      DeepSeekConfig `yaml:"deepseek"`
	OpenAI        OpenAIConfig   `yaml:"openai"`
	TimeoutMs     int            `yaml:"timeout_ms"`     // HTTP 客户端超时（毫秒），默认 30000
	MaxContentLen int            `yaml:"max_content_len"` // 内容截断长度，默认 2000
	MaxTokens     int            `yaml:"max_tokens"`      // 生成最大 token 数，默认 100
	SystemPrompt  string         `yaml:"system_prompt"`   // 系统提示词
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
	BigVThreshold int                     `yaml:"big_v_threshold"`
	TokenBucket   RelationTokenBucketConfig `yaml:"token_bucket"`
	CacheTTL      int                     `yaml:"cache_ttl"`
}

// RelationTokenBucketConfig 配置令牌桶限流。
type RelationTokenBucketConfig struct {
	Capacity int `yaml:"capacity"`
	Rate     int `yaml:"rate"`
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

// Validate 校验配置中的关键字段是否合法。
//
// 验证规则：
//   - Server.Port 必须在 1~65535 范围内
//   - Database.DSN 不能为空（DSN 方法内部从多个字段拼接，但此处校验 DSN() 返回值）
//   - Redis.Addr 不能为空
//   - Jwt.PrivateKeyPath 和 Jwt.PublicKeyPath 不能为空
//
// 注意：
//   - DSN() 会拼接 User、Password、Host、Port、Name 等字段生成连接串，
//     但 Validate() 直接用 Database.DSN 字段（若有独立 DSN 字段）。
//     当前 DatabaseConfig 没有独立的 DSN 字段，而是通过 DSN() 方法拼装，
//     因此此处校验 DSN() 返回值是否为 ""。
//
// 返回值：
//   - error: 如果有任何字段不合法，返回包含所有错误信息的 error
//   - nil: 所有字段合法
// ApplyDefaults 为未显式配置的字段填充默认值（在 Validate 之前调用）。
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

	// 7. Redis — 如果 RequirePass 为 true，密码不能为空
	// 8. Kafka — Broker 列表不能为空，至少配置一个 topic
	// 9. HotKey — BucketCount 必须 > 0
	// 10. Elasticsearch — Addresses 不能为空
	// 11. OSS — 若配置了任一字段，则所有必填字段不能为空
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
//
// 功能：
//
//	Step 1: 使用 os.ReadFile 读取 YAML 文件的完整内容。
//	Step 2: 使用 yaml.Unmarshal 将 YAML 字节数据反序列化为 Config 结构体。
//	Step 3: 返回解析后的 Config 指针。
//
// 参数：
//   - path: YAML 配置文件的路径（如 "config/config-local.yaml"）
//
// 返回值：
//   - *Config: 解析后的配置结构体
//   - error: 文件读取失败或 YAML 格式非法时返回
//
// 函数调用说明：
//   - os.ReadFile(path):
//     Go 标准库函数，读取文件的完整内容为 []byte。
//     在 Go 1.16 中引入，替代了旧的 ioutil.ReadFile。
//   - yaml.Unmarshal(data, cfg):
//     gopkg.in/yaml.v3 包的 YAML 反序列化函数。
//     根据结构体上的 yaml tag 将 YAML 字段映射到结构体字段。
//
// 注意：
//
//	此函数不会校验配置中的字段值是否合理（如端口是否在有效范围、超时值是否为正等），
//	调用方应在构造连接时自行检查或使用默认值。
//	也不会设置默认值（如 charset 默认 utf8mb4），需要调用方自行处理。
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

// Validate 校验配置中的关键字段是否合法。
