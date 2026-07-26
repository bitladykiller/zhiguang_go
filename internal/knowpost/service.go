package knowpost

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
)

// parseJSON 泛型 JSON 反序列化辅助函数。
func parseJSON[T any](data []byte) (T, error) {
	var result T
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("parse json: unmarshal: %w", err)
	}
	return result, nil
}

// detailLayoutVer 定义知文详情缓存的布局版本号。
const detailLayoutVer = 1

// AuditLogger 定义审计日志接口。
type AuditLogger interface {
	LogAction(ctx context.Context, action string, userID int64, resourceType, resourceID, detail string)
}

// KnowPostService 负责 knowpost 的写路径、详情读取编排以及缓存协同。
// KnowPostService 承载知文的**写路径**：草稿、发布、编辑、删除与写后缓存失效。
//
// 详情读取已拆至 KnowPostDetailService，Feed 读取在 KnowPostFeedService——
// 三者依赖面几乎不相交，拆开后各自内聚（写侧不需要 hotKey/counter，
// 读侧不需要 db/idGen/outbox），构造语义与测试边界都更清晰。
type KnowPostService struct {
	db    *sqlx.DB
	repo  Repo
	idGen *SnowflakeIdGenerator
	redis *redis.Client
	// versions 供写后失效作废本实例的版本号短缓存（详情读取侧共用同一 freecache）。
	versions *cache.Versions
	// bloom：写侧维护存在性过滤器——发布时 ADD、删除时 DEL；nil 表示关闭。
	bloom     *cache.RedisBloom
	ossCfg    *config.OssConfig
	feedCache FeedCacheInvalidator
	logger    *zap.Logger
	auditLog  AuditLogger
	cfg       *config.KnowPostConfig
}

const (
	outboxTypeKnowPostMetadataUpdated   = "KnowPostMetadataUpdated"
	outboxTypeKnowPostPublished         = "KnowPostPublished"
	outboxTypeKnowPostDeleted           = "KnowPostDeleted"
	outboxTypeKnowPostVisibilityUpdated = "KnowPostVisibilityUpdated"
	outboxTypeKnowPostTopUpdated        = "KnowPostTopUpdated"
)

// NewKnowPostService 使用完整依赖创建知文服务实例。
//
// 参数：
//   - db: *sqlx.DB，MySQL 数据库连接实例。
//   - idGen: *SnowflakeIdGenerator，雪花算法 ID 生成器。
//   - redisClient: *redis.Client，Redis 客户端，用于 L2 分布式缓存。
//   - l1Cache: *PrefixCache，带前缀的 L1 进程级缓存实例。
//   - hotKey: *cache.HotKeyDetector，热点探测器。
//   - ossCfg: *config.OssConfig，OSS 对象存储配置。
//   - counter: CounterClient 接口实例，nil 表示不使用计数器。
//   - feedCache: FeedCacheInvalidator 接口实例，nil 表示不失效 feed 缓存。
func NewKnowPostService(
	db *sqlx.DB,
	idGen *SnowflakeIdGenerator,
	redisClient *redis.Client,
	l1Cache *PrefixCache,
	bloom *cache.RedisBloom,
	ossCfg *config.OssConfig,
	feedCache FeedCacheInvalidator,
	logger *zap.Logger,
	auditLog AuditLogger,
	cfg *config.KnowPostConfig,
) *KnowPostService {
	if logger == nil {
		logger = zap.L()
	}
	return &KnowPostService{
		db:        db,
		repo:      NewKnowPostRepository(db),
		idGen:     idGen,
		redis:     redisClient,
		versions:  newDetailVersions(redisClient, l1Cache, cfg),
		bloom:     bloom,
		ossCfg:    ossCfg,
		feedCache: feedCache,
		logger:    logger,
		auditLog:  auditLog,
		cfg:       cfg,
	}
}

// NewDetailBloom 按配置装配第三方 RedisBloom CF 客户端；关闭时返回 nil（不创建适配层）。
// 由 bootstrap 创建一次，同时注入写服务（发布 ADD / 删除 DEL）与详情读服务（EXISTS 预判）。
func NewDetailBloom(redisClient *redis.Client, cfg *config.KnowPostConfig, logger *zap.Logger) *cache.RedisBloom {
	if redisClient == nil || cfg == nil {
		return nil
	}
	dc := cfg.DetailCache
	enabled := true
	if dc.BloomEnabled != nil {
		enabled = *dc.BloomEnabled
	}
	return cache.NewRedisBloom(redisClient, cache.BloomConfig{
		Enabled:           enabled,
		ExpectedItems:     dc.BloomExpectedItems,
		FalsePositiveRate: dc.BloomFalsePositiveRate,
		Key:               dc.BloomKey,
	}, logger)
}
