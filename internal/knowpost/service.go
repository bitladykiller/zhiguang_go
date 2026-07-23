package knowpost

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/internal/counter"
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

// CounterClient 为 counter.CounterServiceInterface 的别名。
type CounterClient = counter.CounterServiceInterface

// AuditLogger 定义审计日志接口。
type AuditLogger interface {
	LogAction(ctx context.Context, action string, userID int64, resourceType, resourceID, detail string)
}

// KnowPostService 负责 knowpost 的写路径、详情读取编排以及缓存协同。
type KnowPostService struct {
	db        *sqlx.DB
	repo      Repo
	idGen     *SnowflakeIdGenerator
	redis     *redis.Client
	l1Cache   *PrefixCache
	hotKey    *cache.HotKeyDetector
	// bloom 与空值缓存叠加：前置拦截一定不存在的 ID；nil 表示关闭。
	bloom     *cache.RedisBloom
	ossCfg    *config.OssConfig
	counter   CounterClient
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
	hotKey *cache.HotKeyDetector,
	ossCfg *config.OssConfig,
	counter CounterClient,
	feedCache FeedCacheInvalidator,
	logger *zap.Logger,
	auditLog AuditLogger,
	cfg *config.KnowPostConfig,
) *KnowPostService {
	if logger == nil {
		logger = zap.L()
	}
	svc := &KnowPostService{
		db:        db,
		repo:      NewKnowPostRepository(db),
		idGen:     idGen,
		redis:     redisClient,
		l1Cache:   l1Cache,
		hotKey:    hotKey,
		ossCfg:    ossCfg,
		counter:   counter,
		feedCache: feedCache,
		logger:    logger,
		auditLog:  auditLog,
		cfg:       cfg,
	}
	svc.bloom = newDetailBloom(redisClient, cfg, logger)
	return svc
}

// newDetailBloom 按配置装配详情存在性 Bloom；关闭或依赖缺失时返回 nil。
func newDetailBloom(redisClient *redis.Client, cfg *config.KnowPostConfig, logger *zap.Logger) *cache.RedisBloom {
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

// WarmDetailBloom 从数据库游标扫描未删除知文 ID，批量写入 Bloom。
//
// 启动时异步调用一次即可；写路径 CreateDraft 与读路径回源成功也会增量 Add。
// 空过滤器时 MightContain fail-open，因此预热完成前不会误拦详情。
func (s *KnowPostService) WarmDetailBloom(ctx context.Context) error {
	if s == nil || s.bloom == nil || s.repo == nil {
		return nil
	}
	const batch = 1000
	var lastID uint64
	var total int
	for {
		ids, err := s.repo.ListIDsForBloom(ctx, lastID, batch)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			s.bloom.AddUint64(ctx, id)
			lastID = id
			total++
		}
		if len(ids) < batch {
			break
		}
	}
	s.logger.Info("detail bloom warmed", zap.Int("count", total))
	return nil
}