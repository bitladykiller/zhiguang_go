package knowpost

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
)

// detailLayoutVer 定义知文详情缓存的布局版本号。
// 用于缓存键编码，递增版本号可使旧缓存整体失效。
const detailLayoutVer = 1

// CounterClient 定义 KnowPostService 所依赖的计数器读写接口。
//
// 使用接口而非具体类型注入的原因：
//   - 解耦：knowpost 包无需 import counter 包，避免循环依赖。
//   - 可测试：测试时可以传入 MockCounterClient 避免依赖 Redis 和 Kafka。
type CounterClient interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	GetCountsBatch(ctx context.Context, entityType string, entityIDs, metrics []string) (map[string]map[string]int32, error)
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	BatchIsLiked(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
	BatchIsFaved(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
}

// KnowPostService 负责 knowpost 的写路径、详情读取编排以及缓存协同。
//
// WHY：虽然文件按职责拆分为 cache.go、detail_service.go、write_service.go 等多个文件，
// 但运行时依赖仍属于同一个 KnowPostService 结构体。
// 这种拆分方式既能保持依赖关系集中在一处（service.go 的构造函数），
// 又能让每个文件内的函数职责更清晰，更容易定位和单独测试。
type KnowPostService struct {
	db        *sqlx.DB
	repo      *KnowPostRepository
	idGen     *SnowflakeIdGenerator
	redis     *redis.Client
	l1Cache   *PrefixCache
	hotKey    *cache.HotKeyDetector
	ossCfg    *config.OssConfig
	counter   CounterClient
	feedCache FeedCacheInvalidator
	logger    *zap.Logger
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
) *KnowPostService {
	if logger == nil {
		logger = zap.L()
	}
	return &KnowPostService{
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
	}
}
