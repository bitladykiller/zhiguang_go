package knowpost

import (
	"context"
	"sync"

	"github.com/coocood/freecache"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
)

const detailLayoutVer = 1

// CounterClient 定义 KnowPostService 所依赖的计数器读写接口。
//
// 使用接口而非具体类型注入的原因：
//   - 解耦：knowpost 包无需 import counter 包，避免循环依赖。
//   - 可测试：测试时可以传入 MockCounterClient 避免依赖 Redis 和 Kafka。
type CounterClient interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
}

// KnowPostService 负责 knowpost 的写路径、详情读取编排以及缓存协同。
//
// WHY：虽然文件按职责拆分为 cache.go、detail_service.go、write_service.go 等多个文件，
// 但运行时依赖仍属于同一个 KnowPostService 结构体。
// 这种拆分方式既能保持依赖关系集中在一处（service.go 的构造函数），
// 又能让每个文件内的函数职责更清晰，更容易定位和单独测试。
type KnowPostService struct {
	db           *sqlx.DB
	repo         *KnowPostRepository
	idGen        *SnowflakeIdGenerator
	redis        *redis.Client
	l1Cache      *freecache.Cache
	hotKey       *cache.HotKeyDetector
	ossCfg       *config.OssConfig
	counter      CounterClient
	feedCache    FeedCacheInvalidator
	singleFlight sync.Map // key -> *sync.Mutex，用于防止缓存击穿时的惊群效应
}

const (
	outboxTypeKnowPostMetadataUpdated   = "KnowPostMetadataUpdated"
	outboxTypeKnowPostPublished         = "KnowPostPublished"
	outboxTypeKnowPostDeleted           = "KnowPostDeleted"
	outboxTypeKnowPostVisibilityUpdated = "KnowPostVisibilityUpdated"
	outboxTypeKnowPostTopUpdated        = "KnowPostTopUpdated"
)

// NewKnowPostService 使用完整依赖创建服务实例。
func NewKnowPostService(
	db *sqlx.DB,
	idGen *SnowflakeIdGenerator,
	redisClient *redis.Client,
	l1Cache *freecache.Cache,
	hotKey *cache.HotKeyDetector,
	ossCfg *config.OssConfig,
) *KnowPostService {
	return &KnowPostService{
		db:      db,
		repo:    NewKnowPostRepository(db),
		idGen:   idGen,
		redis:   redisClient,
		l1Cache: l1Cache,
		hotKey:  hotKey,
		ossCfg:  ossCfg,
	}
}

// --- [依赖注入] --- //

// SetCounterClient 注入计数器依赖。
func (s *KnowPostService) SetCounterClient(c CounterClient) { s.counter = c }

// SetFeedCacheInvalidator 注入 feed 缓存失效依赖。
func (s *KnowPostService) SetFeedCacheInvalidator(f FeedCacheInvalidator) { s.feedCache = f }
