package knowpost

import (
	"context"

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
	db        *sqlx.DB
	repo      *KnowPostRepository
	idGen     *SnowflakeIdGenerator
	redis     *redis.Client
	l1Cache   *freecache.Cache
	hotKey    *cache.HotKeyDetector
	ossCfg    *config.OSSConfig
	counter   CounterClient
	feedCache FeedCacheInvalidator
}

// KnowPostServiceDeps 描述知文写路径服务在构造期需要的全部依赖。
//
// 设计原因：
//   - KnowPostService 已经同时协调 DB、Redis、L1 缓存、热点探测和跨领域接口；
//   - 如果继续使用长参数列表 + Setter 后置注入，bootstrap 会出现装配顺序依赖；
//   - 改成 deps struct 后，调用方可以显式看到依赖拓扑，且对象创建完成后即处于可用状态。
type KnowPostServiceDeps struct {
	DB        *sqlx.DB
	IDGen     *SnowflakeIdGenerator
	Redis     *redis.Client
	L1Cache   *freecache.Cache
	HotKey    *cache.HotKeyDetector
	OSSConfig *config.OSSConfig
	Counter   CounterClient
	FeedCache FeedCacheInvalidator
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
// 与早期版本不同，当前构造函数要求跨领域依赖也在创建时显式给出。
// 这样 bootstrap 不再需要先创建半成品对象再调用多个 Setter，能够减少装配顺序耦合。
func NewKnowPostService(deps KnowPostServiceDeps) *KnowPostService {
	return &KnowPostService{
		db:        deps.DB,
		repo:      NewKnowPostRepository(deps.DB),
		idGen:     deps.IDGen,
		redis:     deps.Redis,
		l1Cache:   deps.L1Cache,
		hotKey:    deps.HotKey,
		ossCfg:    deps.OSSConfig,
		counter:   deps.Counter,
		feedCache: deps.FeedCache,
	}
}

// setFreeCacheValue 以 best-effort 方式写入进程内 freecache。
//
// L1 缓存不是权威数据源，写入失败时不应影响主流程，因此这里只做尽力而为的缓存回填。
func setFreeCacheValue(cache *freecache.Cache, key string, value []byte, ttlSeconds int) {
	if cache == nil {
		return
	}
	if err := cache.Set([]byte(key), value, ttlSeconds); err != nil {
		return
	}
}
