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
	ossCfg    *config.OssConfig
	counter   CounterClient
	feedCache FeedCacheInvalidator
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
// 功能：构建 KnowPostService 的核心依赖，包括数据库连接、ID 生成器、Redis 客户端、
// L1 进程级缓存（freecache）、热点探测器和 OSS 配置。
//
// 参数：
//   - db: *sqlx.DB，MySQL 数据库连接实例。
//   - idGen: *SnowflakeIdGenerator，雪花算法 ID 生成器，用于生成知文主键和 outbox 事件 ID。
//   - redisClient: *redis.Client，Redis 客户端，用于 L2 分布式缓存。
//   - l1Cache: *freecache.Cache，L1 进程级缓存实例，约 50ns 响应。
//   - hotKey: *cache.HotKeyDetector，热点探测器，用于识别高频访问的 key 并延长其 TTL。
//   - ossCfg: *config.OssConfig，OSS 对象存储配置，用于生成文件的公开访问地址。
//
// 返回值：*KnowPostService，创建好的服务实例。
//
// 设计决策：
//   - 计数器（counter）和 Feed 缓存失效器（feedCache）不在构造函数中注入，
//     而是通过后续的 Setter 方法注入。
//     这是因为计数器服务依赖自身依赖图的构建（Redis + Kafka Producer），
//     在 KnowPostService 初始化时尚未准备就绪。
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

// SetCounterClient 注入计数器的读接口（依赖注入 setter）。
//
// 功能：将可选的计数器客户端绑定到 KnowPostService 上。
// 计数器服务在详情页和 Feed 列表中提供实时的点赞数、收藏数和当前用户的点赞/收藏状态。
//
// 参数：
//   - c: CounterClient 接口实例。nil 表示不使用计数器，详情页中的点赞/收藏状态将保持默认零值。
//
// WHY 使用接口注入而非具体类型：
//   - 解耦：knowpost 包无需 import counter 包，避免循环依赖。
//   - 可测试：测试时可以传入 MockCounterClient 避免依赖 Redis 和 Kafka。
//   - 可降级：如果计数器服务未初始化，GetCounts/IsLiked/IsFaved 将返回默认零值，
//     不会阻断详情页的展示。
func (s *KnowPostService) SetCounterClient(c CounterClient) { s.counter = c }

// SetFeedCacheInvalidator 注入 Feed 缓存失效器的接口（依赖注入 setter）。
//
// 功能：将 FeedCacheInvalidator 绑定到 KnowPostService 上。
// 在知文发生变更（发布、更新、删除、修改可见性、修改置顶）时，
// 通过此接口通知 KnowPostFeedService 失效 feed 缓存。
//
// 参数：
//   - f: FeedCacheInvalidator 接口实例。
//
// WHY 使用接口注入：
//   - KnowPostService 不关心 Feed 缓存的具体实现（Redis key 结构、过期策略等），
//     它只需要在变更时触发失效接口即可。
//   - 避免了 knowpost 包内部两个 service 之间的直接循环依赖（KnowPostService 与 KnowPostFeedService
//     互相引用）。
func (s *KnowPostService) SetFeedCacheInvalidator(f FeedCacheInvalidator) { s.feedCache = f }
