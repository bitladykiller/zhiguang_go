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
type CounterClient interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
	IsLiked(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
	IsFaved(ctx context.Context, userID uint64, entityType, entityID string) (bool, error)
}

// RagIndexer 定义内容变更后触发 RAG 建索引的依赖接口。
type RagIndexer interface {
	EnsureIndexed(postID uint64) error
}

// KnowPostService 负责 knowpost 的写路径、详情读取编排以及缓存协同。
//
// WHY：虽然文件被拆分了，但运行时依赖仍属于同一个服务对象；
// 这样既能保持依赖关系集中，又能让文件级职责更清晰、更容易定位和测试。
type KnowPostService struct {
	db           *sqlx.DB
	repo         *KnowPostRepository
	idGen        *SnowflakeIdGenerator
	redis        *redis.Client
	l1Cache      *freecache.Cache
	hotKey       *cache.HotKeyDetector
	ossCfg       *config.OssConfig
	counter      CounterClient
	ragIndexer   RagIndexer
	feedCache    FeedCacheInvalidator
	singleFlight sync.Map // key -> *sync.Mutex for stampede prevention
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

// SetRagIndexer 注入 RAG 索引依赖。
func (s *KnowPostService) SetRagIndexer(r RagIndexer) { s.ragIndexer = r }

// SetFeedCacheInvalidator 注入 feed 缓存失效依赖。
func (s *KnowPostService) SetFeedCacheInvalidator(f FeedCacheInvalidator) { s.feedCache = f }
