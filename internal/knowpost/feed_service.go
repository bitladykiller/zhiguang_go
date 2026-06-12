package knowpost

import (
	"context"

	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/internal/cache"
)

const feedLayoutVer = 1

const (
	publicFeedVersionKey = "feed:public:version"
	mineFeedVersionKey   = "feed:mine:version:%d"
)

// KnowPostFeedService 实现公共 Feed 和“我的已发布”列表的读路径。
//
// 当前按职责拆分为多个文件：
//   - feed_public_service.go: 公共 Feed 查询与回源锁区
//   - feed_mine_service.go: 我的已发布列表查询
//   - feed_fragment_cache.go: Redis 碎片缓存组装与失效
//   - feed_item_service.go: 行记录映射、用户态增强、热点条目处理
//   - feed_helpers.go: 版本号、分页辅助函数和 JSON 反序列化
//
// 这样做的目的是让“公共 feed、我的 feed、缓存碎片、条目增强、工具函数”
// 各自维持清晰边界，避免再次回到一个几百行读模型大文件。
type KnowPostFeedService struct {
	repo     *KnowPostRepository
	redis    *redis.Client
	l1Public *freecache.Cache
	l1Mine   *freecache.Cache
	hotKey   *cache.HotKeyDetector
	counter  CounterClient
}

// KnowPostFeedServiceDeps 描述 Feed 读取服务的装配参数。
type KnowPostFeedServiceDeps struct {
	Repo     *KnowPostRepository
	Redis    *redis.Client
	L1Public *freecache.Cache
	L1Mine   *freecache.Cache
	HotKey   *cache.HotKeyDetector
	Counter  CounterClient
}

// FeedCacheInvalidator 暴露知文写操作所需的 feed 缓存失效能力。
type FeedCacheInvalidator interface {
	InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64)
}

// NewKnowPostFeedService 创建 Feed 读取服务。
func NewKnowPostFeedService(deps KnowPostFeedServiceDeps) *KnowPostFeedService {
	return &KnowPostFeedService{
		repo:     deps.Repo,
		redis:    deps.Redis,
		l1Public: deps.L1Public,
		l1Mine:   deps.L1Mine,
		hotKey:   deps.HotKey,
		counter:  deps.Counter,
	}
}
