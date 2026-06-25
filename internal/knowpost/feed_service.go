package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
)

// feedLayoutVer 定义 Feed 列表缓存的布局版本号。
// 用于缓存键编码，递增版本号可使旧缓存整体失效。
const feedLayoutVer = 1

const (
	publicFeedVersionKey = "feed:public:version"
	mineFeedVersionKey   = "feed:mine:version:%d"
)

// Feed 缓存 TTL 常量。
// Base + Jitter 的设计用于防缓存雪崩 — 同一批写入的不同 key TTL 略有差异。
const (
	defaultSafeSize  = 50
	secondsPerHour   = 3600
	l1FeedCacheTTL   = 15
	l2IDListTTLBase  = 60
	l2IDListJitter   = 31
	l2HasMoreTTLBase = 10
	l2HasMoreJitter  = 11
	l2ItemTTLBase    = 60
	l2ItemJitter     = 31
	l2MineTTLBase    = 30
	l2MineJitter     = 21
	l1MineCacheTTL   = 30
	extendTTLBase    = 60
)

// KnowPostFeedService 实现基于碎片缓存架构的 Feed 列表流读取。
//
// 缓存架构（三级、碎片化）：
//
//	L1（freecache）：整页响应缓存，约 50ns。
//	L2（Redis 碎片缓存）：
//	  - IDs 列表（按小时分槽）：保存某一页的有序帖子 ID 列表。
//	  - Item 缓存（按帖子维度）：保存单篇帖子的元信息（标题、描述、封面等）。
//	  - hasMore 软缓存：标记是否还有下一页数据。
//	L3（MySQL）：权威数据源，只会在 Redis 看门狗分布式锁内回源。
//
// WHY 使用碎片缓存而非整页缓存：
//   - 整页缓存方案中，单篇帖子更新会致使所有包含该帖子的分页结果失效，
//     缓存失效范围大，命中率低。
//   - 碎片缓存方案中，帖子创建/更新只需要失效该帖子的 Item 碎片，
//     以及递增 feed version（让旧分页整页缓存整体过期），
//     不会影响其他帖子的缓存。
//
// WHY 按小时分槽保存 IDs：
// 可以控制热门时间窗口失效时的影响范围——只影响该小时的槽，
// 其他小时的缓存不受影响。
type KnowPostFeedService struct {
	repo     *KnowPostRepository
	redis    *redis.Client
	l1Public *PrefixCache
	l1Mine   *PrefixCache
	hotKey   *cache.HotKeyDetector
	counter  CounterClient
	logger   *zap.Logger
}

// FeedCacheInvalidator 暴露知文写操作所需的 feed 缓存失效能力。
//
// 与 service.go 中的 FeedCacheInvalidator 接口不同：
//   - service.go 中的接口用于 KnowPostService 的依赖注入（外部调用方视角）。
//   - 此处的接口用于 KnowPostFeedService 自身实现（内部实现视角）。
type FeedCacheInvalidator interface {
	InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64)
}

// NewKnowPostFeedService 创建带有 L1 缓存实例的 Feed 服务。
//
// 参数：
//   - repo: 知文仓储
//   - redisClient: Redis 客户端
//   - l1Public: 公共 Feed 的 L1 缓存（带前缀的 freecache）
//   - l1Mine: 我的 Feed 的 L1 缓存（带前缀的 freecache）
//   - hotKey: 热点探测器
//   - counter: 计数器客户端，nil 表示不使用计数器
func NewKnowPostFeedService(
	repo *KnowPostRepository,
	redisClient *redis.Client,
	l1Public *PrefixCache,
	l1Mine *PrefixCache,
	hotKey *cache.HotKeyDetector,
	counter CounterClient,
) *KnowPostFeedService {
	return &KnowPostFeedService{
		repo:     repo,
		redis:    redisClient,
		l1Public: l1Public,
		l1Mine:   l1Mine,
		hotKey:   hotKey,
		counter:  counter,
		logger:   zap.L(),
	}
}

// ============================================================================
// 获取公共 Feed
// ============================================================================

// GetPublicFeed 获取公共 Feed 列表（按最新发布时间排序）。
//
// 功能：以三级缓存架构读取公共 Feed，支持分页。
//
// 读取路径：
//  1. L1（freecache）整页缓存：以 "feed:public:{size}:{page}:v1:{feedVersion}" 为键，
//     命中即返回整页 JSON。命中后还会对每个条目调用 recordItemHotKey
//     来识别热点条目并延长其单独碎片缓存的 TTL。
//  2. L2（Redis 碎片缓存）：先通过 assembleFromCache 尝试从碎片缓存组装整页数据。
//     碎片缓存包括三部分：ID 列表（Redis List）、单个条目缓存（Redis String）、
//     以及 hasMore 软缓存（Redis String）。
//  3. Redis 看门狗分布式锁 + L3（MySQL）：当碎片缓存也未命中时，
//     进入 getPublicFeedUnderLock 的锁区回源数据库。
//
// 参数：
//   - ctx: context.Context，用于传递请求上下文和控制超时。
//   - page: int，页码，从 1 开始。若传入 <= 0 则强制为 1。
//   - size: int，每页数量，会被 clamp 到 [1, 50] 之间。
//   - currentUserID: *uint64，当前用户 ID（可选）。
//
// 返回值：
//   - *FeedPageResponse: 包含 Items（FeedItemResponse 列表）、Page、Size 和 HasMore。
//   - error: 数据库查询错误等。
func (s *KnowPostFeedService) GetPublicFeed(ctx context.Context, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	safeSize := clamp(size, 1, defaultSafeSize)
	safePage := max(page, 1)
	feedVersion := s.currentPublicFeedVersion(ctx)
	localPageKey := fmt.Sprintf("feed:public:%d:%d:v%d:%d", safeSize, safePage, feedLayoutVer, feedVersion)

	hourSlot := time.Now().Unix() / secondsPerHour
	idsKey := fmt.Sprintf("feed:public:ids:%d:%d:%d:%d", feedVersion, safeSize, hourSlot, safePage)
	hasMoreKey := idsKey + ":hasMore"

	// --- L1：freecache ---
	if val, err := s.l1Public.Get([]byte(localPageKey)); err == nil {
		resp, parseErr := s.parseFeedPage(val)
		if parseErr == nil {
			for _, item := range resp.Items {
				s.recordItemHotKey(ctx, item.ID)
			}
			return &FeedPageResponse{
				Items:   s.enrichItems(ctx, resp.Items, currentUserID),
				Page:    resp.Page,
				Size:    resp.Size,
				HasMore: resp.HasMore,
			}, nil
		}
	}

	// --- L2：Redis 碎片缓存 ---
	if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, safePage, safeSize, currentUserID); resp != nil {
		s.cacheFeedPage(localPageKey, resp, s.l1Public)
		for _, item := range resp.Items {
			s.recordItemHotKey(ctx, item.ID)
		}
		return resp, nil
	}

	// --- Redis 分布式锁 ---
	return s.getPublicFeedUnderLock(ctx, idsKey, hasMoreKey, localPageKey, safePage, safeSize, currentUserID)
}

// getPublicFeedUnderLock 在 Redis 看门狗分布式锁保护下从 MySQL 查询公共 Feed。
//
// 功能：防止缓存击穿的线程安全回源方法。当 L1（freecache）和 L2（Redis 碎片缓存）
// 同时未命中时，多个并发请求会竞争同一个 idsKey 对应的 Redis 分布式锁。
//
// 实现细节：
//  1. 通过 Redis SET NX PX 抢占 `lock:{idsKey}` 分布式锁。
//     持锁成功后立即启动本地看门狗协程，周期性续租，避免长尾回源时锁提前过期。
//     拿到锁的实例执行回源查询，没拿到锁的实例循环等待并重检缓存。
//  2. 加锁后再次检查碎片缓存（double-check 模式），避免重复查库。
//  3. 查询 MySQL：LIMIT size+1 来判断是否有下一页（HasMore）。
//  4. 将查询结果映射为 FeedItemResponse 并写入碎片缓存和 L1 缓存。
//  5. 写入碎片缓存时批量写入 ID 列表、单个条目缓存和 hasMore 标记。
//  6. 条目叠加当前用户状态（enrichItems）后才返回给调用方。
//
// WHY 使用 size+1 查询：
// 通过多查一条来判断是否还有下一页，避免额外执行 COUNT 查询。
//
// 参数：
//   - ctx: context.Context。
//   - idsKey: string，Redis 碎片缓存中存储 ID 列表的键。
//   - hasMoreKey: string，Redis 存储 hasMore 标记的键。
//   - localPageKey: string，L1（freecache）整页缓存的键。
//   - page: int，页码。
//   - size: int，每页条数。
//   - currentUserID: *uint64，当前用户 ID。
//
// 返回值：
//   - *FeedPageResponse: 已叠加用户状态的分页结果。
//   - error: 数据库等错误。
func (s *KnowPostFeedService) getPublicFeedUnderLock(ctx context.Context, idsKey, hasMoreKey, localPageKey string, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	lockKey := "lock:" + idsKey
	return cacheReadThrough(ctx, s.redis, lockKey,
		s.checkFeedCache(idsKey, hasMoreKey, localPageKey, page, size, currentUserID),
		s.fetchFeed(idsKey, hasMoreKey, localPageKey, page, size, currentUserID),
	)
}

// checkFeedCache 作为 cacheReadThrough 的 checkCache 回调。
func (s *KnowPostFeedService) checkFeedCache(idsKey, hasMoreKey, localPageKey string, page, size int, currentUserID *uint64) func(ctx context.Context) (*FeedPageResponse, bool, error) {
	return func(ctx context.Context) (*FeedPageResponse, bool, error) {
		if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, page, size, currentUserID); resp != nil {
			s.cacheFeedPage(localPageKey, resp, s.l1Public)
			return resp, true, nil
		}
		return nil, false, nil
	}
}

// fetchFeed 作为 cacheReadThrough 的 missHandler 回调。
func (s *KnowPostFeedService) fetchFeed(idsKey, hasMoreKey, localPageKey string, page, size int, currentUserID *uint64) func(ctx context.Context) (*FeedPageResponse, error) {
	return func(ctx context.Context) (*FeedPageResponse, error) {
		offset := (page - 1) * size
		rows, err := s.repo.ListFeedPublic(ctx, size+1, offset)
		if err != nil {
			return nil, fmt.Errorf("get public feed: list: %w", err)
		}

		hasMore := len(rows) > size
		if hasMore {
			rows = rows[:size]
		}

		const includeIsTop = false
		items := s.mapRowsToItems(ctx, rows, currentUserID, includeIsTop)

		resp := &FeedPageResponse{
			Items:   items,
			Page:    page,
			Size:    size,
			HasMore: hasMore,
		}

		s.writeFragmentCaches(ctx, idsKey, hasMoreKey, size, rows, items, hasMore)
		s.cacheFeedPage(localPageKey, resp, s.l1Public)

		return &FeedPageResponse{
			Items:   s.enrichItems(ctx, items, currentUserID),
			Page:    page,
			Size:    size,
			HasMore: hasMore,
		}, nil
	}
}

// ============================================================================
// 获取我的已发布内容
// ============================================================================

// GetMyPublished 返回当前用户已发布的知文列表（自己的"我的 Feed"）。
//
// 功能：查询某个用户的全部已发布知文（不含已删除的），按置顶优先、创建时间倒序排列。
// 此接口也采用三级缓存，但与公共 Feed 的碎片缓存结构不同：
//
// "我的 Feed" 的缓存策略（整页缓存）：
//   - L1（freecache）：整页 JSON 缓存，键为 "feed:mine:{userID}:{size}:{page}:{feedVersion}"。
//   - L2（Redis）：同样也是整页 JSON 缓存，结构比公共 Feed 的碎片缓存简单。
//   - L3（MySQL）：直接在数据库中查询该用户的所有知文。
//
// WHY 不使用碎片缓存：
// "我的 Feed" 的更新频率远低于公共 Feed（只有用户自己修改才会触发），
// 且数据量相对有限，整页缓存的实现更简单、维护成本更低。
//
// 参数：
//   - ctx: context.Context，用于传递请求上下文和控制超时。
//   - userID: uint64，目标用户 ID。
//   - page: int，页码，从 1 开始。
//   - size: int，每页条数，被 clamp 到 [1, 50]。
//
// 返回值：
//   - *FeedPageResponse: 分页结果。
//   - error: 查询失败时的错误。
func (s *KnowPostFeedService) GetMyPublished(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error) {
	safeSize := clamp(size, 1, defaultSafeSize)
	safePage := max(page, 1)
	feedVersion := s.currentMineFeedVersion(ctx, userID)
	key := fmt.Sprintf("feed:mine:%d:%d:%d:%d", userID, safeSize, safePage, feedVersion)

	// L1：freecache
	if val, err := s.l1Mine.Get([]byte(key)); err == nil {
		resp, parseErr := s.parseFeedPage(val)
		if parseErr == nil {
			s.hotKey.Record(key)
			return resp, nil
		}
	}

	// L2：Redis（`我的 feed` 直接缓存整页，结构比碎片缓存更简单）
	cached, err := s.redis.Get(ctx, key).Result()
	if err == nil && cached != "" {
		resp, parseErr := s.parseFeedPage([]byte(cached))
		if parseErr == nil {
			s.l1Mine.Set([]byte(key), []byte(cached), l1MineCacheTTL)
			s.hotKey.Record(key)
			return resp, nil
		}
	}

	// 查询数据库
	offset := (safePage - 1) * safeSize
	rows, err := s.repo.ListMyPublished(ctx, userID, safeSize+1, offset)
	if err != nil {
		return nil, fmt.Errorf("get my published: list: %w", err)
	}

	hasMore := len(rows) > safeSize
	if hasMore {
		rows = rows[:safeSize]
	}

	const includeIsTop = true
	items := s.mapRowsToItems(ctx, rows, &userID, includeIsTop)

	resp := &FeedPageResponse{
		Items:   items,
		Page:    safePage,
		Size:    safeSize,
		HasMore: hasMore,
	}

	// 回填 L2 和 L1
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		return resp, nil
	}
	baseTTL := l2MineTTLBase + rand.Intn(l2MineJitter)
	s.redis.Set(ctx, key, string(jsonBytes), time.Duration(baseTTL)*time.Second)
	s.l1Mine.Set([]byte(key), jsonBytes, baseTTL)
	s.hotKey.Record(key)

	return resp, nil
}
