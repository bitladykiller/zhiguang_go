package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/config"
	"github.com/zhiguang/app/pkg/jsonutil"
)

// feedLayoutVer 定义 Feed 列表缓存的布局版本号。
// 用于缓存键编码，递增版本号可使旧缓存整体失效。
const feedLayoutVer = 1

// maxPublicFeedPage 是公共 Feed 可翻页数上界（防无界 OFFSET 慢查询）。
const maxPublicFeedPage = 1000

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
//
// TTL / 分页上限统一从 cfg.KnowPost.FeedCache 读取（见 feedCacheTTLValues），
// ApplyDefaults 保证缺省值与历史硬编码常量一致。
// feedEngagement 是 Feed 列表对计数模块的最小依赖（消费侧窄接口，理由见 detailEngagement）。
type feedEngagement interface {
	GetCountsBatch(ctx context.Context, entityType string, entityIDs, metrics []string) (map[string]map[string]int32, error)
	BatchIsLiked(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
	BatchIsFaved(ctx context.Context, userID uint64, entityType string, entityIDs []string) (map[string]bool, error)
}

type KnowPostFeedService struct {
	repo     Repo
	redis    *redis.Client
	l1Public *cache.PrefixCache
	l1Mine   *cache.PrefixCache
	hotKey   *cache.HotKeyDetector
	counter  feedEngagement
	logger   *zap.Logger
	cfg      *config.KnowPostFeedCacheConfig
	// homeTimeline 提供关注流的帖子 ID（由扩散模块注入，可为 nil）。
	homeTimeline HomeTimelineReader
}

// FeedCacheInvalidator 暴露知文写操作所需的 feed 缓存失效能力。
type FeedCacheInvalidator interface {
	InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64)
}

// feedCacheParams 是 Feed 缓存运行时参数快照，来自 cfg 或默认值。
type feedCacheParams struct {
	safeSize      int
	l1PublicTTL   int
	idListBase    int
	idListJitter  int
	hasMoreBase   int
	hasMoreJitter int
	itemBase      int
	itemJitter    int
	mineL2Base    int
	mineL2Jitter  int
	l1MineTTL     int
	extendBase    int
	ttlLow        int
	ttlMedium     int
	ttlHigh       int
}

// feedCacheTTLValues 返回 Feed 缓存相关 TTL / 分页参数。
//
// 优先使用 cfg（由 bootstrap 注入 &cfg.KnowPost.FeedCache），
// cfg 为 nil 时回退到与 ApplyDefaults 一致的历史默认值，保证单测零配置可跑。
func (s *KnowPostFeedService) feedCacheTTLValues() feedCacheParams {
	var cfgPtr *config.KnowPostFeedCacheConfig
	if s != nil {
		cfgPtr = s.cfg
	}
	// cfg 为 nil 时走与全局装配同一条默认值路径（零值节 + ApplyDefaults），
	// 默认数字只在 pkg/config 一处维护——此前这里有一张手抄回退表，
	// 与 ApplyDefaults 的一致性只能靠注释约定。
	fc := feedCacheConfig(cfgPtr)
	return feedCacheParams{
		safeSize:      fc.SafeSize,
		l1PublicTTL:   fc.L1TTLSeconds,
		idListBase:    fc.L2IDListTTLBase,
		idListJitter:  fc.L2IDListJitter,
		hasMoreBase:   fc.L2HasMoreTTLBase,
		hasMoreJitter: fc.L2HasMoreJitter,
		itemBase:      fc.L2ItemTTLBase,
		itemJitter:    fc.L2ItemJitter,
		mineL2Base:    fc.L2MineTTLBase,
		mineL2Jitter:  fc.L2MineJitter,
		l1MineTTL:     fc.L1MineTTLSeconds,
		extendBase:    fc.ExtendTTLBase,
		ttlLow:        fc.TTLLow,
		ttlMedium:     fc.TTLMedium,
		ttlHigh:       fc.TTLHigh,
	}
}

// jitterN 返回 [0, n) 的随机偏移；n<=0 时返回 0，避免 rand.Intn(0) panic。
func jitterN(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.Intn(n)
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
//   - cfg: Feed 缓存配置，nil 时使用与 ApplyDefaults 一致的默认 TTL
func NewKnowPostFeedService(
	repo Repo,
	redisClient *redis.Client,
	l1Public *cache.PrefixCache,
	l1Mine *cache.PrefixCache,
	hotKey *cache.HotKeyDetector,
	counter feedEngagement,
	logger *zap.Logger,
	cfg *config.KnowPostFeedCacheConfig,
) *KnowPostFeedService {
	return &KnowPostFeedService{
		repo:     repo,
		redis:    redisClient,
		l1Public: l1Public,
		l1Mine:   l1Mine,
		hotKey:   hotKey,
		counter:  counter,
		logger:   logger,
		cfg:      cfg,
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
	p := s.feedCacheTTLValues()
	safeSize := clamp(size, 1, p.safeSize)
	// page 同样需要上界：offset = (page-1)*size 直接进 SQL 的 OFFSET，
	// 无界的页码等于允许任意大的 offset 扫描（一次恶意翻页就是一条慢查询）。
	// 公共信息流本就无深翻价值，1000 页 × 50 条已远超任何真实浏览深度。
	safePage := clamp(page, 1, maxPublicFeedPage)
	feedVersion := s.currentPublicFeedVersion(ctx)
	localPageKey := publicFeedPageLocalKey(safeSize, safePage, feedVersion)

	// 键里只保留 feedVersion + size + page 三个维度。
	//
	// WHY 移除原先的 hourSlot 维度：
	//
	//	原实现在键里额外编了一个小时槽（Unix 秒 / 3600），注释称其用于
	//	「控制热门时间窗口失效时的影响范围」。但 InvalidateAfterPostMutation 递增的
	//	feedVersion 是**全局**的——任何一篇知文变更都会让全站所有分页缓存一起失效，
	//	失效粒度已经是「全部」，再按小时分槽不可能缩小任何影响范围。
	//	它唯一的实际效果是把同一份流量切碎到更多键上：整点跨越时全部键换名，
	//	命中率白掉一截。两个失效机制不构成组合关系，因此只保留版本号这一个。
	idsKey := publicFeedIDsKey(feedVersion, safeSize, safePage)
	hasMoreKey := idsKey + ":hasMore"

	if resp := s.getPublicFeedL1(ctx, localPageKey, currentUserID); resp != nil {
		return resp, nil
	}
	if resp := s.getPublicFeedL2(ctx, idsKey, hasMoreKey, safePage, safeSize, currentUserID, localPageKey); resp != nil {
		return resp, nil
	}
	return s.getPublicFeedUnderLock(ctx, idsKey, hasMoreKey, localPageKey, safePage, safeSize, currentUserID)
}

// getPublicFeedL1 读取 L1 整页缓存，命中则叠加当前用户状态后返回。
//
// 缓存中存放的始终是「公共视图」（不含 Liked/Faved），用户维度状态在此处即时叠加，
// 原因见 withUserState。
func (s *KnowPostFeedService) getPublicFeedL1(ctx context.Context, localPageKey string, currentUserID *uint64) *FeedPageResponse {
	val, err := s.l1Public.Get([]byte(localPageKey))
	if err != nil {
		return nil
	}
	shared, parseErr := s.parseFeedPage(val)
	if parseErr != nil {
		return nil
	}
	s.recordItemHotKeys(ctx, shared.Items)
	return s.withUserState(ctx, shared, currentUserID)
}

func (s *KnowPostFeedService) getPublicFeedL2(ctx context.Context, idsKey, hasMoreKey string, safePage, safeSize int, currentUserID *uint64, localPageKey string) *FeedPageResponse {
	shared := s.assembleFromCache(ctx, idsKey, hasMoreKey, safePage, safeSize)
	if shared == nil {
		return nil
	}
	s.cacheFeedPage(localPageKey, shared, s.l1Public)
	s.recordItemHotKeys(ctx, shared.Items)
	return s.withUserState(ctx, shared, currentUserID)
}

// withUserState 返回叠加了当前用户点赞/收藏状态的响应副本。
//
// WHY 用户状态一律在缓存之外叠加：
//
//	公共 Feed 的 L1 整页缓存键是 "feed:public:{size}:{page}:v{layout}:{version}"，
//	不含用户 ID——它按设计就是**所有用户共享**的一份数据。
//	一旦把某个用户的 Liked/Faved 写进这份共享缓存，后续命中该键的其他用户
//	会读到别人的点赞收藏状态（跨用户串号）。
//	因此缓存层只保存与用户无关的公共视图，用户维度状态在每次读出后即时叠加。
func (s *KnowPostFeedService) withUserState(ctx context.Context, shared *FeedPageResponse, currentUserID *uint64) *FeedPageResponse {
	items := s.enrichItems(ctx, shared.Items, currentUserID)
	if len(items) == 0 {
		items = []FeedItemResponse{}
	}
	return &FeedPageResponse{
		Items:   items,
		Page:    shared.Page,
		Size:    shared.Size,
		HasMore: shared.HasMore,
	}
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
		func(ctx context.Context) (*FeedPageResponse, bool, error) {
			if shared := s.assembleFromCache(ctx, idsKey, hasMoreKey, page, size); shared != nil {
				s.cacheFeedPage(localPageKey, shared, s.l1Public)
				return s.withUserState(ctx, shared, currentUserID), true, nil
			}
			return nil, false, nil
		},
		func(ctx context.Context) (*FeedPageResponse, error) {
			offset := (page - 1) * size
			rows, err := s.repo.ListFeedPublic(ctx, size+1, offset)
			if err != nil {
				return nil, fmt.Errorf("get public feed: list: %w", err)
			}

			hasMore := len(rows) > size
			if hasMore {
				rows = rows[:size]
			}

			items := s.mapRowsToItems(ctx, rows, currentUserID, false)

			shared := &FeedPageResponse{
				Items:   items,
				Page:    page,
				Size:    size,
				HasMore: hasMore,
			}

			s.writeFragmentCaches(ctx, idsKey, hasMoreKey, size, rows, items, hasMore)
			s.cacheFeedPage(localPageKey, shared, s.l1Public)

			return s.withUserState(ctx, shared, currentUserID), nil
		},
	)
}

// ============================================================================
// 获取我的已发布内容
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
	p := s.feedCacheTTLValues()
	safeSize := clamp(size, 1, p.safeSize)
	safePage := max(page, 1)
	feedVersion := s.currentMineFeedVersion(ctx, userID)
	key := mineFeedPageKey(userID, safeSize, safePage, feedVersion)

	// 「我的已发布」是整页缓存（更新频率低、数据量有限，碎片化不划算），
	// L1→L2→锁内回源的编排复用 cache.Tiered；本方法只提供键、TTL 与加载器。
	//
	// 与旧实现的两个行为差异（均为改进）：
	//  1. 回源加了分布式锁 + double-check——此前该路径无击穿保护；
	//  2. 返回前统一叠加当前用户的 liked/faved——此前三个列表接口只有部分叠加，
	//     同一字段是否出现取决于走了哪条代码路径。
	tiered := &cache.Tiered[*FeedPageResponse]{
		L1:           s.l1Mine,
		Redis:        s.redis,
		Logger:       s.logger,
		Encode:       func(v *FeedPageResponse) ([]byte, error) { return json.Marshal(v) },
		Decode:       parseJSON[*FeedPageResponse],
		L1TTLSeconds: p.l1MineTTL,
		L2TTL: func() time.Duration {
			return time.Duration(p.mineL2Base+jitterN(p.mineL2Jitter)) * time.Second
		},
		LockKey:     lockKeyFor(key),
		LockOptions: knowPostLockOptions(),
		LockRetry:   knowPostLockRetryInterval,
	}

	shared, _, err := tiered.Get(ctx, key, func(ctx context.Context) (*FeedPageResponse, bool, error) {
		offset := (safePage - 1) * safeSize
		rows, dbErr := s.repo.ListMyPublished(ctx, userID, safeSize+1, offset)
		if dbErr != nil {
			return nil, false, fmt.Errorf("get my published: list: %w", dbErr)
		}
		hasMore := len(rows) > safeSize
		if hasMore {
			rows = rows[:safeSize]
		}
		items := s.mapRowsToItems(ctx, rows, &userID, true)
		return &FeedPageResponse{Items: items, Page: safePage, Size: safeSize, HasMore: hasMore}, true, nil
	})
	if err != nil {
		return nil, err
	}

	if s.hotKey != nil {
		s.hotKey.Record(key)
	}
	return s.withUserState(ctx, shared, &userID), nil
}

// ============================================================================
// 碎片缓存组装
// ============================================================================

// assembleFromCache 尝试从 Redis 的碎片缓存中还原一整页 feed。
//
// 功能：碎片缓存由三部分组成，此方法逐一提取并组装：
//  1. ID 列表：使用 Redis LRange 从 List 结构中按范围读取 size 个文档 ID。
//     LRange（key, start, stop）返回从 start 到 stop 范围内的所有元素。复杂度 O(S+N)，
//     其中 S 是偏移量距离。
//  2. 条目详情：使用 Redis MGet 批量读取 FeedItemResponse 的 JSON 字符串。
//     MGet（key1, key2, ...）在单次网络往返中返回多个 key 的值，复杂度 O(N)。
//     如果任意 key 不存在（返回 nil），则判定为缓存未命中，返回 nil。
//  3. hasMore 软缓存：从 Redis 读取该页的 hasMore 标记。
//     标记不存在时使用 fallback 逻辑：如果本页条数 == size 则假定有更多页。
//
// 为什么使用"碎片缓存"而非整页缓存？
//
//	碎片缓存方案中，一篇知文更新只需要失效它的 Item 碎片（而不是包含它的所有页码），
//	再递增 feed version 让旧版本整页缓存整体过期，失效范围远小于整页缓存。
//
// 为什么使用"任意碎片缺失即视为未命中"的策略？
//
//	如果一个页面的 ID 列表比实际条目多，但某一条目的缓存已过期，
//	拼装出的列表会漏掉该条目。为了确保结果正确性，任一碎片缺失即回源数据库重建。
//	此策略的代价是偶尔的缓存命中率波动，但保证了数据完整性。
//
// 参数：
//   - ctx: context.Context。
//   - idsKey: string，Redis List 键名，存储本页的帖子 ID 列表。
//   - hasMoreKey: string，Redis 键名，存储 hasMore 标记（"1" 或 "0"）。
//   - page: int，当前页码，用于构造响应。
//   - size: int，每页条数。
//
// 返回值：
//   - *FeedPageResponse: 若缓存完整命中则返回组装出的**公共视图**（不含用户维度状态，
//     由调用方通过 withUserState 叠加）；若任意碎片缺失则返回 nil。
func (s *KnowPostFeedService) assembleFromCache(ctx context.Context, idsKey, hasMoreKey string, page, size int) *FeedPageResponse {
	// 读取 ID 列表
	idStrs, err := s.redis.LRange(ctx, idsKey, 0, int64(size-1)).Result()
	if err != nil || len(idStrs) == 0 {
		return nil
	}

	// 批量读取条目碎片
	itemKeys := make([]string, len(idStrs))
	for i, idStr := range idStrs {
		itemKeys[i] = feedItemKey(idStr)
	}
	itemJSONs, err := s.redis.MGet(ctx, itemKeys...).Result()
	if err != nil {
		s.logger.Warn("failed to MGet feed item cache entries", zap.Strings("itemKeys", itemKeys), zap.Error(err))
		return nil
	}

	// 解析条目内容
	items := make([]FeedItemResponse, 0, len(idStrs))
	for _, itemJSON := range itemJSONs {
		if itemJSON == nil {
			return nil // 任意碎片缺失则视为缓存未命中
		}
		itemStr, ok := itemJSON.(string)
		if !ok {
			return nil
		}
		var item FeedItemResponse
		if err := json.Unmarshal([]byte(itemStr), &item); err != nil {
			return nil
		}
		items = append(items, item)
	}

	// 读取 hasMore 软缓存；标记缺失时降级为「满页即可能还有更多」
	hasMore := len(items) == size
	if hasMoreStr, getErr := s.redis.Get(ctx, hasMoreKey).Result(); getErr == nil {
		hasMore = hasMoreStr == "1"
	}

	return &FeedPageResponse{
		Items:   items,
		Page:    page,
		Size:    size,
		HasMore: hasMore,
	}
}

// writeFragmentCaches 把 ID 列表、条目碎片和 hasMore 软缓存写入 Redis。
//
// 功能：在回源数据库查询成功后，将结果写入 Redis 碎片缓存供后续请求使用。
//
// 写入的内容包括：
//  1. ID 列表：使用 LPush 将帖子 ID（字符串格式）推入 Redis List。
//     LPush 将新元素插入到 List 的头部。复杂度 O(1)。
//     TTL：60-90 秒（带 jitter，避免同时过期）。
//  2. hasMore 软缓存：使用 Set 写入 "1" 或 "0"，TTL：10-20 秒（短 TTL，
//     因为它只是辅助标记，过期后 fallback 逻辑也可正常工作）。
//  3. 条目碎片：对每个 FeedItemResponse 使用 Set 写入单独的键
//     "feed:item:{id}"，TTL：60-90 秒。
//
// WHY 使用 LPush 而非 RPush：
// 为了与 List 的 LRange 读取配合，LPush + LRange(0, N-1) 可以读取最新写入的 N 个元素。
// 在碎片缓存场景中，写入时保证 ID 的顺序与查询结果的顺序一致。
//
// 参数：
//   - ctx: context.Context。
//   - idsKey: string，Redis List 键名。
//   - hasMoreKey: string，hasMore 标记键名。
//   - size: int，每页条数（用于构造响应）。
//   - rows: []KnowPostFeedRow，数据库查询的原始行记录。
//   - items: []FeedItemResponse，转换后的条目列表。
//   - hasMore: bool，是否有下一页。
func (s *KnowPostFeedService) writeFragmentCaches(ctx context.Context, idsKey, hasMoreKey string, size int, rows []KnowPostFeedRow, items []FeedItemResponse, hasMore bool) {
	s.writeFeedIDListCache(ctx, idsKey, hasMoreKey, rows, hasMore)
	s.writeFeedItemCaches(ctx, items)
}

func (s *KnowPostFeedService) writeFeedIDListCache(ctx context.Context, idsKey, hasMoreKey string, rows []KnowPostFeedRow, hasMore bool) {
	p := s.feedCacheTTLValues()
	idVals := make([]interface{}, len(rows))
	for i, r := range rows {
		idVals[i] = strconv.FormatUint(r.ID, 10)
	}
	if len(idVals) == 0 {
		return
	}
	if err := s.redis.RPush(ctx, idsKey, idVals...).Err(); err != nil {
		s.logger.Warn("failed to RPush feed IDs", zap.String("idsKey", idsKey), zap.Error(err))
	}
	ttl := time.Duration(p.idListBase+jitterN(p.idListJitter)) * time.Second
	if err := s.redis.Expire(ctx, idsKey, ttl).Err(); err != nil {
		s.logger.Warn("failed to set expire on feed IDs", zap.String("idsKey", idsKey), zap.Error(err))
	}
	hasMoreTTL := time.Duration(p.hasMoreBase+jitterN(p.hasMoreJitter)) * time.Second
	if err := s.redis.Set(ctx, hasMoreKey, boolToStr(hasMore), hasMoreTTL).Err(); err != nil {
		s.logger.Warn("failed to set hasMore cache", zap.String("hasMoreKey", hasMoreKey), zap.Error(err))
	}
}

func (s *KnowPostFeedService) writeFeedItemCaches(ctx context.Context, items []FeedItemResponse) {
	p := s.feedCacheTTLValues()
	pipe := s.redis.Pipeline()
	for _, item := range items {
		itemKey := feedItemKey(item.ID)
		jsonBytes, err := json.Marshal(item)
		if err != nil {
			s.logger.Warn("failed to marshal feed item for cache", zap.String("itemID", item.ID), zap.Error(err))
			continue
		}
		ttl := time.Duration(p.itemBase+jitterN(p.itemJitter)) * time.Second
		pipe.Set(ctx, itemKey, string(jsonBytes), ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Warn("failed to pipeline feed item cache entries", zap.Error(err))
	}
}

// InvalidateAfterPostMutation 在知文发生变更后失效相关 feed 缓存。
//
// 功能：当知文被创建、更新或删除时调用，使 feed 缓存不过期即可反映最新状态。
// 具体操作：
//  1. 删除 Redis 中该条目的碎片缓存（"feed:item:{postID}"）。
//  2. 递增公共 Feed 的版本号（"feed:public:version"）。
//  3. 递增该用户"我的 Feed"的版本号（"feed:mine:version:{creatorID}"）。
//
// 原理：递增版本号会使所有带旧版本号的缓存 key 自然失效。
// 因为所有缓存 key 都编码了当前的 feedVersion（如 "feed:public:10:1:v1:3"，
// 其中 3 就是版本号）。旧版本的整页缓存不会被读取，从而实现批量失效。
//
// 这种"版本号递增"策略的优缺点：
//   - 优点：O(1) 复杂度，不管有多少页缓存，一次 Incr 即可让所有旧缓存失效。
//   - 缺点：在并发写高频场景下，版本号会快速递增，缓存命中率下降。
//     对于知文这种写操作远少于读操作的场景，此策略是非常合适的。
//
// WHY 同时递增两个版本号：
// 公共 Feed 是所有用户共享的视图，递增后所有用户都会看到更新。
// "我的 Feed" 只属于创作者本人，递增后只会影响该用户看到的"我的"列表。
//
// 参数：
//   - ctx: context.Context。
//   - postID: uint64，发生变更的知文 ID。
//   - creatorID: uint64，知文作者 ID。
//
// 实现细节：
//   - s.redis.Del 删除单条碎片缓存，复杂度 O(N) 其中 N 是 key 的数量（这里只有 1 个 key），
//     所以是 O(1)。
//   - s.redis.Incr 递增版本号，复杂度 O(1)。
func (s *KnowPostFeedService) InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64) {
	if s.redis == nil || s.logger == nil {
		return
	}
	itemKey := feedItemKeyU(postID)
	mineKey := mineFeedVersionKey(creatorID)

	if err := invalidateFeedScript.Run(ctx, s.redis, []string{itemKey, publicFeedVersionKey, mineKey}).Err(); err != nil {
		s.logger.Warn("failed to invalidate feed caches",
			zap.Uint64("postID", postID),
			zap.Uint64("creatorID", creatorID),
			zap.Error(err),
		)
	}

	// 版本号已在 Redis 递增，同步作废本实例的版本号短缓存，保证自写自读立即切换新键。
	v := s.feedVersions()
	v.Drop(publicFeedVersionKey)
	v.Drop(mineKey)
}

// ============================================================================
// 条目映射与增强
// ============================================================================

// mapRowsToItems 将数据库查询结果（KnowPostFeedRow 切片）转换为 FeedItemResponse 切片。
//
// 功能：数据库行到 Feed 条目响应模型的转换器。在转换过程中：
//   - 解析 JSON 字符串字段（tags、img_urls）为 Go 切片。
//   - 取第一张图片作为封面图（CoverImage）。
//   - 若 counter 不为 nil，查询并填充点赞数和收藏数。
//   - 如果是"我的 Feed"查询，则包含置顶标记。
//
// 参数：
//   - ctx: context.Context。
//   - rows: []KnowPostFeedRow，数据库查询结果。
//   - userID: *uint64，当前用户 ID（用于区分公共 Feed 和"我的 Feed"）。
//   - includeIsTop: bool，是否包含置顶标记（公共 Feed 为 false，"我的 Feed" 为 true）。
//
// 返回值：[]FeedItemResponse，转换后的条目列表，长度与 rows 相同。
func (s *KnowPostFeedService) mapRowsToItems(ctx context.Context, rows []KnowPostFeedRow, userID *uint64, includeIsTop bool) []FeedItemResponse {
	items := make([]FeedItemResponse, len(rows))

	// 批量获取计数信息
	var countsBatch map[string]map[string]int32
	entityIDs := make([]string, len(rows))
	if s.counter != nil && len(rows) > 0 {
		for i, r := range rows {
			entityIDs[i] = strconv.FormatUint(r.ID, 10)
		}
		var err error
		countsBatch, err = s.counter.GetCountsBatch(ctx, "knowpost", entityIDs, []string{"like", "fav"})
		if err != nil {
			s.logger.Warn("failed to batch get counts for feed items", zap.Error(err))
			countsBatch = nil
		}
	}

	for i, r := range rows {
		tags := jsonutil.ParseStringArray(r.Tags)
		imgs := jsonutil.ParseStringArray(r.ImgUrls)
		var cover *string
		if len(imgs) > 0 {
			cover = &imgs[0]
		}

		eid := entityIDs[i]
		item := FeedItemResponse{
			ID:             eid,
			Title:          r.Title,
			Description:    r.Description,
			CoverImage:     cover,
			Tags:           tags,
			AuthorAvatar:   r.AuthorAvatar,
			AuthorNickname: r.AuthorNickname,
			TagJSON:        r.AuthorTagJSON,
		}

		if countsBatch != nil {
			if c, ok := countsBatch[eid]; ok {
				item.LikeCount = int64(c["like"])
				item.FavoriteCount = int64(c["fav"])
			}
		}

		if includeIsTop {
			isTop := r.IsTop
			item.IsTop = &isTop
		}

		items[i] = item
	}
	return items
}

// enrichItems 为 feed 条目叠加当前用户的点赞/收藏状态。
//
// 功能：对每个 FeedItemResponse，查询当前用户是否对该知文点过赞和收藏。
// 这些状态是用户维度的，不会进入缓存（不同用户看到的结果不同）。
//
// 边界情况：
//   - userID 为 nil（未登录）或 counter 为 nil：不做任何查询，直接返回原 items。
//   - IsLiked/IsFaved 查询失败：静默忽略，不阻塞 feed 加载。
//
// 参数：
//   - ctx: context.Context。
//   - items: []FeedItemResponse，需要增强的条目列表。
//   - userID: *uint64，当前用户的 ID（可选）。
//
// 返回值：[]FeedItemResponse，增强了 Liked 和 Faved 字段的新切片。
// 注意：返回的是新切片（enriched），调用方应使用返回值而非原 items。
func (s *KnowPostFeedService) enrichItems(ctx context.Context, items []FeedItemResponse, userID *uint64) []FeedItemResponse {
	if userID == nil || s.counter == nil {
		return items
	}

	itemIDs := make([]string, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID
	}

	likedMap, err := s.counter.BatchIsLiked(ctx, *userID, "knowpost", itemIDs)
	if err != nil {
		s.logWarn("feed: batch is liked failed", err)
	}
	favedMap, favErr := s.counter.BatchIsFaved(ctx, *userID, "knowpost", itemIDs)
	if favErr != nil {
		s.logWarn("feed: batch is faved failed", favErr)
	}

	enriched := make([]FeedItemResponse, len(items))
	for i, item := range items {
		if likedMap != nil {
			if l, ok := likedMap[item.ID]; ok {
				item.Liked = &l
			}
		}
		if favedMap != nil {
			if f, ok := favedMap[item.ID]; ok {
				item.Faved = &f
			}
		}
		enriched[i] = item
	}
	return enriched
}

// batchExtendFeedItemTTLScript 在一次调用内为多个碎片缓存做「只增不减」的 TTL 延长。
//
//	KEYS[i] = 待延长的 feed:item 键
//	ARGV[i] = 该键的目标 TTL（秒），与 KEYS[i] 一一对应
//
// 语义与 extendTTLDualScript 一致：仅当键存在且当前 TTL 小于目标值时才 EXPIRE，
// 因此多实例并发延长同一键不会互相把 TTL 改短。
var batchExtendFeedItemTTLScript = redis.NewScript(`
for i = 1, #KEYS do
  local target = tonumber(ARGV[i])
  local current = redis.call('TTL', KEYS[i])
  if current > 0 and current < target then
    redis.call('EXPIRE', KEYS[i], target)
  end
end
return 1
`)

// recordItemHotKeys 记录整页 feed 条目的访问热度，并为其中的热点条目延长碎片缓存 TTL。
//
// WHY 整页批量而非逐条处理：
//
//	逐条处理时，每个条目都要先查一次热度（EXISTS）再执行一次 TTL 延长（EVAL），
//	即每条 2 次串行 Redis 往返。一页 50 条就是上百次往返——
//	而这段逻辑恰恰挂在 L1 命中路径上，本该是几十纳秒的内存读取被拖成几十毫秒的网络等待，
//	L1 缓存的意义被完全抵消。
//	批量化后：热度查询合并为至多 1 次 MGET（本地等级缓存命中时为 0 次），
//	TTL 延长合并为至多 1 次 EVAL，且整页无热点时直接跳过，不产生任何 Redis 往返。
func (s *KnowPostFeedService) recordItemHotKeys(ctx context.Context, items []FeedItemResponse) {
	if s.hotKey == nil || len(items) == 0 {
		return
	}

	hotKeyIDs := make([]string, len(items))
	for i, item := range items {
		hotKeyIDs[i] = "knowpost:" + item.ID
		s.hotKey.Record(hotKeyIDs[i]) // 纯本地计数，无 Redis IO
	}

	if s.redis == nil {
		return
	}

	baseTTL := s.feedCacheTTLValues().extendBase
	targets := s.hotKey.TTLForPublicBatch(ctx, baseTTL, hotKeyIDs)

	itemKeys := make([]string, 0, len(items))
	itemTTLs := make([]any, 0, len(items))
	for i, target := range targets {
		if target <= baseTTL {
			continue // 冷键：目标 TTL 不高于基准值，延长是空操作，不值得占一次往返
		}
		itemKeys = append(itemKeys, feedItemKey(items[i].ID))
		itemTTLs = append(itemTTLs, target)
	}
	if len(itemKeys) == 0 {
		return
	}

	if err := batchExtendFeedItemTTLScript.Run(ctx, s.redis, itemKeys, itemTTLs...).Err(); err != nil {
		s.logWarn("failed to extend hot feed item TTLs", err)
	}
}

// logWarn 在 logger 未注入时静默降级，避免零值 service（单测/装配中途）触发空指针。
func (s *KnowPostFeedService) logWarn(msg string, err error) {
	if s.logger == nil {
		return
	}
	s.logger.Warn(msg, zap.Error(err))
}

// ============================================================================
// 辅助函数
// ============================================================================

// cacheFeedPage 将整页的 Feed 响应写入 freecache（L1 进程级缓存）。
//
// 功能：把序列化后的 FeedPageResponse 写入 L1 缓存，供后续请求快速命中。
// TTL 固定为 15 秒，因为 L1 是最快的缓存层，但副本数受限于进程内存，
// 不需要太长的 TTL——即使 L1 过期，还有 L2 碎片缓存和 L3 MySQL。
//
// freecache.Set 的参数：
//   - key: []byte，缓存键。
//   - value: []byte，序列化后的 JSON 数据。
//   - expireSeconds: int，过期秒数。
//
// freecache 的注意事项：
//   - 当缓存满了会自动淘汰最旧的条目（LRU 近似淘汰机制）。
//   - 这是进程级缓存，重启后丢失，因此 TTL 不需要太长。
//
// 参数：
//   - key: string，缓存键名。
//   - resp: *FeedPageResponse，需要缓存的整页响应。
//   - cache: *freecache.Cache，目标缓存实例（公共 Feed 使用 l1Public，"我的 Feed" 使用 l1Mine）。
func (s *KnowPostFeedService) cacheFeedPage(key string, resp *FeedPageResponse, cache *cache.PrefixCache) {
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Warn("failed to marshal feed page for cache", zap.String("key", key), zap.Error(err))
		return
	}
	p := s.feedCacheTTLValues()
	cache.SetOrWarn(s.logger, []byte(key), jsonBytes, p.l1PublicTTL)
}

// parseFeedPage 将 feed 页的 JSON 缓存数据反序列化为 FeedPageResponse。
func (s *KnowPostFeedService) parseFeedPage(data []byte) (*FeedPageResponse, error) {
	return parseJSON[*FeedPageResponse](data)
}

// clamp 将一个整数值限制在 [lo, hi] 范围内，用于约束分页 size。
//
// 直接复用 Go 1.21 起的内建 min/max。
// 此前本文件还自定义了一个 `func max(a, b int) int`，它会在整个包内遮蔽同名内建函数——
// 读者需要先确认包里有没有同名定义才能判断一处 max 调用的语义，这类遮蔽应当避免。
func clamp(v, lo, hi int) int {
	return min(max(v, lo), hi)
}

// boolToStr 将布尔值转换为 Redis 易于存储的字符串 "1" 或 "0"。
//
// 功能：Redis 的字符串值不能直接存储 Go 的 bool 类型，
// 此函数将 true 映射为 "1"、false 映射为 "0"。
// 读取时通过检查字符串是否等于 "1" 来还原布尔值。
//
// 参数：
//   - b: bool，输入的布尔值。
//
// 返回值：string，"1"（true）或 "0"（false）。
func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

var invalidateFeedScript = redis.NewScript(`
local itemKey = KEYS[1]
local publicVerKey = KEYS[2]
local mineVerKey = KEYS[3]

redis.call('DEL', itemKey)
redis.call('INCR', publicVerKey)
redis.call('INCR', mineVerKey)

return 1
`)

// currentPublicFeedVersion 返回公共 Feed 的当前版本号。
//
// 功能：从 Redis 读取 "feed:public:version" 键的值。
// 若该键不存在或值 <= 0，返回默认版本号 1。
// 每次有任意知文发生变更（发布、编辑、删除等）时，此版本号会递增。
func (s *KnowPostFeedService) currentPublicFeedVersion(ctx context.Context) int64 {
	return s.feedVersion(ctx, publicFeedVersionKey)
}

// currentMineFeedVersion 返回指定用户的"我的 Feed"当前版本号。
//
// 功能：从 Redis 读取 "feed:mine:version:{userID}" 键的值。
// 每次该用户自己的知文发生变更时，此版本号会递增。
//
// 参数：
//   - ctx: context.Context。
//   - userID: uint64，用户 ID。
//
// 返回值：int64，当前版本号。若不存在或无效则返回 1。
func (s *KnowPostFeedService) currentMineFeedVersion(ctx context.Context, userID uint64) int64 {
	return s.feedVersion(ctx, mineFeedVersionKey(userID))
}

// feedVersion 通用的 Feed 版本号读取函数。
//
// 功能：从 Redis 读取指定 key 的整数值作为版本号。
// Redis GET 返回字符串，通过 Int64() 解析为 int64。
// 若 key 不存在、值不是合法整数或值 <= 0，返回 1（默认版本）。
//
// 参数：
//   - ctx: context.Context。
//   - key: string，Redis 键名，如 "feed:public:version" 或 "feed:mine:version:{userID}"。
//
// 返回值：int64，当前版本号。默认返回 1。
func (s *KnowPostFeedService) feedVersion(ctx context.Context, key string) int64 {
	return s.feedVersions().Get(ctx, key)
}

// feedVersions 返回 Feed 版本号读取器（带进程内短缓存，无状态可按调用构造）。
//
// 公共 Feed 的 L1 整页键编入了版本号：没有本地短缓存时，
// 每次 L1 命中都要先付一次 Redis GET 才能拼出键——与详情链路当年同款缺陷。
func (s *KnowPostFeedService) feedVersions() *cache.Versions {
	return newFeedVersions(s.redis, s.l1Public)
}

// ============================================================================
// 关注流（首页信息流）
// ============================================================================

// HomeTimelineReader 抽象关注流的帖子 ID 来源，由 fanout 模块实现。
//
// 契约是「一页有序 postID + 下一页游标」：knowpost 不需要知道这些 ID
// 是推来的还是拉来的，也不解释游标内容（不透明回传），扩散策略的演进不波及本模块。
type HomeTimelineReader interface {
	HomeTimelinePage(ctx context.Context, userID uint64, cursor string, limit int) (ids []uint64, nextCursor string, hasMore bool, err error)
}

// SetHomeTimelineReader 注入关注流 ID 来源。
//
// 采用装配后回注而非构造参数：扩散模块需要 relation 服务，而 relation 的装配
// 晚于 knowpost，构造期无法拿到。未注入时 GetHomeFeed 返回空列表而非报错。
func (s *KnowPostFeedService) SetHomeTimelineReader(r HomeTimelineReader) {
	s.homeTimeline = r
}

// GetHomeFeed 返回当前用户的**关注流**：所关注作者发布的知文，按发布时间倒序。
//
// 三个列表接口语义互不相同：public（全站）/ home（关注流，本方法）/ mine（我的已发布）。
//
// 分页是**游标制**（cursor 不透明回传，空串为第一页）：
// 信息流是动态列表，offset 分页在翻页期间有新帖插入时会跳条或重条；
// 游标以 (发布时间, postID) 双键定位，翻页不重不漏。
//
// 可见性：关注流按定义只包含「我关注的作者」，因此 followers-only 内容
// **应当**对我可见——批量查询使用 public+followers 双档过滤
// （private/unlisted 仍被排除）。公共 Feed 则维持仅 public。
func (s *KnowPostFeedService) GetHomeFeed(ctx context.Context, userID uint64, cursor string, size int) (*HomeFeedResponse, error) {
	p := s.feedCacheTTLValues()
	safeSize := clamp(size, 1, p.safeSize)

	empty := &HomeFeedResponse{Items: []FeedItemResponse{}, HasMore: false}
	if s.homeTimeline == nil {
		return empty, nil
	}

	ids, next, hasMore, err := s.homeTimeline.HomeTimelinePage(ctx, userID, cursor, safeSize)
	if err != nil {
		return nil, fmt.Errorf("get home feed: read timeline: %w", err)
	}
	if len(ids) == 0 {
		return empty, nil
	}

	rows, err := s.repo.FindFeedRowsByIDs(ctx, ids, []KnowPostVisibility{KnowPostVisibilityPublic, KnowPostVisibilityFollowers})
	if err != nil {
		return nil, fmt.Errorf("get home feed: find by ids: %w", err)
	}

	// 时间线顺序是权威顺序，按 ids 重排（FindFeedRowsByIDs 的 SQL 排序与其无关）。
	rows = reorderRowsByIDs(rows, ids)

	items := s.mapRowsToItems(ctx, rows, &userID, false)
	shared := &FeedPageResponse{Items: items, Page: 1, Size: safeSize, HasMore: hasMore}
	enriched := s.withUserState(ctx, shared, &userID)

	return &HomeFeedResponse{Items: enriched.Items, NextCursor: next, HasMore: hasMore}, nil
}

// reorderRowsByIDs 按给定的 ID 顺序重排数据库行，丢弃不在 ids 中的行。
func reorderRowsByIDs(rows []KnowPostFeedRow, ids []uint64) []KnowPostFeedRow {
	byID := make(map[uint64]KnowPostFeedRow, len(rows))
	for _, r := range rows {
		byID[r.ID] = r
	}
	out := make([]KnowPostFeedRow, 0, len(ids))
	for _, id := range ids {
		if r, ok := byID[id]; ok {
			out = append(out, r)
		}
	}
	return out
}
