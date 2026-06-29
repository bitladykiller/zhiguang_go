package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/zhiguang/app/internal/cache"
	"github.com/zhiguang/app/pkg/jsonutil"
)

// feedLayoutVer 定义 Feed 列表缓存的布局版本号。
// 用于缓存键编码，递增版本号可使旧缓存整体失效。
const feedLayoutVer = 1

const (
	publicFeedVersionKey = "feed:public:version"
	mineFeedVersionKey   = "feed:mine:version:%d"
)

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
	ttlLowFeed       = 30
	ttlMediumFeed    = 60
	ttlHighFeed      = 300
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
	repo     Repo
	redis    *redis.Client
	l1Public *PrefixCache
	l1Mine   *PrefixCache
	hotKey   *cache.HotKeyDetector
	counter  CounterClient
	sf       singleflight.Group
	logger   *zap.Logger
}

// FeedCacheInvalidator 暴露知文写操作所需的 feed 缓存失效能力。
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
	repo Repo,
	redisClient *redis.Client,
	l1Public *PrefixCache,
	l1Mine *PrefixCache,
	hotKey *cache.HotKeyDetector,
	counter CounterClient,
	logger *zap.Logger,
) *KnowPostFeedService {
	return &KnowPostFeedService{
		repo:     repo,
		redis:    redisClient,
		l1Public: l1Public,
		l1Mine:   l1Mine,
		hotKey:   hotKey,
		counter:  counter,
		logger:   logger,
	}
}

// idsPool 为 GetMineFeed 中的临时 []uint64 切片提供复用。
var idsPool = sync.Pool{
	New: func() any {
		ids := make([]uint64, 0, 50)
		return &ids
	},
}

// itemKeysPool 为 assembleFromCache 中的临时 []string 切片提供复用。
var itemKeysPool = sync.Pool{
	New: func() any {
		keys := make([]string, 0, 50)
		return &keys
	},
}

// itemIDsPool 为 enrichItems 中的临时 []string 切片提供复用。
var itemIDsPool = sync.Pool{
	New: func() any {
		ids := make([]string, 0, 50)
		return &ids
	},
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

	if resp := s.getPublicFeedL1(ctx, localPageKey, safePage, safeSize, currentUserID); resp != nil {
		return resp, nil
	}
	if resp := s.getPublicFeedL2(ctx, idsKey, hasMoreKey, safePage, safeSize, currentUserID, localPageKey); resp != nil {
		return resp, nil
	}
	return s.getPublicFeedUnderLock(ctx, idsKey, hasMoreKey, localPageKey, safePage, safeSize, currentUserID)
}

func (s *KnowPostFeedService) getPublicFeedL1(ctx context.Context, localPageKey string, safePage, safeSize int, currentUserID *uint64) *FeedPageResponse {
	val, err := s.l1Public.Get([]byte(localPageKey))
	if err != nil {
		return nil
	}
	resp, parseErr := s.parseFeedPage(val)
	if parseErr != nil {
		return nil
	}
	for _, item := range resp.Items {
		s.recordItemHotKey(ctx, item.ID)
	}
	return &FeedPageResponse{
		Items:   resp.Items,
		Page:    resp.Page,
		Size:    resp.Size,
		HasMore: resp.HasMore,
	}
}

func (s *KnowPostFeedService) getPublicFeedL2(ctx context.Context, idsKey, hasMoreKey string, safePage, safeSize int, currentUserID *uint64, localPageKey string) *FeedPageResponse {
	resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, safePage, safeSize, currentUserID)
	if resp == nil {
		return nil
	}
	s.cacheFeedPage(localPageKey, resp, s.l1Public)
	for _, item := range resp.Items {
		s.recordItemHotKey(ctx, item.ID)
	}
	return resp
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
			if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, page, size, currentUserID); resp != nil {
				s.cacheFeedPage(localPageKey, resp, s.l1Public)
				return resp, true, nil
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

			resp := &FeedPageResponse{
				Items:   items,
				Page:    page,
				Size:    size,
				HasMore: hasMore,
			}

			s.writeFragmentCaches(ctx, idsKey, hasMoreKey, size, rows, items, hasMore)
			s.cacheFeedPage(localPageKey, resp, s.l1Public)

			enriched := s.enrichItems(ctx, items, currentUserID)
			if len(enriched) == 0 {
				enriched = []FeedItemResponse{}
			}
			return &FeedPageResponse{
				Items:   enriched,
				Page:    page,
				Size:    size,
				HasMore: hasMore,
			}, nil
		},
	)
}

// ============================================================================
// 获取我的已发布内容
// ============================================================================

// GetMineFeed 返回当前用户的 Feed 时间线（写扩散优先，降级到读扩散）。
//
// 读取路径：
//  1. 先尝试从 timeline:{user_id} ZSet 读取 post_id 列表（写扩散路径）
//  2. 如果 ZSet 有数据，按 post_id 批量查 know_posts 详情
//  3. 如果 ZSet 为空或只有部分数据，降级到原来的读扩散路径
func (s *KnowPostFeedService) GetMineFeed(ctx context.Context, userID uint64, page, size int) (*FeedPageResponse, error) {
	safeSize := clamp(size, 1, defaultSafeSize)
	safePage := max(page, 1)
	offset := (safePage - 1) * safeSize

	timelineKey := fmt.Sprintf("timeline:%d", userID)
	memberIDs, err := s.redis.ZRevRange(ctx, timelineKey, int64(offset), int64(offset+safeSize-1)).Result()
	if err == nil && len(memberIDs) > 0 {
		idsPtr := idsPool.Get().(*[]uint64)
		ids := *idsPtr
		ids = ids[:0]
		for _, idStr := range memberIDs {
			if id, parseErr := strconv.ParseUint(idStr, 10, 64); parseErr == nil {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			rows, dbErr := s.repo.FindByIDs(ctx, ids)
			idsPtr = &ids
			idsPool.Put(idsPtr)
			if dbErr == nil && len(rows) > 0 {
				items := s.mapRowsToItems(ctx, rows, &userID, true)
				enriched := s.enrichItems(ctx, items, &userID)

				total, totalErr := s.redis.ZCard(ctx, timelineKey).Result()
				hasMore := false
				if totalErr == nil {
					hasMore = int(offset+safeSize) < int(total)
				} else {
					hasMore = len(rows) >= safeSize
				}

				return &FeedPageResponse{
					Items:   enriched,
					Page:    safePage,
					Size:    safeSize,
					HasMore: hasMore,
				}, nil
			}
		} else {
			idsPtr = &ids
			idsPool.Put(idsPtr)
		}
	}

	return s.GetMyPublished(ctx, userID, page, size)
}

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

	items := s.mapRowsToItems(ctx, rows, &userID, true)

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
	if setErr := s.redis.Set(ctx, key, string(jsonBytes), time.Duration(baseTTL)*time.Second).Err(); setErr != nil {
		s.logger.Warn("failed to set mine feed L2 cache", zap.String("key", key), zap.Error(setErr))
	}
	s.l1Mine.Set([]byte(key), jsonBytes, baseTTL)
	s.hotKey.Record(key)

	return resp, nil
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
//   - currentUserID: *uint64，当前用户（可选），用于 enrichItems 叠加点赞/收藏状态。
//
// 返回值：
//   - *FeedPageResponse: 若缓存完整命中则返回已组装的响应；若任意碎片缺失则返回 nil。
func (s *KnowPostFeedService) assembleFromCache(ctx context.Context, idsKey, hasMoreKey string, page, size int, currentUserID *uint64) *FeedPageResponse {
	// 读取 ID 列表
	idStrs, err := s.redis.LRange(ctx, idsKey, 0, int64(size-1)).Result()
	if err != nil || len(idStrs) == 0 {
		return nil
	}

	// 批量读取条目碎片
	itemKeysPtr := itemKeysPool.Get().(*[]string)
	itemKeys := *itemKeysPtr
	itemKeys = itemKeys[:0]
	for _, idStr := range idStrs {
		itemKeys = append(itemKeys, "feed:item:"+idStr)
	}
	itemJsons, err := s.redis.MGet(ctx, itemKeys...).Result()
	if err != nil {
		s.logger.Warn("failed to MGet feed item cache entries", zap.Strings("itemKeys", itemKeys), zap.Error(err))
		itemKeysPtr = &itemKeys
		itemKeysPool.Put(itemKeysPtr)
		return nil
	}

	// 解析条目内容
	items := make([]FeedItemResponse, 0, len(idStrs))
	for _, itemJson := range itemJsons {
		if itemJson == nil {
			itemKeysPtr = &itemKeys
			itemKeysPool.Put(itemKeysPtr)
			return nil // 任意碎片缺失则视为缓存未命中
		}
		itemStr, ok := itemJson.(string)
		if !ok {
			itemKeysPtr = &itemKeys
			itemKeysPool.Put(itemKeysPtr)
			return nil
		}
		var item FeedItemResponse
		if err := json.Unmarshal([]byte(itemStr), &item); err != nil {
			itemKeysPtr = &itemKeys
			itemKeysPool.Put(itemKeysPtr)
			return nil
		}
		items = append(items, item)
	}
	itemKeysPtr = &itemKeys
	itemKeysPool.Put(itemKeysPtr)

	// 读取 hasMore 软缓存
	hasMore := false
	hasMoreStr, err := s.redis.Get(ctx, hasMoreKey).Result()
	if err == nil {
		hasMore = hasMoreStr == "1"
	} else {
		hasMore = len(items) == size // 降级: 满页说明可能还有更多
	}

	// 叠加当前用户状态
	enriched := s.enrichItems(ctx, items, currentUserID)

	if len(enriched) == 0 {
		enriched = []FeedItemResponse{}
	}

	return &FeedPageResponse{
		Items:   enriched,
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
//  4. 页注册：使用 SAdd 将 idsKey 注册到 "feed:public:pages" 集合中，
//     便于后续批量失效（虽然当前版本未使用此集合，但为未来维护留下了扩展点）。
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
	s.registerFeedPageKey(ctx, idsKey)
}

func (s *KnowPostFeedService) writeFeedIDListCache(ctx context.Context, idsKey, hasMoreKey string, rows []KnowPostFeedRow, hasMore bool) {
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
	ttl := time.Duration(l2IDListTTLBase+rand.Intn(l2IDListJitter)) * time.Second
	if err := s.redis.Expire(ctx, idsKey, ttl).Err(); err != nil {
		s.logger.Warn("failed to set expire on feed IDs", zap.String("idsKey", idsKey), zap.Error(err))
	}
	hasMoreTTL := time.Duration(l2HasMoreTTLBase+rand.Intn(l2HasMoreJitter)) * time.Second
	if err := s.redis.Set(ctx, hasMoreKey, boolToStr(hasMore), hasMoreTTL).Err(); err != nil {
		s.logger.Warn("failed to set hasMore cache", zap.String("hasMoreKey", hasMoreKey), zap.Error(err))
	}
}

func (s *KnowPostFeedService) writeFeedItemCaches(ctx context.Context, items []FeedItemResponse) {
	pipe := s.redis.Pipeline()
	for _, item := range items {
		itemKey := "feed:item:" + item.ID
		jsonBytes, err := json.Marshal(item)
		if err != nil {
			s.logger.Warn("failed to marshal feed item for cache", zap.String("itemID", item.ID), zap.Error(err))
			continue
		}
		ttl := time.Duration(l2ItemTTLBase+rand.Intn(l2ItemJitter)) * time.Second
		pipe.Set(ctx, itemKey, string(jsonBytes), ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Warn("failed to pipeline feed item cache entries", zap.Error(err))
	}
}

func (s *KnowPostFeedService) registerFeedPageKey(ctx context.Context, idsKey string) {
	if err := s.redis.SAdd(ctx, "feed:public:pages", idsKey).Err(); err != nil {
		s.logger.Warn("failed to register feed page key", zap.String("idsKey", idsKey), zap.Error(err))
	}
	if err := s.redis.Expire(ctx, "feed:public:pages", 24*time.Hour).Err(); err != nil {
		s.logger.Warn("failed to set expire on feed public pages set", zap.Error(err))
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
	itemKey := "feed:item:" + strconv.FormatUint(postID, 10)
	mineKey := fmt.Sprintf(mineFeedVersionKey, creatorID)

	if err := invalidateFeedScript.Run(ctx, s.redis, []string{itemKey, publicFeedVersionKey, mineKey}).Err(); err != nil {
		s.logger.Warn("failed to invalidate feed caches",
			zap.Uint64("postID", postID),
			zap.Uint64("creatorID", creatorID),
			zap.Error(err),
		)
	}
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
			TagJson:        r.AuthorTagJson,
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

	itemIDsPtr := itemIDsPool.Get().(*[]string)
	itemIDs := *itemIDsPtr
	itemIDs = itemIDs[:0]
	for _, item := range items {
		itemIDs = append(itemIDs, item.ID)
	}

	likedMap, err := s.counter.BatchIsLiked(ctx, *userID, "knowpost", itemIDs)
	if err != nil {
		s.logger.Warn("feed: batch is liked failed", zap.Error(err))
	}
	favedMap, favErr := s.counter.BatchIsFaved(ctx, *userID, "knowpost", itemIDs)
	if favErr != nil {
		s.logger.Warn("feed: batch is faved failed", zap.Error(favErr))
	}
	itemIDsPtr = &itemIDs
	itemIDsPool.Put(itemIDsPtr)

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

// recordItemHotKey 记录某个 feed 条目为热点，并酌情延长其 Redis 碎片缓存的 TTL。
//
// 功能：当用户在查看公共 Feed 时，此方法会被调用以记录每个展示条目的访问行为。
// HotKeyDetector 通过本地映射 + Redis Hash 滑动窗口统计每个 key 的跨实例访问频率，
// 当频率超过阈值时，会"标记"该 key 为热点。后续通过 TtlForPublic 可以根据热度
// 计算一个更长的 TTL。
//
// TTL 延长使用 Lua 脚本保证只增不减，多实例并发安全。
func (s *KnowPostFeedService) recordItemHotKey(ctx context.Context, itemID string) {
	if s.hotKey == nil {
		return
	}
	hotKeyID := "knowpost:" + itemID
	s.hotKey.Record(hotKeyID)

	baseTTL := extendTTLBase
	target := s.hotKey.TtlForPublic(ctx, baseTTL, hotKeyID)

	// Lua 脚本原子操作：只有当前 TTL < 目标 TTL 时才延长
	itemKey := "feed:item:" + itemID
	extendTTL(ctx, s.redis, itemKey, target)
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
func (s *KnowPostFeedService) cacheFeedPage(key string, resp *FeedPageResponse, cache *PrefixCache) {
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		s.logger.Warn("failed to marshal feed page for cache", zap.String("key", key), zap.Error(err))
		return
	}
	cache.Set([]byte(key), jsonBytes, l1FeedCacheTTL)
}

// parseFeedPage 将 feed 页的 JSON 缓存数据反序列化为 FeedPageResponse。
func (s *KnowPostFeedService) parseFeedPage(data []byte) (*FeedPageResponse, error) {
	return parseJSON[*FeedPageResponse](data)
}

// clamp 将一个整数值限制在 [lo, hi] 范围内。
//
// 功能：用于限制分页 size 参数的取值范围，防止过大或过小的查询。
//
// 参数：
//   - v: int，原始值。
//   - lo: int，最小值边界。
//   - hi: int，最大值边界。
//
// 返回值：
//   - int，限制在 [lo, hi] 范围内的值。
//
// 边界情况：
//   - v < lo 返回 lo。
//   - v > hi 返回 hi。
//   - lo <= v <= hi 返回 v。
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// max 返回两个整数中的较大者。
//
// 功能：用于确保页码最小值为 1。
// Go 标准库 math.Max 只支持 float64，这里提供 int 版本以避免类型转换。
//
// 参数：
//   - a: int，第一个值。
//   - b: int，第二个值。
//
// 返回值：int，a 和 b 中较大的值。
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
	return s.feedVersion(ctx, fmt.Sprintf(mineFeedVersionKey, userID))
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
	version, err := s.redis.Get(ctx, key).Int64()
	if err == nil && version > 0 {
		return version
	}
	return 1
}