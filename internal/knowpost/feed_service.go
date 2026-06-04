package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/redis/go-redis/v9"

	"github.com/zhiguang/app/internal/cache"
)

const feedLayoutVer = 1

const (
	publicFeedVersionKey = "feed:public:version"
	mineFeedVersionKey   = "feed:mine:version:%d"
)

// KnowPostFeedService 实现基于碎片缓存架构的 Feed 流读取。
//
// 缓存架构（三级、碎片化）：
//
//	L1（freecache）：整页响应，约 50ns
//	L2（Redis 碎片缓存）：
//	  - IDs 列表（按小时分槽）：保存某一页的有序帖子 ID 列表
//	  - Item 缓存（按帖子维度）：保存单篇帖子的元信息
//	  - hasMore 软缓存：标记是否还有下一页
//	L3（MySQL）：真实数据源
//
// WHY：使用碎片缓存而不是整页缓存，是因为单篇帖子更新时只需要失效该帖子的碎片；
// 如果使用整页缓存，则所有包含该帖子的分页结果都要失效。
// WHY：按小时分槽保存 IDs，可以控制热门页失效时的影响范围。
type KnowPostFeedService struct {
	repo         *KnowPostRepository
	redis        *redis.Client
	l1Public     *freecache.Cache
	l1Mine       *freecache.Cache
	hotKey       *cache.HotKeyDetector
	counter      CounterClient
	singleFlight sync.Map // key → *sync.Mutex
}

// FeedCacheInvalidator 暴露知文写操作所需的 feed 缓存失效能力。
type FeedCacheInvalidator interface {
	InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64)
}

// NewKnowPostFeedService 创建带有 L1 缓存实例的 Feed 服务。
func NewKnowPostFeedService(
	repo *KnowPostRepository,
	redisClient *redis.Client,
	l1Public *freecache.Cache,
	l1Mine *freecache.Cache,
	hotKey *cache.HotKeyDetector,
) *KnowPostFeedService {
	return &KnowPostFeedService{
		repo:     repo,
		redis:    redisClient,
		l1Public: l1Public,
		l1Mine:   l1Mine,
		hotKey:   hotKey,
	}
}

// SetCounterClient 注入计数器依赖。
func (s *KnowPostFeedService) SetCounterClient(c CounterClient) { s.counter = c }

// ============================================================================
// 获取公共 Feed
// ============================================================================

// 获取公共 Feed returns a paginated list of published public posts (newest first).
func (s *KnowPostFeedService) GetPublicFeed(page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	ctx := context.Background()
	safeSize := clamp(size, 1, 50)
	safePage := max(page, 1)
	feedVersion := s.currentPublicFeedVersion(ctx)
	localPageKey := fmt.Sprintf("feed:public:%d:%d:v%d:%d", safeSize, safePage, feedLayoutVer, feedVersion)

	hourSlot := time.Now().Unix() / 3600
	idsKey := fmt.Sprintf("feed:public:ids:%d:%d:%d:%d", feedVersion, safeSize, hourSlot, safePage)
	hasMoreKey := idsKey + ":hasMore"

	// --- L1：freecache ---
	if val, err := s.l1Public.Get([]byte(localPageKey)); err == nil {
		resp, parseErr := s.parseFeedPage(val)
		if parseErr == nil {
			for _, item := range resp.Items {
				s.recordItemHotKey(item.ID)
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
			s.recordItemHotKey(item.ID)
		}
		return resp, nil
	}

	// --- Singleflight 锁 ---
	return s.getPublicFeedUnderLock(ctx, idsKey, hasMoreKey, localPageKey, safePage, safeSize, currentUserID)
}

func (s *KnowPostFeedService) getPublicFeedUnderLock(ctx context.Context, idsKey, hasMoreKey, localPageKey string, page, size int, currentUserID *uint64) (*FeedPageResponse, error) {
	lockIface, _ := s.singleFlight.LoadOrStore(idsKey, &sync.Mutex{})
	mu := lockIface.(*sync.Mutex)
	mu.Lock()
	defer func() {
		s.singleFlight.Delete(idsKey)
		mu.Unlock()
	}()

	// 在锁内再次检查 L2
	if resp := s.assembleFromCache(ctx, idsKey, hasMoreKey, page, size, currentUserID); resp != nil {
		s.cacheFeedPage(localPageKey, resp, s.l1Public)
		return resp, nil
	}

	// --- 查询数据库 ---
	offset := (page - 1) * size
	rows, err := s.repo.ListFeedPublic(size+1, offset)
	if err != nil {
		return nil, err
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

	// 写入碎片缓存
	s.writeFragmentCaches(ctx, idsKey, hasMoreKey, size, rows, items, hasMore)

	// 把整页结果写入 L1
	s.cacheFeedPage(localPageKey, resp, s.l1Public)

	return &FeedPageResponse{
		Items:   s.enrichItems(ctx, items, currentUserID),
		Page:    page,
		Size:    size,
		HasMore: hasMore,
	}, nil
}

// ============================================================================
// 获取我的已发布内容
// ============================================================================

// 获取我的已发布内容 returns a user's own published posts.
func (s *KnowPostFeedService) GetMyPublished(userID uint64, page, size int) (*FeedPageResponse, error) {
	ctx := context.Background()
	safeSize := clamp(size, 1, 50)
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
			s.l1Mine.Set([]byte(key), []byte(cached), 30)
			s.hotKey.Record(key)
			return resp, nil
		}
	}

	// 查询数据库
	offset := (safePage - 1) * safeSize
	rows, err := s.repo.ListMyPublished(userID, safeSize+1, offset)
	if err != nil {
		return nil, err
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
	jsonBytes, _ := json.Marshal(resp)
	baseTTL := 30 + rand.Intn(21) // 30-50s with jitter
	s.redis.Set(ctx, key, string(jsonBytes), time.Duration(baseTTL)*time.Second)
	s.l1Mine.Set([]byte(key), jsonBytes, baseTTL)
	s.hotKey.Record(key)

	return resp, nil
}

// ============================================================================
// 碎片缓存组装
// ============================================================================

// assembleFromCache 尝试从 Redis 的碎片缓存中还原一整页 feed。
// 只要任意碎片缺失，就返回 nil，表示本次缓存未命中。
func (s *KnowPostFeedService) assembleFromCache(ctx context.Context, idsKey, hasMoreKey string, page, size int, currentUserID *uint64) *FeedPageResponse {
	// 读取 ID 列表
	idStrs, err := s.redis.LRange(ctx, idsKey, 0, int64(size-1)).Result()
	if err != nil || len(idStrs) == 0 {
		return nil
	}

	// 批量读取条目碎片
	itemKeys := make([]string, len(idStrs))
	for i, idStr := range idStrs {
		itemKeys[i] = "feed:item:" + idStr
	}
	itemJsons, err := s.redis.MGet(ctx, itemKeys...).Result()
	if err != nil {
		return nil
	}

	// 解析条目内容
	items := make([]FeedItemResponse, 0, len(idStrs))
	for _, itemJson := range itemJsons {
		if itemJson == nil {
			return nil // 任意碎片缺失则视为缓存未命中
		}
		var item FeedItemResponse
		if err := json.Unmarshal([]byte(itemJson.(string)), &item); err != nil {
			return nil
		}
		items = append(items, item)
	}

	// 读取 hasMore 软缓存
	hasMore := false
	hasMoreStr, err := s.redis.Get(ctx, hasMoreKey).Result()
	if err == nil {
		hasMore = hasMoreStr == "1"
	} else {
		hasMore = len(items) == size // Fallback: full page means probably has more
	}

	// 叠加当前用户状态
	enriched := s.enrichItems(ctx, items, currentUserID)

	return &FeedPageResponse{
		Items:   enriched,
		Page:    page,
		Size:    size,
		HasMore: hasMore,
	}
}

// writeFragmentCaches 把 ID 列表、条目碎片和 hasMore 软缓存写入 Redis。
func (s *KnowPostFeedService) writeFragmentCaches(ctx context.Context, idsKey, hasMoreKey string, size int, rows []KnowPostFeedRow, items []FeedItemResponse, hasMore bool) {
	// 写入 ID 列表
	idVals := make([]interface{}, len(rows))
	for i, r := range rows {
		idVals[i] = strconv.FormatUint(r.ID, 10)
	}
	if len(idVals) > 0 {
		s.redis.LPush(ctx, idsKey, idVals...)
		ttl := time.Duration(60+rand.Intn(31)) * time.Second
		s.redis.Expire(ctx, idsKey, ttl)

		// hasMore 软缓存
		hasMoreTTL := time.Duration(10+rand.Intn(11)) * time.Second
		s.redis.Set(ctx, hasMoreKey, boolToStr(hasMore), hasMoreTTL)
	}

	// 写入条目碎片
	for _, item := range items {
		itemKey := "feed:item:" + item.ID
		jsonBytes, _ := json.Marshal(item)
		ttl := time.Duration(60+rand.Intn(31)) * time.Second
		s.redis.Set(ctx, itemKey, string(jsonBytes), ttl)
	}

	// 把页键注册到 pages 集合中，便于后续批量失效
	s.redis.SAdd(ctx, "feed:public:pages", idsKey)
}

// InvalidateAfterPostMutation 在知文发生变更后失效相关 feed 缓存。
// WHY：feed 页缓存和条目碎片缓存与详情缓存是独立维护的，
// 只失效详情页会导致 feed 中继续展示旧数据，直到 TTL 自然过期。
func (s *KnowPostFeedService) InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64) {
	s.redis.Del(ctx, "feed:item:"+strconv.FormatUint(postID, 10))
	s.redis.Incr(ctx, publicFeedVersionKey)
	s.redis.Incr(ctx, fmt.Sprintf(mineFeedVersionKey, creatorID))
}

// ============================================================================
// 条目映射与增强
// ============================================================================

func (s *KnowPostFeedService) mapRowsToItems(ctx context.Context, rows []KnowPostFeedRow, userID *uint64, includeIsTop bool) []FeedItemResponse {
	items := make([]FeedItemResponse, len(rows))
	for i, r := range rows {
		tags := parseStringArray(r.Tags)
		imgs := parseStringArray(r.ImgUrls)
		var cover *string
		if len(imgs) > 0 {
			cover = &imgs[0]
		}

		item := FeedItemResponse{
			ID:             strconv.FormatUint(r.ID, 10),
			Title:          r.Title,
			Description:    r.Description,
			CoverImage:     cover,
			Tags:           tags,
			AuthorAvatar:   r.AuthorAvatar,
			AuthorNickname: r.AuthorNickname,
			TagJson:        r.AuthorTagJson,
		}

		// 获取计数信息
		if s.counter != nil {
			counts, _ := s.counter.GetCounts(ctx, "knowpost", strconv.FormatUint(r.ID, 10), []string{"like", "fav"})
			item.LikeCount = int64(counts["like"])
			item.FavoriteCount = int64(counts["fav"])
		}

		if includeIsTop {
			isTop := r.IsTop
			item.IsTop = &isTop
		}

		items[i] = item
	}
	return items
}

// enrichItems 为 feed 条目叠加当前用户的点赞/收藏状态，这部分不会进入缓存。
func (s *KnowPostFeedService) enrichItems(ctx context.Context, items []FeedItemResponse, userID *uint64) []FeedItemResponse {
	if userID == nil || s.counter == nil {
		return items
	}

	enriched := make([]FeedItemResponse, len(items))
	for i, item := range items {
		liked, _ := s.counter.IsLiked(ctx, *userID, "knowpost", item.ID)
		faved, _ := s.counter.IsFaved(ctx, *userID, "knowpost", item.ID)
		item.Liked = &liked
		item.Faved = &faved
		enriched[i] = item
	}
	return enriched
}

func (s *KnowPostFeedService) recordItemHotKey(itemID string) {
	hotKeyID := "knowpost:" + itemID
	s.hotKey.Record(hotKeyID)

	baseTTL := 60
	target := s.hotKey.TtlForPublic(baseTTL, hotKeyID)

	itemKey := "feed:item:" + itemID
	itemTTL := s.redis.TTL(context.Background(), itemKey).Val()
	if itemTTL > 0 && int(itemTTL.Seconds()) < target {
		s.redis.Expire(context.Background(), itemKey, time.Duration(target)*time.Second)
	}
}

// ============================================================================
// 辅助函数
// ============================================================================

func (s *KnowPostFeedService) cacheFeedPage(key string, resp *FeedPageResponse, cache *freecache.Cache) {
	jsonBytes, _ := json.Marshal(resp)
	cache.Set([]byte(key), jsonBytes, 15) // 15s for L1 feed cache
}

func (s *KnowPostFeedService) parseFeedPage(data []byte) (*FeedPageResponse, error) {
	var resp FeedPageResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (s *KnowPostFeedService) currentPublicFeedVersion(ctx context.Context) int64 {
	return s.feedVersion(ctx, publicFeedVersionKey)
}

func (s *KnowPostFeedService) currentMineFeedVersion(ctx context.Context, userID uint64) int64 {
	return s.feedVersion(ctx, fmt.Sprintf(mineFeedVersionKey, userID))
}

func (s *KnowPostFeedService) feedVersion(ctx context.Context, key string) int64 {
	version, err := s.redis.Get(ctx, key).Int64()
	if err == nil && version > 0 {
		return version
	}
	return 1
}
