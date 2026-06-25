package knowpost

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"go.uber.org/zap"
)

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
	itemKeys := make([]string, len(idStrs))
	for i, idStr := range idStrs {
		itemKeys[i] = "feed:item:" + idStr
	}
	itemJsons, err := s.redis.MGet(ctx, itemKeys...).Result()
	if err != nil {
		s.logger.Warn("failed to MGet feed item cache entries", zap.Strings("itemKeys", itemKeys), zap.Error(err))
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
	// 写入 ID 列表
	idVals := make([]interface{}, len(rows))
	for i, r := range rows {
		idVals[i] = strconv.FormatUint(r.ID, 10)
	}
	if len(idVals) > 0 {
		if err := s.redis.LPush(ctx, idsKey, idVals...).Err(); err != nil {
			s.logger.Warn("failed to LPush feed IDs", zap.String("idsKey", idsKey), zap.Error(err))
		}
		ttl := time.Duration(l2IDListTTLBase+rand.Intn(l2IDListJitter)) * time.Second
		if err := s.redis.Expire(ctx, idsKey, ttl).Err(); err != nil {
			s.logger.Warn("failed to set expire on feed IDs", zap.String("idsKey", idsKey), zap.Error(err))
		}

		// hasMore 软缓存
		hasMoreTTL := time.Duration(l2HasMoreTTLBase+rand.Intn(l2HasMoreJitter)) * time.Second
		if err := s.redis.Set(ctx, hasMoreKey, boolToStr(hasMore), hasMoreTTL).Err(); err != nil {
			s.logger.Warn("failed to set hasMore cache", zap.String("hasMoreKey", hasMoreKey), zap.Error(err))
		}
	}

	// 写入条目碎片
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
		s.logger.Warn("pipeline write feed item caches failed", zap.Error(err))
	}

	// 把页键注册到 pages 集合中，便于后续批量失效
	if err := s.redis.SAdd(ctx, "feed:public:pages", idsKey).Err(); err != nil {
		s.logger.Warn("failed to register feed page key", zap.String("idsKey", idsKey), zap.Error(err))
	}
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
//
// 功能：与 parseDetail 类似，用于从 L1（freecache）或 L2（Redis）的 JSON 缓存中解析分页数据。
//
// 参数：
//   - data: []byte，JSON 格式的缓存数据。
//
// 返回值：
//   - *FeedPageResponse: 反序列化成功后的分页响应。
//   - error: JSON 解析失败时返回错误。
func (s *KnowPostFeedService) parseFeedPage(data []byte) (*FeedPageResponse, error) {
	var resp FeedPageResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("map rows to items: %w", err)
	}
	return &resp, nil
}
