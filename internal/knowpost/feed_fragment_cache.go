package knowpost

import (
	"context"
	"encoding/json"
	"math/rand"
	"strconv"
	"time"
)

// assembleFromCache 尝试从 Redis 的碎片缓存拼装出一页公共 Feed。
//
// 任意碎片缺失都视为未命中，直接回源。
// 这样做比“部分拼装再补查”更简单，也更不容易返回不完整页面。
func (s *KnowPostFeedService) assembleFromCache(ctx context.Context, idsKey, hasMoreKey string, page, size int, currentUserID *uint64) *FeedPageResponse {
	idStrs, err := s.redis.LRange(ctx, idsKey, 0, int64(size-1)).Result()
	if err != nil || len(idStrs) == 0 {
		return nil
	}

	itemKeys := make([]string, len(idStrs))
	for i, idStr := range idStrs {
		itemKeys[i] = "feed:item:" + idStr
	}
	itemJSONs, err := s.redis.MGet(ctx, itemKeys...).Result()
	if err != nil {
		return nil
	}

	items := make([]FeedItemResponse, 0, len(idStrs))
	for _, itemJSON := range itemJSONs {
		if itemJSON == nil {
			return nil
		}
		var item FeedItemResponse
		if err := json.Unmarshal([]byte(itemJSON.(string)), &item); err != nil {
			return nil
		}
		items = append(items, item)
	}

	hasMore := false
	hasMoreStr, err := s.redis.Get(ctx, hasMoreKey).Result()
	if err == nil {
		hasMore = hasMoreStr == "1"
	} else {
		hasMore = len(items) == size
	}

	return &FeedPageResponse{
		Items:   s.enrichItems(ctx, items, currentUserID),
		Page:    page,
		Size:    size,
		HasMore: hasMore,
	}
}

// writeFragmentCaches 把公共 Feed 的页级 ID 列表、条目碎片和 hasMore 标记写入 Redis。
//
// IDs 和 item 分开缓存的收益是：
//   - 单条帖子更新时只需要删掉 item 碎片；
//   - 整体页结构通过版本号递增自然失效。
func (s *KnowPostFeedService) writeFragmentCaches(ctx context.Context, idsKey, hasMoreKey string, size int, rows []KnowPostFeedRow, items []FeedItemResponse, hasMore bool) {
	idVals := make([]interface{}, len(rows))
	for i, row := range rows {
		idVals[i] = strconv.FormatUint(row.ID, 10)
	}
	if len(idVals) > 0 {
		s.redis.LPush(ctx, idsKey, idVals...)
		ttl := time.Duration(60+rand.Intn(31)) * time.Second
		s.redis.Expire(ctx, idsKey, ttl)

		hasMoreTTL := time.Duration(10+rand.Intn(11)) * time.Second
		s.redis.Set(ctx, hasMoreKey, boolToStr(hasMore), hasMoreTTL)
	}

	for _, item := range items {
		itemKey := "feed:item:" + item.ID
		jsonBytes, _ := json.Marshal(item)
		ttl := time.Duration(60+rand.Intn(31)) * time.Second
		s.redis.Set(ctx, itemKey, string(jsonBytes), ttl)
	}

	s.redis.SAdd(ctx, "feed:public:pages", idsKey)
}
