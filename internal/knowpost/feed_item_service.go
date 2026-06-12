package knowpost

import (
	"context"
	"encoding/json"
	"strconv"
)

// mapRowsToItems 把数据库行模型转换为 FeedItemResponse。
//
// 这里顺手叠加计数值，但不会叠加用户态 liked/faved。
// 用户态信息留到 enrichItems 再单独处理，避免缓存里混入用户相关视图。
func (s *KnowPostFeedService) mapRowsToItems(ctx context.Context, rows []KnowPostFeedRow, userID *uint64, includeIsTop bool) []FeedItemResponse {
	items := make([]FeedItemResponse, len(rows))
	for i, row := range rows {
		tags := parseStringArray(row.Tags)
		images := parseStringArray(row.ImgUrls)
		var cover *string
		if len(images) > 0 {
			cover = &images[0]
		}

		item := FeedItemResponse{
			ID:             strconv.FormatUint(row.ID, 10),
			Title:          row.Title,
			Description:    row.Description,
			CoverImage:     cover,
			Tags:           tags,
			AuthorAvatar:   row.AuthorAvatar,
			AuthorNickname: row.AuthorNickname,
			TagJson:        row.AuthorTagJson,
		}

		if s.counter != nil {
			counts, _ := s.counter.GetCounts(ctx, "knowpost", strconv.FormatUint(row.ID, 10), []string{"like", "fav"})
			item.LikeCount = int64(counts["like"])
			item.FavoriteCount = int64(counts["fav"])
		}

		if includeIsTop {
			isTop := row.IsTop
			item.IsTop = &isTop
		}
		items[i] = item
	}
	return items
}

// enrichItems 为条目叠加当前用户的点赞/收藏状态。
//
// 这些字段是用户维度的派生状态，不进入共享缓存。
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

// recordItemHotKey 记录热点条目，并按热度延长条目碎片 TTL。
//
// TTL 延长使用 Lua 脚本只增不减，避免并发请求把热点 key 的 TTL 反向缩短。
func (s *KnowPostFeedService) recordItemHotKey(ctx context.Context, itemID string) {
	hotKeyID := "knowpost:" + itemID
	s.hotKey.Record(hotKeyID)

	baseTTL := 60
	target := s.hotKey.TtlForPublic(baseTTL, hotKeyID)
	itemKey := "feed:item:" + itemID
	opCtx, cancel := bestEffortCacheContext(ctx)
	defer cancel()

	extendTTL(opCtx, s.redis, itemKey, target)
}

// parseFeedPage 把缓存中的 JSON 页数据反序列化为 FeedPageResponse。
func (s *KnowPostFeedService) parseFeedPage(data []byte) (*FeedPageResponse, error) {
	var resp FeedPageResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
