package knowpost

import (
	"context"
	"strconv"

	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/jsonutil"
)

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
	if s.counter != nil && len(rows) > 0 {
		entityIDs := make([]string, len(rows))
		for i, r := range rows {
			entityIDs[i] = strconv.FormatUint(r.ID, 10)
		}
		var err error
		countsBatch, err = s.counter.GetCountsBatch(ctx, "knowpost", entityIDs, []string{"like", "fav"})
		if err != nil {
			s.logger.Warn("feed enrichment: get counts batch failed", zap.Error(err))
		}
	}

	for i, r := range rows {
		tags := jsonutil.ParseStringArray(r.Tags)
		imgs := jsonutil.ParseStringArray(r.ImgUrls)
		var cover *string
		if len(imgs) > 0 {
			cover = &imgs[0]
		}

		eid := strconv.FormatUint(r.ID, 10)
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

	itemIDs := make([]string, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID
	}

	likedMap, err := s.counter.BatchIsLiked(ctx, *userID, "knowpost", itemIDs)
	if err != nil {
		s.logger.Warn("feed enrichment: batch is liked failed", zap.Error(err))
	}
	favedMap, err := s.counter.BatchIsFaved(ctx, *userID, "knowpost", itemIDs)
	if err != nil {
		s.logger.Warn("feed enrichment: batch is faved failed", zap.Error(err))
	}

	enriched := make([]FeedItemResponse, len(items))
	for i, item := range items {
		item.ApplyLikedFaved(likedMap, favedMap)
		enriched[i] = item
	}
	return enriched
}

// recordItemHotKey 记录某个 feed 条目为热点，并酌情延长其 Redis 碎片缓存的 TTL。
//
// 功能：当用户在查看公共 Feed 时，此方法会被调用以记录每个展示条目的访问行为。
// HotKeyDetector 通过本地映射 + Redis Hash 滑动窗口统计每个 key 的跨实例访问频率，
// 当频率超过阈值时，会"标记"该 key 为热点。后续通过 TTLForPublic 可以根据热度
// 计算一个更长的 TTL。
//
// TTL 延长使用 Lua 脚本保证只增不减，多实例并发安全。
func (s *KnowPostFeedService) recordItemHotKey(ctx context.Context, itemID string) {
	hotKeyID := "knowpost:" + itemID
	s.hotKey.Record(hotKeyID)

	baseTTL := extendTTLBase
	target := s.hotKey.TTLForPublic(ctx, baseTTL, hotKeyID)
	if target <= baseTTL {
		return
	}

	itemKey := "feed:item:" + itemID
	extendTTL(ctx, s.redis, itemKey, target)
}
