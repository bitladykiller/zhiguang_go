package knowpost

import (
	"context"
	"fmt"
	"time"
)

// --- [缓存协调] ---

// invalidateCache 删除知文章节的 L1 和 L2 缓存。
//
// 缓存键格式：`knowpost:detail:{id}:v{version}`
// 版本号（detailLayoutVer）用于在缓存结构不兼容时全局爆破全部详情缓存。
//
// 在写操作前后各调用一次（缓存双删策略）：
//   - 写入前删除：确保旧数据不会在写入过程中被读取到。
//   - 写入后删除：确保后续读取不会被写入过程中加载到的旧数据污染。
func (s *KnowPostService) invalidateCache(id uint64) {
	ctx := context.Background()
	pageKey := fmt.Sprintf("knowpost:detail:%d:v%d", id, detailLayoutVer)
	s.redis.Del(ctx, pageKey)
	s.l1Cache.Del([]byte(pageKey))
}

// invalidateFeedCaches 在知文章节发生变更后失效对应的 Feed 缓存。
//
// 失效策略：
//   - Feed 列表由 KnowPostFeedService 管理，不直接操作 Redis。
//   - 调用 InvalidateAfterPostMutation 来递增 feed version，
//     使已缓存的整页结果通过版本号变更整体过期。
func (s *KnowPostService) invalidateFeedCaches(id, creatorID uint64) {
	if s.feedCache == nil {
		return
	}
	s.feedCache.InvalidateAfterPostMutation(context.Background(), id, creatorID)
}

// recordHotKeyAndExtendTTL 记录某篇知文的热点访问，并酌情延长缓存 TTL。
//
// 当 HotKeyDetector 判断某个键的访问频次达到阈值后，
// 会延长其详情缓存和 feed 条目碎片的 TTL，减少后续缓存穿透。
func (s *KnowPostService) recordHotKeyAndExtendTTL(id uint64, pageKey string) {
	hotKeyID := fmt.Sprintf("knowpost:%d", id)
	s.hotKey.Record(hotKeyID)

	baseTTL := 60
	target := s.hotKey.TtlForPublic(baseTTL, hotKeyID)

	detailTTL := s.redis.TTL(context.Background(), pageKey).Val()
	if detailTTL > 0 && int(detailTTL.Seconds()) < target {
		s.redis.Expire(context.Background(), pageKey, time.Duration(target)*time.Second)
	}

	itemKey := fmt.Sprintf("feed:item:%d", id)
	itemTTL := s.redis.TTL(context.Background(), itemKey).Val()
	if itemTTL > 0 && int(itemTTL.Seconds()) < target {
		s.redis.Expire(context.Background(), itemKey, time.Duration(target)*time.Second)
	}
}
