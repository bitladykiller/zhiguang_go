package knowpost

import (
	"context"
	"fmt"
	"time"
)

// --- [缓存协调] ---

// invalidateCache 删除知文详情页的 L1（freecache）和 L2（Redis）缓存。
//
// 功能：根据知文 ID 和当前布局版本号（detailLayoutVer）生成缓存键，
// 然后同时删除 L1（进程级 freecache）和 L2（Redis）中的缓存数据。
//
// 缓存键格式：`knowpost:detail:{id}:v{version}`
//   其中 version = detailLayoutVer（当前为 1）。
//   这个版本号用于在缓存结构不兼容时全局爆破全部详情缓存。
//   例如从 v1 升级到 v2 时，所有旧版本的缓存自然失效。
//
// 在写操作前后各调用一次（缓存双删策略，Cache-Aside Double Delete）：
//   - 写入前删除：确保旧数据不会在写入过程中被读取到（最终一致性窗口最小化）。
//   - 写入后删除：确保后续读取不会被写入过程中加载到的旧数据污染。
//     在并发场景下，可能有一个读取线程在写入线程完成前将旧数据加载到缓存中，
//     第二次删除可以清除这种竞争条件导致的不一致。
//
// 参数：
//   - id: uint64，知文 ID。
func (s *KnowPostService) invalidateCache(id uint64) {
	ctx := context.Background()
	pageKey := fmt.Sprintf("knowpost:detail:%d:v%d", id, detailLayoutVer)
	s.redis.Del(ctx, pageKey)
	s.l1Cache.Del([]byte(pageKey))
}

// invalidateFeedCaches 在知文发生变更后失效对应的 Feed 缓存。
//
// 功能：通过 FeedCacheInvalidator 接口委派 Feed 缓存失效逻辑给
// KnowPostFeedService。KnowPostService（写操作）不直接操作 Feed 的 Redis key，
// 而是通过接口调用 InvalidateAfterPostMutation，该接口会：
//   - 递增公共 Feed 版本号（publicFeedVersionKey）。
//   - 递增用户"我的 Feed"版本号（mineFeedVersionKey）。
//   - 删除该条目的碎片缓存（"feed:item:{id}"）。
//
// 参数：
//   - id: uint64，知文 ID。
//   - creatorID: uint64，作者 ID。
//
// 边界情况：
//   - feedCache == nil：不做任何操作，不会 panic。
//     这在 KnowPostService 刚构造完成但 SetFeedCacheInvalidator 尚未被调用时发生。
func (s *KnowPostService) invalidateFeedCaches(id, creatorID uint64) {
	if s.feedCache == nil {
		return
	}
	s.feedCache.InvalidateAfterPostMutation(context.Background(), id, creatorID)
}

// recordHotKeyAndExtendTTL 记录某篇知文的热点访问，并酌情延长缓存 TTL。
//
// 功能：在详情页或 Feed 被访问时调用。HotKeyDetector 使用本地 map + Redis Hash
// 滑动窗口统计每个 key 的访问频率。当频率超过阈值时，通过 TtlForPublic 返回一个
// 更长的 TTL（比如从 60s 延长到 300s），并通过 EXPIPE 命令更新 Redis 中的 TTL。
//
// 会延长 TTL 的缓存包括：
//   - 详情页缓存（knowpost:detail:{id}:v{version}）
//   - Feed 条目碎片缓存（feed:item:{id}）
//
// 设计意图：
//   热点条目被大量用户频繁访问。如果不延长 TTL，这些条目会在每个 TTL 周期结束后
//   引发大量缓存回源查询。通过 HotKeyDetector 的识别和 TTL 延长，
//   热点条目在缓存中停留时间更长，有效降低数据库负载。
//
// 边界情况：
//   - Redis TTL 查询失败（如 key 已过期）：TTL 返回负值，此时不执行延长。
//   - 当前 TTL 已经比目标 TTL 长：不缩短。
//   - 延期后的 TTL 最小值不低于 baseTTL（安全下限）。
//
// 参数：
//   - id: uint64，当前被访问的知文 ID。
//   - pageKey: string，详情页的缓存键名。
//
// HotKeyDetector 的工作原理：
//   cache.HotKeyDetector 使用本地 map 记录每个 key 在 6 秒窗口内的访问计数，
//   每 6 秒批量 flush 到 Redis Hash 进行跨实例聚合。当某个 key 在 60 秒窗口内的
//   全局访问计数超过配置阈值时，被认为是一个"热点 key"。
//   TtlForPublic 方法根据热度和基础 TTL 计算出一个延长的 TTL 值。
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
