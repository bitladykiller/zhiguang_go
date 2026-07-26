package knowpost

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// --- [缓存协调] ---

// invalidateCache 通过递增版本号使知文详情页的缓存全局失效。
//
// 功能：递增 Redis 中该知文对应的版本计数器（knowpost:ver:{id}），
// 使所有实例的 L1（freecache）和 L2（Redis）中旧版本缓存键自动失效。
// 由于缓存键中包含版本号，旧版本的 L1 条目在其他实例上即使未被主动删除，
// 也不会被后续读取命中（键不匹配）。
//
// 版本计数器设计：
//   - 首次写入时，版本计数器从 0 递增到 1，与 detailLayoutVer 初始值一致。
//   - 每次写操作 INCR 一次，生成全新的缓存键。
//   - 读取时若版本计数器不存在（GET 返回 0），则以 detailLayoutVer 为默认值。
//
// 在写操作前后各调用一次（缓存双删策略，Cache-Aside Double Delete）：
//   - 写入前删除：确保旧数据不会在写入过程中被读取到（最终一致性窗口最小化）。
//   - 写入后删除：确保后续读取不会被写入过程中加载到的旧数据污染。
//     在并发场景下，可能有一个读取线程在写入线程完成前将旧数据加载到缓存中，
//     第二次删除可以清除这种竞争条件导致的不一致。
//
// 参数：
//   - ctx: context.Context，用于传递请求上下文和控制超时。
//   - id: uint64，知文 ID。
var invalidateCacheScript = redis.NewScript(`
local ver = redis.call('INCR', KEYS[1])
if ver < 1 then
  redis.call('SET', KEYS[1], 1)
  ver = 1
end
local pageKey = KEYS[2] .. ver
redis.call('DEL', pageKey)
return ver
`)

func (s *KnowPostService) invalidateCache(ctx context.Context, id uint64) {
	verKey := detailVersionKey(id)
	detailPrefix := fmt.Sprintf("knowpost:detail:%d:v%d:ver", id, detailLayoutVer)
	if _, err := invalidateCacheScript.Run(ctx, s.redis, []string{verKey, detailPrefix}).Result(); err != nil {
		s.logger.Warn("failed to invalidate post cache", zap.Uint64("id", id), zap.Error(err))
	}
	// 本实例刚把版本号推进了一格，必须同步作废进程内的版本号短缓存，
	// 否则本实例在短缓存 TTL 内仍会用旧版本号拼键，出现「自己写完自己读不到」。
	s.versions.Drop(verKey)
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
//   - ctx: context.Context，用于传递请求上下文和控制超时。
//   - id: uint64，知文 ID。
//   - creatorID: uint64，作者 ID。
//
// 边界情况：
//   - feedCache == nil：不做任何操作，不会 panic。
//     这在 KnowPostService 刚构造完成但 SetFeedCacheInvalidator 尚未被调用时发生。
func (s *KnowPostService) invalidateFeedCaches(ctx context.Context, id, creatorID uint64) {
	if s.feedCache == nil {
		return
	}
	s.feedCache.InvalidateAfterPostMutation(ctx, id, creatorID)
}
