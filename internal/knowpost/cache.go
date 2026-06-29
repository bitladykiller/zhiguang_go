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
	verKey := fmt.Sprintf("knowpost:ver:%d", id)
	detailPrefix := fmt.Sprintf("knowpost:detail:%d:v%d:ver", id, detailLayoutVer)
	_, err := invalidateCacheScript.Run(ctx, s.redis, []string{verKey, detailPrefix}).Result()
	if err != nil {
		s.logger.Warn("failed to invalidate post cache", zap.Uint64("id", id), zap.Error(err))
	}
	l1Key := fmt.Sprintf("knowpost:detail:%d:v%d", id, detailLayoutVer)
	s.l1Cache.Del([]byte(l1Key))
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

// recordHotKeyAndExtendTTL 记录某篇知文的热点访问，并酌情延长缓存 TTL。
//
// 功能：在详情页或 Feed 被访问时调用。HotKeyDetector 使用本地 map + Redis Hash
// 滑动窗口统计每个 key 的访问频率。当频率超过阈值时，通过 TtlForPublic 返回一个
// 更长的 TTL（比如从 60s 延长到 300s），并通过 EXPIRE GT 命令更新 Redis 中的 TTL。
//
// 会延长 TTL 的缓存包括：
//   - 详情页缓存（knowpost:detail:{id}:v{detailLayoutVer}:ver{版本}）
//   - Feed 条目碎片缓存（feed:item:{id}）
//
// 设计意图：
//   热点条目被大量用户频繁访问。如果不延长 TTL，这些条目会在每个 TTL 周期结束后
//   引发大量缓存回源查询。通过 HotKeyDetector 的识别和 TTL 延长，
//   热点条目在缓存中停留时间更长，有效降低数据库负载。
//
// TTL 延长使用 Lua 脚本保证只增不减：
//   多实例并发延长同一 key 时，Lua 脚本在 Redis 中原子执行，
//   先读当前 TTL，只有当前 TTL < 目标 TTL 时才 EXPIRE。
//   不存在竞态条件导致 TTL 被缩短的问题。
//   兼容 Redis 6.x（比 EXPIRE GT 要求 7.0+ 更通用）。
//
// 边界情况：
//   - key 已过期（不存在）：Lua 脚本中 TTL 返回 -2，条件不满足，不操作。
//   - 当前 TTL 已经比目标 TTL 长：Lua 脚本中条件不满足，不操作，TTL 保持原值。
//   - Lua 脚本执行出错：extendTTL 返回 false，不影响业务正常运行（只是 TTL 延长功能降级）。
//
// 参数：
//   - ctx: context.Context，用于控制 Redis 操作超时
//   - id: uint64，当前被访问的知文 ID。
//   - pageKey: string，详情页的缓存键名。
//
// HotKeyDetector 的工作原理：
//   cache.HotKeyDetector 使用本地 map 记录每个 key 在 6 秒窗口内的访问计数，
//   每 6 秒批量 flush 到 Redis Hash 进行跨实例聚合。当某个 key 在 60 秒窗口内的
//   全局访问计数超过配置阈值时，被认为是一个"热点 key"。
//   TtlForPublic 方法根据热度和基础 TTL 计算出一个延长的 TTL 值。
func (s *KnowPostService) recordHotKeyAndExtendTTL(ctx context.Context, id uint64, pageKey string) {
	hotKeyID := fmt.Sprintf("knowpost:%d", id)
	s.hotKey.Record(hotKeyID)

	_, _, _, _, _, _, ttlMedium, _ := s.detailCacheTTLValues()
	baseTTL := ttlMedium
	target := s.hotKey.TtlForPublic(ctx, baseTTL, hotKeyID)

	extendTTLScript := redis.NewScript(`
local current = redis.call('TTL', KEYS[1])
if current > 0 and current < tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
current = redis.call('TTL', KEYS[2])
if current > 0 and current < tonumber(ARGV[1]) then
    redis.call('EXPIRE', KEYS[2], ARGV[1])
end
return 1
`)
	itemKey := fmt.Sprintf("feed:item:%d", id)
	extendTTLScript.Run(ctx, s.redis, []string{pageKey, itemKey}, target)
}

// extendTTLScript 是 Redis Lua 脚本，原子性地延长缓存 TTL（只增不减）。
//
// 逻辑：只有当 key 存在且当前 TTL 小于目标 TTL 时，才执行 EXPIRE。
// 在 Redis 中 Lua 脚本是原子执行的，不存在 TTL 查询和 EXPIRE 之间的竞态窗口。
// 多实例并发调用时，每个实例都只会在当前 TTL < targetSeconds 时更新，
// 不会把其他实例刚延长的 TTL 缩短。
//
// 参数：
//   KEYS[1]   = 缓存键名
//   ARGV[1]   = 目标 TTL（秒）
//
// 返回值：
//   1 = TTL 已延长
//   0 = 未延长（key 不存在或当前 TTL >= 目标 TTL）
var extendTTLScript = redis.NewScript(`
local current = redis.call('TTL', KEYS[1])
if current > 0 and current < tonumber(ARGV[1]) then
    return redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return 0
`)

// extendTTL 使用 Redis Lua 脚本原子性地延长缓存 TTL。
//
// 相比 EXPIRE GT 命令（需要 Redis 7.0+），Lua 脚本在 Redis 6.x 也能运行，
// 且行为完全一致：只有当新 TTL 大于当前 TTL 时才更新。
// Lua 脚本在 Redis 中原子执行，不存在 TTL 查询和 EXPIRE 之间的竞态窗口。
//
// 参数：
//   - ctx: context.Context
//   - client: Redis 客户端
//   - key: 缓存键
//   - targetSeconds: 目标 TTL（秒）
//
// 返回值：
//   true  = TTL 已延长
//   false = 未延长（key 不存在或当前 TTL >= 目标 TTL）
//
// 边界情况：
//   - key 不存在（TTL 返回 -2）：条件不满足，不操作
//   - key 永不过期（TTL 返回 -1）：条件不满足（-1 < targetSeconds 为 false），不操作
//   - 当前 TTL >= 目标 TTL：不操作，保持原值
func extendTTL(ctx context.Context, client *redis.Client, key string, targetSeconds int) bool {
	result, err := extendTTLScript.Run(ctx, client, []string{key}, targetSeconds).Int()
	if err != nil {
		return false
	}
	return result == 1
}