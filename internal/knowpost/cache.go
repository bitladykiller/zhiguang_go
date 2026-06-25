package knowpost

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const hotKeyBaseTTL = 60

// --- [缓存协调] ---

// invalidateCache 删除知文详情页的 L1 和 L2 缓存（缓存双删策略的后半段）。
// 写前删除在 write_service.go 对应方法中完成。
// 缓存键格式：knowpost:detail:{id}:v{version}
func (s *KnowPostService) invalidateCache(ctx context.Context, id uint64) {
	pageKey := fmt.Sprintf("knowpost:detail:%d:v%d", id, detailLayoutVer)
	if err := s.redis.Del(ctx, pageKey).Err(); err != nil {
		zap.L().Warn("failed to delete L2 detail cache", zap.String("pageKey", pageKey), zap.Error(err))
	}
	s.l1Cache.Del([]byte(pageKey))
}

// invalidateFeedCaches 在知文变更后失效对应的 Feed 缓存。
func (s *KnowPostService) invalidateFeedCaches(ctx context.Context, id, creatorID uint64) {
	if s.feedCache == nil {
		return
	}
	s.feedCache.InvalidateAfterPostMutation(ctx, id, creatorID)
}

// recordHotKeyAndExtendTTL 记录热点访问并酌情延长缓存 TTL。
func (s *KnowPostService) recordHotKeyAndExtendTTL(ctx context.Context, id uint64, pageKey string) {
	hotKeyID := fmt.Sprintf("knowpost:%d", id)
	s.hotKey.Record(hotKeyID)

	baseTTL := hotKeyBaseTTL
	target := s.hotKey.TTLForPublic(ctx, baseTTL, hotKeyID)
	if target <= baseTTL {
		return
	}

	if !extendTTL(ctx, s.redis, pageKey, target) {
		zap.L().Debug("extendTTL skipped for pageKey", zap.String("pageKey", pageKey), zap.Int("targetTTL", target))
	}

	itemKey := fmt.Sprintf("feed:item:%d", id)
	if !extendTTL(ctx, s.redis, itemKey, target) {
		zap.L().Debug("extendTTL skipped for itemKey", zap.String("itemKey", itemKey), zap.Int("targetTTL", target))
	}
}

// extendTTLScript 是 Redis Lua 脚本，原子延长缓存 TTL（只增不减）。
var extendTTLScript = redis.NewScript(`
local current = redis.call('TTL', KEYS[1])
if current > 0 and current < tonumber(ARGV[1]) then
    return redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return 0
`)

// extendTTL  使用 Redis Lua 脚本原子延长缓存 TTL（只增不减）。
// 兼容 Redis 6.x+，多实例并发安全。
func extendTTL(ctx context.Context, client *redis.Client, key string, targetSeconds int) bool {
	result, err := extendTTLScript.Run(ctx, client, []string{key}, targetSeconds).Int()
	if err != nil {
		return false
	}
	return result == 1
}
