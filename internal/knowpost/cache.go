package knowpost

import (
	"context"
	"fmt"
	"time"
)

// --- [缓存协调] --- //

func (s *KnowPostService) invalidateCache(id uint64) {
	ctx := context.Background()
	pageKey := fmt.Sprintf("knowpost:detail:%d:v%d", id, detailLayoutVer)
	s.redis.Del(ctx, pageKey)
	s.l1Cache.Del([]byte(pageKey))
}

func (s *KnowPostService) invalidateFeedCaches(id, creatorID uint64) {
	if s.feedCache == nil {
		return
	}
	s.feedCache.InvalidateAfterPostMutation(context.Background(), id, creatorID)
}

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
