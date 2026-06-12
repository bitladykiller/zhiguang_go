package knowpost

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coocood/freecache"
)

// cacheFeedPage 把整页响应 best-effort 写入 L1 freecache。
func (s *KnowPostFeedService) cacheFeedPage(key string, resp *FeedPageResponse, cache *freecache.Cache) {
	jsonBytes, _ := json.Marshal(resp)
	setFreeCacheValue(cache, key, jsonBytes, 15)
}

// clamp 将整数限制在 [lo, hi] 范围内。
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// max 返回两个整数中的较大值。
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// boolToStr 把布尔值转成 Redis 中更便于存储的 "1"/"0"。
func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// feedVersion 统一读取 feed 版本号，默认从 1 开始。
//
// 版本号进缓存键之后，写路径只需要 Incr，而不用枚举所有页键删除。
func (s *KnowPostFeedService) feedVersion(ctx context.Context, key string) int64 {
	version, err := s.redis.Get(ctx, key).Int64()
	if err == nil && version > 0 {
		return version
	}
	return 1
}

func (s *KnowPostFeedService) currentPublicFeedVersion(ctx context.Context) int64 {
	return s.feedVersion(ctx, publicFeedVersionKey)
}

func (s *KnowPostFeedService) currentMineFeedVersion(ctx context.Context, userID uint64) int64 {
	return s.feedVersion(ctx, fmt.Sprintf(mineFeedVersionKey, userID))
}
