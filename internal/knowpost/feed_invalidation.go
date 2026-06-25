package knowpost

import (
	"context"
	"fmt"
	"strconv"

	"go.uber.org/zap"
)

// InvalidateAfterPostMutation 在知文发生变更后失效相关 feed 缓存。
//
// 功能：当知文被创建、更新或删除时调用，使 feed 缓存不过期即可反映最新状态。
// 具体操作：
//  1. 删除 Redis 中该条目的碎片缓存（"feed:item:{postID}"）。
//  2. 递增公共 Feed 的版本号（"feed:public:version"）。
//  3. 递增该用户"我的 Feed"的版本号（"feed:mine:version:{creatorID}"）。
//
// 原理：递增版本号会使所有带旧版本号的缓存 key 自然失效。
// 因为所有缓存 key 都编码了当前的 feedVersion（如 "feed:public:10:1:v1:3"，
// 其中 3 就是版本号）。旧版本的整页缓存不会被读取，从而实现批量失效。
//
// 这种"版本号递增"策略的优缺点：
//   - 优点：O(1) 复杂度，不管有多少页缓存，一次 Incr 即可让所有旧缓存失效。
//   - 缺点：在并发写高频场景下，版本号会快速递增，缓存命中率下降。
//     对于知文这种写操作远少于读操作的场景，此策略是非常合适的。
//
// WHY 同时递增两个版本号：
// 公共 Feed 是所有用户共享的视图，递增后所有用户都会看到更新。
// "我的 Feed" 只属于创作者本人，递增后只会影响该用户看到的"我的"列表。
//
// 参数：
//   - ctx: context.Context。
//   - postID: uint64，发生变更的知文 ID。
//   - creatorID: uint64，知文作者 ID。
//
// 实现细节：
//   - s.redis.Del 删除单条碎片缓存，复杂度 O(N) 其中 N 是 key 的数量（这里只有 1 个 key），
//     所以是 O(1)。
//   - s.redis.Incr 递增版本号，复杂度 O(1)。
func (s *KnowPostFeedService) InvalidateAfterPostMutation(ctx context.Context, postID, creatorID uint64) {
	if err := s.redis.Del(ctx, "feed:item:"+strconv.FormatUint(postID, 10)).Err(); err != nil {
		s.logger.Warn("failed to invalidate feed item cache", zap.Uint64("postID", postID), zap.Error(err))
	}
	if err := s.redis.Incr(ctx, publicFeedVersionKey).Err(); err != nil {
		s.logger.Warn("failed to increment public feed version", zap.Error(err))
	}
	if err := s.redis.Incr(ctx, fmt.Sprintf(mineFeedVersionKey, creatorID)).Err(); err != nil {
		s.logger.Warn("failed to increment mine feed version", zap.Uint64("creatorID", creatorID), zap.Error(err))
	}
}
