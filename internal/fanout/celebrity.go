package fanout

import (
	"context"
	"strconv"

	"go.uber.org/zap"
)

// FollowerCounter 提供粉丝数快查能力。
//
// 由 counter 模块的用户维度计数（cnt:user:{id} 的 follower 字段）实现。
type FollowerCounter interface {
	// FollowerCount 返回用户的粉丝数。
	// 计数缺失或不可用时返回 (0, false)，调用方据此走慢路径。
	FollowerCount(ctx context.Context, userID uint64) (int64, bool)
}

// isCelebrity 判断作者是否应走「拉」模式。
//
// 两级判定，快路径不可信时自动降级到慢路径：
//
//  1. **快路**：读粉丝计数。达到阈值即判定为大 V，无需遍历粉丝列表。
//  2. **慢路**：计数缺失/异常时返回 unknown，由写扩散过程边推边数——
//     累计触达超过阈值就中止推送并把作者补记为大 V（见 FanoutPost）。
//
// WHY 不完全依赖计数器：
//
//	粉丝计数是 Redis 里的增量值，可能因消费失败、键过期或重建而失真。
//	若完全依赖它，一次计数丢失就会让某个真实大 V 被判成普通作者，
//	触发一次几十万粉丝的写扩散风暴。
//	「边推边数」让判定不依赖任何外部状态的正确性，是自我修正的。
func (s *Service) isCelebrity(ctx context.Context, authorID uint64) (celebrity bool, known bool) {
	// 已在名单中：直接走拉，省掉计数查询。
	if s.redisClient != nil {
		if member, err := s.redisClient.SIsMember(ctx, celebritySetKey, authorID).Result(); err == nil && member {
			return true, true
		}
	}

	if s.followerCounter == nil {
		return false, false
	}
	count, ok := s.followerCounter.FollowerCount(ctx, authorID)
	if !ok {
		return false, false
	}
	return count >= int64(s.cfg.CelebrityThreshold), true
}

// markCelebrity 把作者加入大 V 名单。
//
// 名单不设 TTL：大 V 身份是单调的（粉丝数掉下阈值的情况罕见，且误判为大 V 只是让
// 读者多拉一次发件箱，不会丢内容）。需要移除时由运维显式 SREM。
func (s *Service) markCelebrity(ctx context.Context, authorID uint64) {
	if s.redisClient == nil {
		return
	}
	if err := s.redisClient.SAdd(ctx, celebritySetKey, authorID).Err(); err != nil {
		s.logger.Warn("failed to mark author as celebrity",
			zap.Uint64("authorID", authorID), zap.Error(err))
	}
}

// celebritiesAmong 从给定的关注列表中筛出大 V。
//
// 用一次 SMISMEMBER 完成批量判定，而不是逐个 SISMEMBER。
// 返回值保持输入顺序，便于上层按关注时间倒序截断。
func (s *Service) celebritiesAmong(ctx context.Context, authorIDs []uint64) ([]uint64, error) {
	if len(authorIDs) == 0 || s.redisClient == nil {
		return nil, nil
	}

	members := make([]any, len(authorIDs))
	for i, id := range authorIDs {
		members[i] = strconv.FormatUint(id, 10)
	}

	flags, err := s.redisClient.SMIsMember(ctx, celebritySetKey, members...).Result()
	if err != nil {
		return nil, err
	}

	out := make([]uint64, 0, len(authorIDs))
	for i, isMember := range flags {
		if i < len(authorIDs) && isMember {
			out = append(out, authorIDs[i])
		}
	}
	return out, nil
}
