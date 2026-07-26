package fanout

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// OnFollow 在用户关注某作者后，把该作者近期的帖子回填进关注者的收件箱。
//
// WHY 需要回填：
//
//	写扩散只在**发帖那一刻**推送给当时的粉丝。刚关注的人不在那批粉丝里，
//	因此在对方发下一条帖之前，关注者的信息流里看不到任何该作者的内容。
//	对于发帖不频繁的作者，这个空窗可能长达数天——用户会认为「关注没生效」。
//
// 成本有界：一次 ZRANGE（最多 FollowBackfillLimit 条）+ 一次 ZADD，与粉丝数无关。
//
// 大 V 无需回填：读者会通过拉路直接读到对方发件箱的全部近期内容，
// 回填反而会在收件箱里留下重复条目（虽然归并时会去重，但白占空间）。
func (s *Service) OnFollow(ctx context.Context, followerID, authorID uint64) error {
	if s == nil || s.redisClient == nil || s.cfg.FollowBackfillLimit <= 0 {
		return nil
	}

	if celebrity, known := s.isCelebrity(ctx, authorID); known && celebrity {
		return nil // 走拉路，无需回填
	}

	items, err := s.redisClient.ZRevRangeWithScores(
		ctx, authorBoxKey(authorID), 0, int64(s.cfg.FollowBackfillLimit-1),
	).Result()
	if err != nil {
		return fmt.Errorf("fanout: read author box for backfill: %w", err)
	}
	if len(items) == 0 {
		return nil
	}

	key := timelineKey(followerID)
	pipe := s.redisClient.Pipeline()
	pipe.ZAdd(ctx, key, items...)
	pipe.ZRemRangeByRank(ctx, key, 0, int64(-s.cfg.TimelineMaxItems-1))
	pipe.Expire(ctx, key, s.cfg.TimelineTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("fanout: backfill timeline: %w", err)
	}

	s.logger.Debug("backfilled timeline after follow",
		zap.Uint64("followerID", followerID),
		zap.Uint64("authorID", authorID),
		zap.Int("posts", len(items)),
	)
	return nil
}

// OnUnfollow 在用户取关某作者后，清理该作者遗留在其收件箱中的帖子。
//
// WHY 必须清理：
//
//	写扩散把帖子**复制**进了粉丝收件箱，取关并不会让这些副本消失。
//	若不清理，用户取关后仍会在信息流里持续看到对方的历史内容——
//	这是纯写扩散方案最容易被忽略、也最容易被用户投诉的一环。
//
// 实现方式：从作者发件箱取出其近期帖子 ID，批量 ZREM 出收件箱。
// 这也是「发件箱对所有作者都写」的另一个理由——它是这里唯一可用的清理依据。
//
// 已知边界：只能清掉发件箱仍保留的那部分（最多 AuthorBoxMaxItems 条）。
// 更早的帖子会随收件箱的长度裁剪（TimelineMaxItems）自然淘汰。
func (s *Service) OnUnfollow(ctx context.Context, followerID, authorID uint64) error {
	if s == nil || s.redisClient == nil {
		return nil
	}

	members, err := s.redisClient.ZRevRange(ctx, authorBoxKey(authorID), 0, -1).Result()
	if err != nil {
		return fmt.Errorf("fanout: read author box for cleanup: %w", err)
	}
	if len(members) == 0 {
		return nil
	}

	values := make([]any, len(members))
	for i, m := range members {
		values[i] = m
	}
	if err := s.redisClient.ZRem(ctx, timelineKey(followerID), values...).Err(); err != nil {
		return fmt.Errorf("fanout: remove author posts from timeline: %w", err)
	}

	s.logger.Debug("cleaned timeline after unfollow",
		zap.Uint64("followerID", followerID),
		zap.Uint64("authorID", authorID),
		zap.Int("removed", len(members)),
	)
	return nil
}

// 编译期断言：Service 满足关系模块所需的钩子形状。
var _ interface {
	OnFollow(ctx context.Context, followerID, authorID uint64) error
	OnUnfollow(ctx context.Context, followerID, authorID uint64) error
} = (*Service)(nil)
