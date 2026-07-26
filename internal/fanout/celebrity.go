package fanout

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// FollowerCounter 提供粉丝数快查能力（由 counter 模块的用户维度计数实现）。
type FollowerCounter interface {
	// FollowerCount 返回用户的粉丝数。
	// 计数缺失或不可用时返回 (0, false)，调用方据此走不依赖计数的慢路径。
	FollowerCount(ctx context.Context, userID uint64) (int64, bool)
}

// CelebrityRegistry 维护「大 V 名单」及其判定逻辑。
//
// WHY 独立成类型，而不是挂在 Service 上：
//
//	写路径（Service）与读路径（TimelineReader）都需要判定大 V——
//	此前 Reader 为了借用一个查询方法而持有整个 *Service，
//	读路径由此假性依赖了写路径的全部依赖（粉丝列表、推送批量等）。
//	名单本是两者共享的独立概念，独立后依赖图回到真实形状：
//	Service → Registry ← Reader。
type CelebrityRegistry struct {
	redis     redis.UniversalClient
	counter   FollowerCounter // 可为 nil：判定完全交给写路径的「边推边数」
	threshold int
	logger    *zap.Logger
}

// NewCelebrityRegistry 创建大 V 名单。
func NewCelebrityRegistry(redisClient redis.UniversalClient, counter FollowerCounter, threshold int, logger *zap.Logger) *CelebrityRegistry {
	if logger == nil {
		logger = zap.NewNop()
	}
	if threshold <= 0 {
		threshold = DefaultConfig().CelebrityThreshold
	}
	return &CelebrityRegistry{redis: redisClient, counter: counter, threshold: threshold, logger: logger}
}

// demoteRatio 是惰性降级的滞回系数：粉丝数跌破 threshold×0.8 才移出名单。
//
// 滞回的意义：阈值附近抖动的账号若立即双向切换，会在「推」「拉」两种模式间
// 反复横跳——每次切回推模式都要补一轮全量扩散。80% 的缓冲带把切换频率压到可忽略。
const demoteRatioPercent = 80

// IsCelebrity 判定作者是否应走「拉」模式。
//
// 判定顺序（含**惰性降级**）：
//
//  1. 名单命中 → 若粉丝计数可信且已跌破滞回线，当场移出名单（解决「名单只进不出」：
//     掉粉后无需任何离线任务，下一次发帖判定时自动回到推模式）；否则确认为大 V。
//  2. 名单未命中 → 计数快路判定；计数不可用返回 unknown，
//     由写路径「边推边数」兜底（不依赖任何外部状态的正确性，自我修正）。
func (r *CelebrityRegistry) IsCelebrity(ctx context.Context, authorID uint64) (celebrity bool, known bool) {
	if r == nil || r.redis == nil {
		return false, false
	}

	if member, err := r.redis.SIsMember(ctx, celebritySetKey, authorID).Result(); err == nil && member {
		if r.counter != nil {
			if count, ok := r.counter.FollowerCount(ctx, authorID); ok && count < int64(r.threshold*demoteRatioPercent/100) {
				r.demote(ctx, authorID, count)
				return false, true
			}
		}
		return true, true
	}

	if r.counter == nil {
		return false, false
	}
	count, ok := r.counter.FollowerCount(ctx, authorID)
	if !ok {
		return false, false
	}
	return count >= int64(r.threshold), true
}

// Mark 把作者加入大 V 名单（由写路径在触达量越过阈值时调用）。
func (r *CelebrityRegistry) Mark(ctx context.Context, authorID uint64) {
	if r == nil || r.redis == nil {
		return
	}
	if err := r.redis.SAdd(ctx, celebritySetKey, authorID).Err(); err != nil {
		r.logger.Warn("failed to mark author as celebrity",
			zap.Uint64("authorID", authorID), zap.Error(err))
	}
}

// demote 把掉粉的作者移出名单（惰性触发，见 IsCelebrity）。
//
// 已知边界——**降级内容空窗**：该作者大 V 时期发的帖子只存在于发件箱
// （当时刻意不推送）；降级后读者不再拉取其发件箱，这批帖子会从关注流中消失，
// 直到被新帖自然稀释。修复需要在降级时对现存粉丝补一轮回填（O(粉丝数×箱深)，
// 与混合方案要消除的写放大同量级），在"降级是罕见事件"的前提下不值得。
// 诚实取舍：接受空窗，靠 0.8 滞回带把降级频率压到可忽略。
func (r *CelebrityRegistry) demote(ctx context.Context, authorID uint64, count int64) {
	if err := r.redis.SRem(ctx, celebritySetKey, authorID).Err(); err != nil {
		r.logger.Warn("failed to demote celebrity", zap.Uint64("authorID", authorID), zap.Error(err))
		return
	}
	r.logger.Info("celebrity demoted to push mode",
		zap.Uint64("authorID", authorID),
		zap.Int64("followers", count),
		zap.Int("threshold", r.threshold),
	)
}

// Among 从给定的作者列表中筛出大 V（一次 SMISMEMBER 批量判定，保持输入顺序）。
func (r *CelebrityRegistry) Among(ctx context.Context, authorIDs []uint64) ([]uint64, error) {
	if r == nil || r.redis == nil || len(authorIDs) == 0 {
		return nil, nil
	}

	members := make([]any, len(authorIDs))
	for i, id := range authorIDs {
		members[i] = strconv.FormatUint(id, 10)
	}

	flags, err := r.redis.SMIsMember(ctx, celebritySetKey, members...).Result()
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
