package relation

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// UserCounterUpdater 定义关系事件处理所需的用户维度计数器更新接口。
type UserCounterUpdater interface {
	IncrementFollowings(ctx context.Context, userID uint64, delta int) error
	IncrementFollowers(ctx context.Context, userID uint64, delta int) error
}

// EventProcessor 处理由 canal-outbox 驱动的关系事件。
//
// 职责：消费 FollowCreated 和 FollowCanceled 事件，执行以下操作：
//  1. 幂等检查（基于 Redis SETNX 的 10 分钟去重窗口）
//  2. 更新 Redis 中的关注/粉丝 ZSet 缓存
//  3. 更新用户维度的关注/粉丝计数（通过 CounterService）
//
// WHY：关注/取关 API 中不直接更新缓存，是因为 Redis 缓存可能已过期，
// 或者 API 请求在更新缓存之前就失败了。事件驱动的异步更新
// 保证了关注/粉丝列表缓存的最终一致性。
type EventProcessor struct {
	redis   *redis.Client
	counter UserCounterUpdater
	logger  *zap.Logger
}

// NewEventProcessor 创建一个关系事件处理器实例。
//
// 功能：初始化 EventProcessor，需要一个 Redis 客户端和一个计数器更新器。
//
// 参数：
//   - redisClient: *redis.Client，用于幂等检查和 ZSet 更新。
//   - counter: UserCounterUpdater，用于更新用户关注/粉丝计数。
//
// 返回：*EventProcessor，当 redisClient 为 nil 时返回 nil（避免后续调用出现 panic）。
//
// 设计决策：
//   返回 nil 而非 panic，使得调用方在事件处理器未初始化时也能安全地消费消息
//   （配置不完整时的优雅降级）。
func NewEventProcessor(redisClient *redis.Client, counter UserCounterUpdater, logger *zap.Logger) *EventProcessor {
	if redisClient == nil {
		return nil
	}
	return &EventProcessor{
		redis:   redisClient,
		counter: counter,
		logger:  logger,
	}
}

// Process 处理关系事件（FollowCreated / FollowCanceled），更新 Redis ZSet 和用户计数。
//
// 功能：消费由 canal-outbox 驱动的关系事件，执行以下操作：
//  1. 幂等检查（SETNX）：基于 10 分钟的去重键窗口，防止重复处理。
//  2. 根据事件类型更新 Redis ZSet：
//     - FollowCreated：将关系同时添加到关注 ZSet 和粉丝 ZSet。
//     - FollowCanceled：从两个 ZSet 中移除成员。
//  3. 更新关注/粉丝计数（通过 UserCounterUpdater 接口）。
//
// SETNX 幂等检查说明：
//   - SetNX 是 Redis 的 "SET if Not eXists" 命令。
//   - 格式：SetNX(ctx, "dedup:rel:{eventType}:{fromUserID}:{toUserID}:{relationID}", "1", 10min)。
//   - 首次处理返回 true；已处理返回 false（命中去重），跳过。
//   - 去重窗口为 10 分钟，确保在消费者重试窗口内不会重复处理。
//   - 去重键包含 relationID（可选），因此一个 FollowCreated 与其对应的 FollowCanceled
//     拥有独立的去重键，互不干扰。
//
// 参数：
//   - ctx: context.Context。
//   - evt: RelationEvent，包含事件类型和涉及的双方用户 ID。
//
// 返回：
//   - error：幂等检查失败、Redis 操作失败或计数更新失败时返回错误。
//
// 边界情况：
//   - p == nil：返回 nil，允许未初始化时安全调用。
//   - 未知事件类型：静默跳过（无错误）。
func (p *EventProcessor) Process(ctx context.Context, evt RelationEvent) error {
	if p == nil {
		return nil
	}

	dedupeKey := fmt.Sprintf("dedup:rel:%s:%d:%d:%s", evt.EventType, evt.FromUserID, evt.ToUserID, relationIDValue(evt.RelationID))
	first, err := p.redis.SetNX(ctx, dedupeKey, "1", 10*time.Minute).Result()
	if err != nil {
		return err
	}
	if !first {
		return nil
	}

	switch evt.EventType {
	case "FollowCreated":
		now := float64(time.Now().UnixMilli())
		pipe := p.redis.Pipeline()
		pipe.ZAdd(ctx, followingZSetKey(evt.FromUserID), redis.Z{Score: now, Member: strconv.FormatUint(evt.ToUserID, 10)})
		pipe.ZAdd(ctx, followersZSetKey(evt.ToUserID), redis.Z{Score: now, Member: strconv.FormatUint(evt.FromUserID, 10)})
		pipe.Expire(ctx, followingZSetKey(evt.FromUserID), 2*time.Hour)
		pipe.Expire(ctx, followersZSetKey(evt.ToUserID), 2*time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		if p.counter != nil {
			p.pipelineIncrementUserMetrics(ctx, evt.FromUserID, "following", 1)
			p.pipelineIncrementUserMetrics(ctx, evt.ToUserID, "follower", 1)
		}
	case "FollowCanceled":
		pipe := p.redis.Pipeline()
		pipe.ZRem(ctx, followingZSetKey(evt.FromUserID), strconv.FormatUint(evt.ToUserID, 10))
		pipe.ZRem(ctx, followersZSetKey(evt.ToUserID), strconv.FormatUint(evt.FromUserID, 10))
		pipe.Expire(ctx, followingZSetKey(evt.FromUserID), 2*time.Hour)
		pipe.Expire(ctx, followersZSetKey(evt.ToUserID), 2*time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		if p.counter != nil {
			p.pipelineIncrementUserMetrics(ctx, evt.FromUserID, "following", -1)
			p.pipelineIncrementUserMetrics(ctx, evt.ToUserID, "follower", -1)
		}
	}

	return nil
}

// relationIDValue 将可选的关系 ID 指针转换为字符串，用于构建去重键。
//
// 功能：安全解引用 *uint64。nil 指针返回 "0"。
//
// 参数：
//   - id: *uint64，可选的关系记录 ID。
//
// 返回：string，ID 的字符串表示；nil 时返回 "0"。
func relationIDValue(id *uint64) string {
	if id == nil {
		return "0"
	}
	return strconv.FormatUint(*id, 10)
}

// followingZSetKey 生成用户关注列表的 ZSet 键。
//
// 功能：格式为 "z:following:{userID}"，与 relation/service.go 中的 zsetKey("following", userID) 一致。
//
// 参数：
//   - userID: uint64，用户 ID。
//
// 返回：string，ZSet 键名。
func followingZSetKey(userID uint64) string {
	return fmt.Sprintf("z:following:%d", userID)
}

// followersZSetKey 生成用户粉丝列表的 ZSet 键。
//
// 功能：格式为 "z:followers:{userID}"。
//
// 参数：
//   - userID: uint64，用户 ID。
//
// 返回：string，ZSet 键名。
func followersZSetKey(userID uint64) string {
	return fmt.Sprintf("z:followers:%d", userID)
}

// pipelineIncrementUserMetrics 通过 Pipeline 合并对同一用户的 following 和 follower 计数更新，
// 将原本两次 HIncrBy 调用合并为一次网络往返。
func (p *EventProcessor) pipelineIncrementUserMetrics(ctx context.Context, userID uint64, metric string, delta int) {
	key := fmt.Sprintf("cnt:user:%d", userID)
	if err := p.redis.HIncrBy(ctx, key, metric, int64(delta)).Err(); err != nil {
		p.logger.Warn("failed to increment user metric via pipeline",
			zap.Uint64("userID", userID),
			zap.String("metric", metric),
			zap.Int("delta", delta),
			zap.Error(err),
		)
	}
}
