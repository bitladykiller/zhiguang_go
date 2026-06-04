package relation

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// UserCounterUpdater 定义关系事件处理所需的用户维度计数更新接口。
type UserCounterUpdater interface {
	IncrementFollowings(ctx context.Context, userID uint64, delta int) error
	IncrementFollowers(ctx context.Context, userID uint64, delta int) error
}

// EventProcessor 处理由 canal-outbox 驱动的关系事件。
//
// 负责消费 FollowCreated 和 FollowCanceled 事件，并执行以下操作：
//  1. 幂等校验（基于 Redis SETNX 的 10 分钟去重窗口）
//  2. 更新 Redis 中的关注/粉丝 ZSet 缓存
//  3. 更新用户维度的关注/粉丝计数（通过 CounterService）
//
// WHY：不直接在关注/取关 API 中更新缓存，是因为 Redis 缓存可能已过期，
// 或者 API 请求可能在缓存更新前就失败了。通过事件驱动的异步更新，
// 可以使关注/粉丝列表的缓存最终一致。
type EventProcessor struct {
	redis   *redis.Client
	counter UserCounterUpdater
}

// NewEventProcessor 创建关系事件处理器实例。
//
// 功能：初始化 EventProcessor，需要 Redis 客户端和计数更新器。
//
// 参数：
//   - redisClient: *redis.Client，用于幂等校验和 ZSet 更新。
//   - counter: UserCounterUpdater，用于更新用户的关注/粉丝计数。
//
// 返回值：*EventProcessor，redisClient 为 nil 时返回 nil（避免后续调用报错）。
//
// 设计决策：
//   返回 nil 而非 panic，使调用方可以在事件处理器未初始化时安全地消费消息
// （在配置不完整时优雅降级）。
func NewEventProcessor(redisClient *redis.Client, counter UserCounterUpdater) *EventProcessor {
	if redisClient == nil {
		return nil
	}
	return &EventProcessor{
		redis:   redisClient,
		counter: counter,
	}
}

// Process 处理关系事件（FollowCreated / FollowCanceled），更新 Redis ZSet 和用户计数。
//
// 功能：消费由 canal-outbox 驱动的关系事件，执行以下操作：
//  1. 幂等校验（SETNX）：基于去重键的 10 分钟窗口，防止重复处理。
//  2. 根据事件类型更新 Redis ZSet：
//     - FollowCreated：将关系加入 following ZSet 和 followers ZSet。
//     - FollowCanceled：从两个 ZSet 中移除对应成员。
//  3. 更新关注/粉丝计数（通过 UserCounterUpdater 接口）。
//
// SETNX 幂等校验说明：
//   - SetNX 是 Redis 的"SET if Not eXists"命令。
//   - 格式：SetNX(ctx, "dedup:rel:{eventType}:{fromUserID}:{toUserID}:{relationID}", "1", 10min)。
//   - 返回 true 表示第一次处理该事件；false 表示已处理过（去重命中），跳过。
//   - 去重窗口 10 分钟，确保在消费重试窗口期内不会被重复处理。
//   - 去重键包含 relationID（可选），使得正常情况下 FollowCreated 和相关的 FollowCanceled
//     有独立的去重键，不会互相影响。
//
// 参数：
//   - ctx: context.Context。
//   - evt: RelationEvent，包含事件类型和涉及的两个用户 ID。
//
// 返回值：
//   - error: 幂等校验失败、Redis 操作失败或计数更新失败时返回错误。
//
// 边界情况：
//   - p == nil：返回 nil，允许在未初始化时安全调用。
//   - 未知事件类型：静默跳过（不报错）。
func (p *EventProcessor) Process(ctx context.Context, evt RelationEvent) error {
	if p == nil {
		return nil
	}

	dedupeKey := fmt.Sprintf("dedup:rel:%s:%d:%d:%s", evt.EventType, evt.FromUserID, evt.ToUserID, relationIDValue(evt.RelationID))
	first, err := p.redis.SetNX(ctx, dedupeKey, "1", 10*time.Minute).Result()
	if err != nil || !first {
		return err
	}

	switch evt.EventType {
	case "FollowCreated":
		now := float64(time.Now().UnixMilli())
		if err := p.redis.ZAdd(ctx, followingZSetKey(evt.FromUserID), redis.Z{Score: now, Member: strconv.FormatUint(evt.ToUserID, 10)}).Err(); err != nil {
			return err
		}
		if err := p.redis.ZAdd(ctx, followersZSetKey(evt.ToUserID), redis.Z{Score: now, Member: strconv.FormatUint(evt.FromUserID, 10)}).Err(); err != nil {
			return err
		}
		p.redis.Expire(ctx, followingZSetKey(evt.FromUserID), 2*time.Hour)
		p.redis.Expire(ctx, followersZSetKey(evt.ToUserID), 2*time.Hour)
		if p.counter != nil {
			if err := p.counter.IncrementFollowings(ctx, evt.FromUserID, 1); err != nil {
				return err
			}
			if err := p.counter.IncrementFollowers(ctx, evt.ToUserID, 1); err != nil {
				return err
			}
		}
	case "FollowCanceled":
		if err := p.redis.ZRem(ctx, followingZSetKey(evt.FromUserID), strconv.FormatUint(evt.ToUserID, 10)).Err(); err != nil {
			return err
		}
		if err := p.redis.ZRem(ctx, followersZSetKey(evt.ToUserID), strconv.FormatUint(evt.FromUserID, 10)).Err(); err != nil {
			return err
		}
		p.redis.Expire(ctx, followingZSetKey(evt.FromUserID), 2*time.Hour)
		p.redis.Expire(ctx, followersZSetKey(evt.ToUserID), 2*time.Hour)
		if p.counter != nil {
			if err := p.counter.IncrementFollowings(ctx, evt.FromUserID, -1); err != nil {
				return err
			}
			if err := p.counter.IncrementFollowers(ctx, evt.ToUserID, -1); err != nil {
				return err
			}
		}
	}

	return nil
}

// relationIDValue 将可选的 relation ID 指针转换为字符串，用于去重键构建。
//
// 功能：*uint64 类型的安全解引用。nil 指针返回 "0"。
//
// 参数：
//   - id: *uint64，可选的关系记录 ID。
//
// 返回值：string，ID 的字符串表示；nil 返回 "0"。
func relationIDValue(id *uint64) string {
	if id == nil {
		return "0"
	}
	return strconv.FormatUint(*id, 10)
}

// followingZSetKey 生成用户关注列表的 ZSet 键。
//
// 功能：格式为 "z:following:{userID}"，与 relation/service.go 中的 zsetKey("following", userID) 保持一致。
//
// 参数：
//   - userID: uint64，用户 ID。
//
// 返回值：string，ZSet 键名。
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
// 返回值：string，ZSet 键名。
func followersZSetKey(userID uint64) string {
	return fmt.Sprintf("z:followers:%d", userID)
}
