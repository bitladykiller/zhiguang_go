package relation

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// UserCounterUpdater 是关系事件处理对计数模块的最小依赖（消费侧窄接口）。
//
// 计数与去重标记必须原子（见 counter.UserCounter.ApplyFollowDeltaOnce 的注释），
// 因此这里依赖的是「带一次性语义的双向增减」，而不是两个裸的 Increment。
type UserCounterUpdater interface {
	ApplyFollowDeltaOnce(ctx context.Context, dedupeKey string, ttl time.Duration, fromUserID, toUserID uint64, delta int64) (first bool, err error)
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
//
//	返回 nil 而非 panic，使得调用方在事件处理器未初始化时也能安全地消费消息
//	（配置不完整时的优雅降级）。
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

	var delta int64
	switch evt.EventType {
	case "FollowCreated":
		delta = 1
	case "FollowCanceled":
		delta = -1
	default:
		return nil // 未知事件类型：静默跳过
	}

	// ── 幂等协议：先做可重放的操作，最后原子地「落标 + 执行不可重放的操作」 ──
	//
	// 早期实现的顺序是「SetNX 落标 → ZSet → 计数」。在至少一次投递语义下这是错的：
	// 标记落下后任一后续步骤失败，Kafka 重投会命中标记被当成“已处理”跳过——
	// 那次关注在 ZSet 与计数里**永久丢失**。
	//
	// 正确协议按操作性质分两类：
	//   1. ZAdd/ZRem 天然幂等：重放无害，放在前面，失败返回错误由重投兜底。
	//   2. HIncrBy 不幂等：与去重标记合进**同一段 Lua** 原子执行——
	//      要么「标记+计数」都发生，要么都不发生，不存在“标记了但没数”的中间态。
	if err := p.applyZSets(ctx, evt, delta); err != nil {
		return err
	}

	dedupeKey := fmt.Sprintf("dedup:rel:%s:%d:%d:%s", evt.EventType, evt.FromUserID, evt.ToUserID, relationIDValue(evt.RelationID))
	if p.counter == nil {
		// 无计数依赖时仅落标（保持与历史一致的重复投递观测语义）。
		_, err := p.redis.SetNX(ctx, dedupeKey, "1", dedupeTTL).Result()
		return err
	}

	// 「落标 + 双向计数」在 counter 侧以单段 Lua 原子完成；
	// 返回 false 表示重复投递（首次执行时计数已完成），无需处理。
	if _, err := p.counter.ApplyFollowDeltaOnce(ctx, dedupeKey, dedupeTTL, evt.FromUserID, evt.ToUserID, delta); err != nil {
		return fmt.Errorf("relation event mark+count: %w", err)
	}
	return nil
}

// applyZSets 更新关注/粉丝 ZSet 投影（幂等，可安全重放）。
func (p *EventProcessor) applyZSets(ctx context.Context, evt RelationEvent, delta int64) error {
	pipe := p.redis.Pipeline()
	fromKey := followingZSetKey(evt.FromUserID)
	toKey := followersZSetKey(evt.ToUserID)
	if delta > 0 {
		now := float64(time.Now().UnixMilli())
		pipe.ZAdd(ctx, fromKey, redis.Z{Score: now, Member: strconv.FormatUint(evt.ToUserID, 10)})
		pipe.ZAdd(ctx, toKey, redis.Z{Score: now, Member: strconv.FormatUint(evt.FromUserID, 10)})
	} else {
		pipe.ZRem(ctx, fromKey, strconv.FormatUint(evt.ToUserID, 10))
		pipe.ZRem(ctx, toKey, strconv.FormatUint(evt.FromUserID, 10))
	}
	pipe.Expire(ctx, fromKey, 2*time.Hour)
	pipe.Expire(ctx, toKey, 2*time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("relation event zset update: %w", err)
	}
	return nil
}

// dedupeTTL 是去重标记的生存期，覆盖 Kafka 的重投窗口。
const dedupeTTL = 10 * time.Minute

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
