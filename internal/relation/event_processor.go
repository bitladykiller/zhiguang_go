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

func NewEventProcessor(redisClient *redis.Client, counter UserCounterUpdater) *EventProcessor {
	if redisClient == nil {
		return nil
	}
	return &EventProcessor{
		redis:   redisClient,
		counter: counter,
	}
}

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

func relationIDValue(id *uint64) string {
	if id == nil {
		return "0"
	}
	return strconv.FormatUint(*id, 10)
}

func followingZSetKey(userID uint64) string {
	return fmt.Sprintf("z:following:%d", userID)
}

func followersZSetKey(userID uint64) string {
	return fmt.Sprintf("z:followers:%d", userID)
}
