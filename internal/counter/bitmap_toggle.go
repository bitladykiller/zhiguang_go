// Package counter — 开关操作（位图 toggle + Kafka 事件发布）。
//
// Like / Unlike / Fav / Unfav 委托给 toggle() 统一处理。
// toggle() 执行原子 Lua 脚本切换位图，并在状态变化时异步发布 Kafka 事件。
//
// 用户维度的 IncrementFollowings / IncrementFollowers 已迁移到 user_counter.go。
package counter

import (
	"context"
	"fmt"
)

// Like 为指定用户对指定实体打开点赞状态。
func (s *CounterService) Like(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "add")
}

// Unlike 为指定用户取消对指定实体的点赞状态。
func (s *CounterService) Unlike(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "like", "remove")
}

// Fav 为指定用户对指定实体打开收藏状态。
func (s *CounterService) Fav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "add")
}

// Unfav 为指定用户取消对指定实体的收藏状态。
func (s *CounterService) Unfav(ctx context.Context, userID uint64, entityType, entityID string) (bool, error) {
	return s.toggle(ctx, userID, entityType, entityID, "fav", "remove")
}

// toggle 执行原子 Lua 脚本；如果状态发生变化，则发布 CounterEvent 到 Kafka。
func (s *CounterService) toggle(ctx context.Context, userID uint64, entityType, entityID, metric, op string) (bool, error) {
	chunk := ChunkOf(userID)
	offset := BitOf(userID)
	bmKey := BitmapKey(metric, entityType, entityID, chunk)

	changed, err := s.redis.Eval(ctx, TOGGLE_LUA, []string{bmKey}, offset, op).Int()
	if err != nil {
		return false, fmt.Errorf("lua toggle: %w", err)
	}

	if changed == 1 {
		delta := 1
		if op == "remove" {
			delta = -1
		}
		event := &CounterEvent{
			MessageID:  s.nextMessageID(),
			EntityType: entityType,
			EntityID:   entityID,
			Metric:     metric,
			Index:      nameToIdx[metric],
			UserID:     userID,
			Delta:      delta,
		}
		if s.producer != nil {
			pubCtx, cancel := context.WithTimeout(ctx, s.publishTimeout)
			defer cancel()
			s.publishCounterEvent(pubCtx, event)
		}
		return true, nil
	}
	return false, nil
}
