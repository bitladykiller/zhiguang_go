package relation

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// Follow 创建一条关注关系。
func (s *RelationService) Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	rlKey := fmt.Sprintf("rl:follow:%d", fromUserID)
	allowed, err := s.redis.Eval(ctx, TOKEN_BUCKET_LUA, []string{rlKey}, 10, 1).Int()
	if err != nil {
		zap.L().Warn("token bucket eval failed", zap.String("key", rlKey), zap.Error(err))
		return false, nil
	}
	if allowed == 0 {
		return false, nil
	}

	id := s.idGen.NextID()
	reverseID := s.idGen.NextID()
	outboxID := s.idGen.NextID()

	event := RelationEvent{EventType: "FollowCreated", FromUserID: fromUserID, ToUserID: toUserID, RelationID: &id}

	if err := outbox.RunInTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRepo := s.repo.WithDB(tx)
		if err := txRepo.UpsertFollowing(ctx, id, fromUserID, toUserID, 1); err != nil {
			return err
		}
		if err := txRepo.UpsertFollower(ctx, reverseID, toUserID, fromUserID, 1); err != nil {
			return err
		}
		return nil
	}, []outbox.OutboxEvent{{
		ID:            outboxID,
		AggregateType: "following",
		AggregateID:   &id,
		EventType:     "FollowCreated",
		Payload:       event,
	}}); err != nil {
		return false, fmt.Errorf("follow: run tx: %w", err)
	}

	s.invalidateCaches(ctx, fromUserID, toUserID)
	return true, nil
}

// Unfollow 取消关注关系，并在同一事务中写入 outbox 事件。
func (s *RelationService) Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	outboxID := s.idGen.NextID()
	event := RelationEvent{EventType: "FollowCanceled", FromUserID: fromUserID, ToUserID: toUserID}

	err := outbox.RunInTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRepo := s.repo.WithDB(tx)
		affected, err := txRepo.CancelFollowing(ctx, fromUserID, toUserID)
		if err != nil {
			return err
		}
		if affected == 0 {
			return errNothingToCancel
		}
		reverseAffected, err := txRepo.CancelFollower(ctx, toUserID, fromUserID)
		if err != nil {
			return err
		}
		if reverseAffected == 0 {
		}
		return nil
	}, []outbox.OutboxEvent{{
		ID:            outboxID,
		AggregateType: "following",
		AggregateID:   nil,
		EventType:     "FollowCanceled",
		Payload:       event,
	}})
	if err != nil {
		if errors.Is(err, errNothingToCancel) {
			return false, nil
		}
		return false, fmt.Errorf("unfollow: run tx: %w", err)
	}

	s.invalidateCaches(ctx, fromUserID, toUserID)
	return true, nil
}