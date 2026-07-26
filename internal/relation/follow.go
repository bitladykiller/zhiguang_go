package relation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/zhiguang/app/internal/outbox"
)

// Follow 创建关注关系。
func (s *RelationService) Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	rlKey := fmt.Sprintf("rl:follow:%d", fromUserID)
	capacity, rate := s.tokenBucketParams()
	allowed, err := s.redis.Eval(ctx, tokenBucketLua, []string{rlKey}, capacity, rate).Int()
	if err != nil {
		s.logger.Warn("token bucket eval failed", zap.String("key", rlKey), zap.Error(err))
		return false, nil
	}
	if allowed == 0 {
		return false, nil
	}

	id := s.idGen.NextID()
	reverseID := s.idGen.NextID()
	outboxID := s.idGen.NextID()

	event := RelationEvent{EventType: "FollowCreated", FromUserID: fromUserID, ToUserID: toUserID, RelationID: &id}
	raw, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("marshal follow event: %w", err)
	}

	// 事件只在「真实激活迁移」时发出（首次关注或取关后复关注）。
	// 重复 Follow 是幂等成功：关系行不变、不写 outbox——否则每次都会产生
	// 带新 RelationID 的 FollowCreated，消费端去重无法拦截，关注数被重复累加。
	// 迁移判定必须在事务内完成，因此事件改为条件传入。
	transitioned := false
	if err := outbox.RunInTx(ctx, s.db, func(tx *sqlx.Tx) error {
		txRepo := s.repo.WithDB(tx)
		var err error
		if transitioned, err = txRepo.UpsertFollowing(ctx, id, fromUserID, toUserID, 1); err != nil {
			return err
		}
		if _, err = txRepo.UpsertFollower(ctx, reverseID, toUserID, fromUserID, 1); err != nil {
			return err
		}
		if !transitioned {
			return errNothingToFollow // 借错误通道让 RunInTx 回滚空事务并跳过事件写入
		}
		return nil
	}, []outbox.OutboxEvent{{
		ID:            outboxID,
		AggregateType: "following",
		AggregateID:   &id,
		EventType:     "FollowCreated",
		Payload:       json.RawMessage(raw),
	}}); err != nil {
		if errors.Is(err, errNothingToFollow) {
			return false, nil // 已处于关注状态：幂等成功，无事件、无回填、无审计
		}
		return false, fmt.Errorf("follow: run tx: %w", err)
	}

	s.invalidateCaches(ctx, fromUserID, toUserID)
	s.notifyFanoutFollow(ctx, fromUserID, toUserID)

	if s.auditLog != nil {
		s.auditLog.LogAction(ctx, "follow", int64(fromUserID), "relation", strconv.FormatUint(id, 10), fmt.Sprintf("follow user %d", toUserID))
	}
	return true, nil
}

// notifyFanoutFollow 关注成功后回填信息流，失败只告警不影响关注本身。
//
// 回填是体验优化而非正确性要求：即便失败，对方下次发帖仍会正常推送过来。
func (s *RelationService) notifyFanoutFollow(ctx context.Context, followerID, authorID uint64) {
	if s.fanoutHooks == nil {
		return
	}
	if err := s.fanoutHooks.OnFollow(ctx, followerID, authorID); err != nil {
		s.logger.Warn("fanout backfill after follow failed",
			zap.Uint64("followerID", followerID), zap.Uint64("authorID", authorID), zap.Error(err))
	}
}

// notifyFanoutUnfollow 取关成功后清理信息流中该作者的遗留帖子。
//
// 失败只告警不影响取关本身，但**会导致用户继续看到已取关作者的内容**，
// 因此该告警需要纳入监控。
func (s *RelationService) notifyFanoutUnfollow(ctx context.Context, followerID, authorID uint64) {
	if s.fanoutHooks == nil {
		return
	}
	if err := s.fanoutHooks.OnUnfollow(ctx, followerID, authorID); err != nil {
		s.logger.Warn("fanout cleanup after unfollow failed; the reader may keep seeing this author",
			zap.Uint64("followerID", followerID), zap.Uint64("authorID", authorID), zap.Error(err))
	}
}

// Unfollow 取消关注关系，在同一事务中写入 outbox 事件。
func (s *RelationService) Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	outboxID := s.idGen.NextID()
	event := RelationEvent{EventType: "FollowCanceled", FromUserID: fromUserID, ToUserID: toUserID}
	raw, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("marshal unfollow event: %w", err)
	}

	txErr := outbox.RunInTx(ctx, s.db, func(tx *sqlx.Tx) error {
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
			s.logger.Warn("unfollow: CancelFollower affected 0 rows, possible data inconsistency",
				zap.Uint64("fromUserID", fromUserID), zap.Uint64("toUserID", toUserID))
		}
		return nil
	}, []outbox.OutboxEvent{{
		ID:            outboxID,
		AggregateType: "following",
		AggregateID:   nil,
		EventType:     "FollowCanceled",
		Payload:       json.RawMessage(raw),
	}})
	if txErr != nil {
		if errors.Is(txErr, errNothingToCancel) {
			return false, nil
		}
		return false, fmt.Errorf("unfollow: run tx: %w", txErr)
	}

	s.invalidateCaches(ctx, fromUserID, toUserID)
	s.notifyFanoutUnfollow(ctx, fromUserID, toUserID)

	if s.auditLog != nil {
		s.auditLog.LogAction(ctx, "unfollow", int64(fromUserID), "relation", strconv.FormatUint(outboxID, 10), fmt.Sprintf("unfollow user %d", toUserID))
	}
	return true, nil
}
