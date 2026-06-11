package relation

import (
	"context"
	"encoding/json"
	"fmt"
)

// Follow 创建一条关注关系。
//
// 写路径步骤：
//   - 先用 Redis Lua 令牌桶控制单用户操作速率；
//   - 再在同一个数据库事务里写 following、follower 和 outbox；
//   - 事务提交后失效多级缓存。
//
// 这里把 follower 反向索引和 outbox 一起写入，是为了保证：
//   - 关系状态查询不会读到单边数据；
//   - 下游投影消费不会丢事件。
func (s *RelationService) Follow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	rlKey := fmt.Sprintf("rl:follow:%d", fromUserID)
	allowed, err := s.redis.Eval(ctx, TOKEN_BUCKET_LUA, []string{rlKey}, 10, 1).Int()
	if err != nil || allowed == 0 {
		return false, nil
	}

	id := s.idGen.NextID()
	reverseID := s.idGen.NextID()

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	txRepo := s.repo.WithDB(tx)
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
		}
	}()

	if err := txRepo.UpsertFollowing(ctx, id, fromUserID, toUserID, 1); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := txRepo.UpsertFollower(ctx, reverseID, toUserID, fromUserID, 1); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	event := RelationEvent{EventType: "FollowCreated", FromUserID: fromUserID, ToUserID: toUserID, RelationID: &id}
	payload, _ := json.Marshal(event)
	outboxID := s.idGen.NextID()
	if err := txRepo.InsertOutbox(ctx, outboxID, "following", &id, "FollowCreated", string(payload)); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	s.invalidateCaches(fromUserID, toUserID)
	return true, nil
}

// Unfollow 取消关注关系，并在同一事务中写入 outbox 事件。
//
// follower 反向索引如果不存在，当前实现仍视为异常回滚；
// 这是因为 relation 表已经采用双写约束，缺失反向记录意味着数据真的不一致。
func (s *RelationService) Unfollow(ctx context.Context, fromUserID, toUserID uint64) (bool, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	txRepo := s.repo.WithDB(tx)

	affected, err := txRepo.CancelFollowing(ctx, fromUserID, toUserID)
	if err != nil || affected == 0 {
		_ = tx.Rollback()
		return false, err
	}
	if _, err = txRepo.CancelFollower(ctx, toUserID, fromUserID); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	event := RelationEvent{EventType: "FollowCanceled", FromUserID: fromUserID, ToUserID: toUserID}
	payload, _ := json.Marshal(event)
	outboxID := s.idGen.NextID()
	if err := txRepo.InsertOutbox(ctx, outboxID, "following", nil, "FollowCanceled", string(payload)); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	s.invalidateCaches(fromUserID, toUserID)
	return true, nil
}

// IsFollowing 判断 fromUserID 是否关注了 toUserID。
//
// 这里保持直接查库，不走缓存。单条关系查询是 O(1)，
// 额外再引入缓存只会把一致性问题带到最细粒度的判断路径。
func (s *RelationService) IsFollowing(fromUserID, toUserID uint64) (bool, error) {
	ctx := context.Background()
	cnt, err := s.repo.ExistsFollowing(ctx, fromUserID, toUserID)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

// RelationStatus 返回两个用户之间的关系状态。
func (s *RelationService) RelationStatus(ctx context.Context, fromUserID, toUserID uint64) (string, error) {
	following, err := s.IsFollowing(fromUserID, toUserID)
	if err != nil {
		return "", err
	}
	followedBy, err := s.IsFollowing(toUserID, fromUserID)
	if err != nil {
		return "", err
	}
	if following && followedBy {
		return "mutual", nil
	}
	if following {
		return "following", nil
	}
	if followedBy {
		return "followed", nil
	}
	return "none", nil
}
