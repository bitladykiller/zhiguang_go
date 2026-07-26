package relation

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// RelationRepository 封装关系域的数据访问，使用 sqlx.ExtContext 来同时支持 DB 和 Tx。
type RelationRepository struct {
	db sqlx.ExtContext
}

// NewRelationRepository 创建一个 RelationRepository 实例。
//
// 参数:
//   - db: sqlx.ExtContext，支持 *sqlx.DB 或 *sqlx.Tx
//
// 返回:
//   - *RelationRepository: 初始化后的仓库实例
func NewRelationRepository(db sqlx.ExtContext) *RelationRepository {
	return &RelationRepository{db: db}
}

// WithDB 克隆仓库并绑定到指定的 sqlx 句柄，用于事务上下文。
func (r *RelationRepository) WithDB(db sqlx.ExtContext) *RelationRepository {
	return &RelationRepository{db: db}
}

// UpsertFollowing INSERT ... ON DUPLICATE KEY UPDATE，使用 ExecContext。
// UpsertFollowing 写入/激活正向关系，返回**是否发生激活迁移**（首次插入或 0→1 复关注）。
//
// WHY 需要感知迁移：Follow 是 upsert 语义，重复调用不会多出关系行；
// 但事件必须只在真实迁移时发出——否则每次重复 Follow 都产生带新 RelationID 的
// FollowCreated，消费端去重键含 RelationID 无法拦截，用户关注数被重复 +1（计数漂移）。
//
// 实现依赖 MySQL affected-rows 语义：INSERT=1、UPDATE 且有实际变更=2、无变更=0。
// 关键在 SQL：updated_at 仅在 rel_status 真正变化时才更新——
// 若无条件写 updated_at，重复 Follow 也会 affected=2，迁移判定即失效。
func (r *RelationRepository) UpsertFollowing(ctx context.Context, id, fromUserID, toUserID uint64, status int) (transitioned bool, err error) {
	now := time.Now()
	res, err := r.db.ExecContext(ctx, `
INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    updated_at = IF(rel_status <> VALUES(rel_status), VALUES(updated_at), updated_at),
    rel_status = VALUES(rel_status)
`, id, fromUserID, toUserID, status, now, now)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

// UpsertFollower 写入/激活反向关系，迁移语义同 UpsertFollowing。
func (r *RelationRepository) UpsertFollower(ctx context.Context, id, toUserID, fromUserID uint64, status int) (transitioned bool, err error) {
	now := time.Now()
	res, err := r.db.ExecContext(ctx, `
INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    updated_at = IF(rel_status <> VALUES(rel_status), VALUES(updated_at), updated_at),
    rel_status = VALUES(rel_status)
`, id, toUserID, fromUserID, status, now, now)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

// CancelFollowing 取消正向关注（rel_status → 0），使用 ExecContext。
func (r *RelationRepository) CancelFollowing(ctx context.Context, fromUserID, toUserID uint64) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE following SET rel_status = 0, updated_at = ? WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1",
		time.Now(), fromUserID, toUserID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// CancelFollower 取消反向关注，使用 ExecContext。
func (r *RelationRepository) CancelFollower(ctx context.Context, toUserID, fromUserID uint64) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE follower SET rel_status = 0, updated_at = ? WHERE to_user_id = ? AND from_user_id = ? AND rel_status = 1",
		time.Now(), toUserID, fromUserID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ExistsFollowing 检查关注关系是否存在，使用 sqlx.GetContext。
func (r *RelationRepository) ExistsFollowing(ctx context.Context, fromUserID, toUserID uint64) (int, error) {
	var count int
	err := sqlx.GetContext(ctx, r.db, &count, `
SELECT COUNT(1)
FROM following
WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1
`, fromUserID, toUserID)
	return count, err
}

// ListFollowingRows 查询关注列表并分页，使用 sqlx.SelectContext。
func (r *RelationRepository) ListFollowingRows(ctx context.Context, userID uint64, limit, offset int) ([]FollowingRow, error) {
	var rows []FollowingRow
	err := sqlx.SelectContext(ctx, r.db, &rows, `
SELECT id, from_user_id, to_user_id, created_at
FROM following
WHERE from_user_id = ? AND rel_status = 1
ORDER BY created_at DESC
LIMIT ? OFFSET ?
`, userID, limit, offset)
	return rows, err
}

// ListFollowerRows 查询粉丝列表并分页，使用 sqlx.SelectContext。
func (r *RelationRepository) ListFollowerRows(ctx context.Context, userID uint64, limit, offset int) ([]FollowerRow, error) {
	var rows []FollowerRow
	err := sqlx.SelectContext(ctx, r.db, &rows, `
SELECT id, to_user_id, from_user_id, created_at
FROM follower
WHERE to_user_id = ? AND rel_status = 1
ORDER BY created_at DESC
LIMIT ? OFFSET ?
`, userID, limit, offset)
	return rows, err
}

// ListFollowerRowsFromFollowing 从 following 表查询粉丝作为降级方案（向后兼容旧数据），使用 sqlx.SelectContext。
func (r *RelationRepository) ListFollowerRowsFromFollowing(ctx context.Context, userID uint64, limit, offset int) ([]FollowerRow, error) {
	var rows []FollowerRow
	err := sqlx.SelectContext(ctx, r.db, &rows, `
SELECT id, to_user_id, from_user_id, created_at
FROM following
WHERE to_user_id = ? AND rel_status = 1
ORDER BY created_at DESC
LIMIT ? OFFSET ?
`, userID, limit, offset)
	return rows, err
}
