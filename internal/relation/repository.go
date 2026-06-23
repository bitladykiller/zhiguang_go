package relation

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// RelationRepository 封装关系域的数据库访问，使用 sqlx.ExtContext 同时支持 DB 和 Tx。
type RelationRepository struct {
	db sqlx.ExtContext
}

// NewRelationRepository 创建 RelationRepository 实例。
//
// 参数:
//   - db: sqlx.ExtContext，支持 *sqlx.DB 或 *sqlx.Tx
//
// 返回值:
//   - *RelationRepository: 已初始化的仓储实例
func NewRelationRepository(db sqlx.ExtContext) *RelationRepository {
	return &RelationRepository{db: db}
}

// WithDB 克隆绑定到指定 sqlx 句柄的仓储实例，用于事务上下文。
func (r *RelationRepository) WithDB(db sqlx.ExtContext) *RelationRepository {
	return &RelationRepository{db: db}
}

// UpsertFollowing INSERT ... ON DUPLICATE KEY UPDATE，使用 ExecContext。
func (r *RelationRepository) UpsertFollowing(ctx context.Context, id, fromUserID, toUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, fromUserID, toUserID, status, now, now)
	return err
}

// UpsertFollower INSERT ... ON DUPLICATE KEY UPDATE，使用 ExecContext。
func (r *RelationRepository) UpsertFollower(ctx context.Context, id, toUserID, fromUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, toUserID, fromUserID, status, now, now)
	return err
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

// ExistsFollowing 判断关注关系是否存在，使用 sqlx.GetContext。
func (r *RelationRepository) ExistsFollowing(ctx context.Context, fromUserID, toUserID uint64) (int, error) {
	var count int
	err := sqlx.GetContext(ctx, r.db, &count, `
SELECT COUNT(1)
FROM following
WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1
`, fromUserID, toUserID)
	return count, err
}

// ListFollowingRows 分页查询关注列表，使用 sqlx.SelectContext。
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

// ListFollowerRows 分页查询粉丝列表，使用 sqlx.SelectContext。
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

// ListFollowerRowsFromFollowing 从 following 表降级查询粉丝（向后兼容旧数据），使用 sqlx.SelectContext。
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
