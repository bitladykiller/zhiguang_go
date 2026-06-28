package relation

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// RelationRepository encapsulates database access for the relationship domain, using sqlx.ExtContext to support both DB and Tx.
type RelationRepository struct {
	db sqlx.ExtContext
}

// NewRelationRepository creates a RelationRepository instance.
//
// Parameters:
//   - db: sqlx.ExtContext, supports *sqlx.DB or *sqlx.Tx
//
// Returns:
//   - *RelationRepository: initialized repository instance
func NewRelationRepository(db sqlx.ExtContext) *RelationRepository {
	return &RelationRepository{db: db}
}

// WithDB clones the repository bound to the specified sqlx handle, for use in transactional contexts.
func (r *RelationRepository) WithDB(db sqlx.ExtContext) *RelationRepository {
	return &RelationRepository{db: db}
}

// UpsertFollowing INSERT ... ON DUPLICATE KEY UPDATE, using ExecContext.
func (r *RelationRepository) UpsertFollowing(ctx context.Context, id, fromUserID, toUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, fromUserID, toUserID, status, now, now)
	return err
}

// UpsertFollower INSERT ... ON DUPLICATE KEY UPDATE, using ExecContext.
func (r *RelationRepository) UpsertFollower(ctx context.Context, id, toUserID, fromUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, toUserID, fromUserID, status, now, now)
	return err
}

// CancelFollowing cancels the forward follow (rel_status → 0), using ExecContext.
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

// CancelFollower cancels the reverse follow, using ExecContext.
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

// ExistsFollowing checks whether a follow relationship exists, using sqlx.GetContext.
func (r *RelationRepository) ExistsFollowing(ctx context.Context, fromUserID, toUserID uint64) (int, error) {
	var count int
	err := sqlx.GetContext(ctx, r.db, &count, `
SELECT COUNT(1)
FROM following
WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1
`, fromUserID, toUserID)
	return count, err
}

// ListFollowingRows queries the following list with pagination, using sqlx.SelectContext.
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

// ListFollowerRows queries the follower list with pagination, using sqlx.SelectContext.
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

// ListFollowerRowsFromFollowing queries followers from the following table as a fallback (backward compatibility for old data), using sqlx.SelectContext.
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
