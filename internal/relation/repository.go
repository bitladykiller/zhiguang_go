package relation

import (
	"math"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

// RelationRepository 封装关系域的数据库访问操作。
type RelationRepository struct {
	db sqlx.Ext
}

func NewRelationRepository(db *sqlx.DB) *RelationRepository {
	return &RelationRepository{db: db}
}

// WithDB 基于指定 DB 句柄克隆一个仓储实例，常用于事务上下文。
func (r *RelationRepository) WithDB(db sqlx.Ext) *RelationRepository {
	return &RelationRepository{db: db}
}

func (r *RelationRepository) InsertFollowing(id, fromUserID, toUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.Exec(
		"INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		id, fromUserID, toUserID, status, now, now,
	)
	return err
}

func (r *RelationRepository) InsertFollower(id, toUserID, fromUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.Exec(
		"INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		id, toUserID, fromUserID, status, now, now,
	)
	return err
}

func (r *RelationRepository) UpsertFollowing(id, fromUserID, toUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.Exec(`
INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, fromUserID, toUserID, status, now, now)
	return err
}

func (r *RelationRepository) UpsertFollower(id, toUserID, fromUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.Exec(`
INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, toUserID, fromUserID, status, now, now)
	return err
}

func (r *RelationRepository) CancelFollowing(fromUserID, toUserID uint64) (int64, error) {
	result, err := r.db.Exec(
		"UPDATE following SET rel_status = 0, updated_at = ? WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1",
		time.Now(), fromUserID, toUserID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *RelationRepository) CancelFollower(toUserID, fromUserID uint64) (int64, error) {
	result, err := r.db.Exec(
		"UPDATE follower SET rel_status = 0, updated_at = ? WHERE to_user_id = ? AND from_user_id = ? AND rel_status = 1",
		time.Now(), toUserID, fromUserID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *RelationRepository) ExistsFollowing(fromUserID, toUserID uint64) (int, error) {
	var count int
	err := sqlx.Get(r.db, &count, `
SELECT COUNT(1)
FROM following
WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1
`, fromUserID, toUserID)
	return count, err
}

func (r *RelationRepository) ListFollowingRows(userID uint64, limit, offset int) ([]FollowingRow, error) {
	var rows []FollowingRow
	err := sqlx.Select(r.db, &rows, `
SELECT id, from_user_id, to_user_id, created_at
FROM following
WHERE from_user_id = ? AND rel_status = 1
ORDER BY created_at DESC
LIMIT ? OFFSET ?
`, userID, limit, offset)
	return rows, err
}

func (r *RelationRepository) ListFollowerRows(userID uint64, limit, offset int) ([]FollowerRow, error) {
	var rows []FollowerRow
	err := sqlx.Select(r.db, &rows, `
SELECT id, to_user_id, from_user_id, created_at
FROM follower
WHERE to_user_id = ? AND rel_status = 1
ORDER BY created_at DESC
LIMIT ? OFFSET ?
`, userID, limit, offset)
	return rows, err
}

func (r *RelationRepository) ListFollowerRowsFromFollowing(userID uint64, limit, offset int) ([]FollowerRow, error) {
	var rows []FollowerRow
	err := sqlx.Select(r.db, &rows, `
SELECT id, to_user_id, from_user_id, created_at
FROM following
WHERE to_user_id = ? AND rel_status = 1
ORDER BY created_at DESC
LIMIT ? OFFSET ?
`, userID, limit, offset)
	return rows, err
}

func (r *RelationRepository) InsertOutbox(id uint64, aggType string, aggID *uint64, eventType, payload string) error {
	_, err := r.db.Exec(
		"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
		id, aggType, aggID, eventType, payload,
	)
	return err
}

// NextID 为新行生成一个伪唯一 ID。
func NextID() uint64 {
	// 把 ID 控制在有符号 63 位范围内，
	// 这样 MySQL 与轻量测试驱动（尤其是 SQLite）都能稳定持久化。
	return uint64(rand.Int63n(math.MaxInt64))
}
