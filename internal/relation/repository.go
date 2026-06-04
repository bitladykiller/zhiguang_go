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

// UpsertFollowing 插入或更新正向关注记录（following 表）。
//
// 功能：使用 INSERT ... ON DUPLICATE KEY UPDATE 实现在一条 SQL 中的 UPSERT 语义。
// 当 (from_user_id, to_user_id) 唯一键冲突时，更新 rel_status 和 updated_at。
//
// 参数：
//   - id: uint64，记录主键。
//   - fromUserID: uint64，关注者 ID。
//   - toUserID: uint64，被关注者 ID。
//   - status: int，关系状态（1=关注中，0=取消）。
//
// 返回值：
//   - error: 数据库执行失败时的错误。
func (r *RelationRepository) UpsertFollowing(id, fromUserID, toUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.Exec(`
INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, fromUserID, toUserID, status, now, now)
	return err
}

// UpsertFollower 插入或更新反向关注记录（follower 表）。
//
// 功能：与 UpsertFollowing 功能相同，但操作的是 follower 表。
// 使用 UPSERT 语义：INSERT ... ON DUPLICATE KEY UPDATE。
//
// 参数：
//   - id: uint64，记录主键。
//   - toUserID: uint64，被关注者（作为粉丝列表的拥有者）。
//   - fromUserID: uint64，关注者（作为粉丝）。
//   - status: int，关系状态。
//
// 返回值：
//   - error: 数据库执行失败时的错误。
func (r *RelationRepository) UpsertFollower(id, toUserID, fromUserID uint64, status int) error {
	now := time.Now()
	_, err := r.db.Exec(`
INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)
`, id, toUserID, fromUserID, status, now, now)
	return err
}

// CancelFollowing 取消正向关注关系（将 rel_status 设为 0）。
//
// 功能：软删除关注记录。使用 AND rel_status = 1 防止重复取消。
//
// 参数：
//   - fromUserID: uint64，关注者 ID。
//   - toUserID: uint64，被关注者 ID。
//
// 返回值：
//   - int64: 影响的行数（0 表示记录不存在或已取消）。
//   - error: 数据库执行失败时的错误。
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

// CancelFollower 取消反向关注记录（follower 表）。
//
// 功能：与 CancelFollowing 对应，操作 follower 表。使用 AND rel_status = 1 防止重复取消。
//
// 参数：
//   - toUserID: uint64，被关注者 ID（粉丝列表的拥有者）。
//   - fromUserID: uint64，关注者 ID。
//
// 返回值：
//   - int64: 影响的行数。
//   - error: 数据库执行失败时的错误。
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

// ExistsFollowing 查询 fromUserID 是否关注了 toUserID。
//
// 功能：使用 COUNT(1) 检查正向关注记录是否存在且有效（rel_status = 1）。
//
// 参数：
//   - fromUserID: uint64，关注者 ID。
//   - toUserID: uint64，被关注者 ID。
//
// 返回值：
//   - int: COUNT 结果，0 表示未关注，> 0 表示已关注。
//   - error: 查询错误。
func (r *RelationRepository) ExistsFollowing(fromUserID, toUserID uint64) (int, error) {
	var count int
	err := sqlx.Get(r.db, &count, `
SELECT COUNT(1)
FROM following
WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1
`, fromUserID, toUserID)
	return count, err
}

// ListFollowingRows 分页查询某个用户的关注列表。
//
// 功能：查询 from_user_id = userID 的有效（rel_status = 1）关注记录，
// 按 created_at DESC 排序。
//
// 参数：
//   - userID: uint64，目标用户 ID。
//   - limit: int，查询条数上限。
//   - offset: int，偏移量。
//
// 返回值：
//   - []FollowingRow: 关注记录行。
//   - error: 查询错误。
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

// ListFollowerRows 分页查询某个用户的粉丝列表（从 follower 表）。
//
// 功能：查询 to_user_id = userID 的有效粉丝记录，按 created_at DESC 排序。
//
// 参数：
//   - userID: uint64，目标用户 ID。
//   - limit: int，查询条数上限。
//   - offset: int，偏移量。
//
// 返回值：
//   - []FollowerRow: 粉丝记录行。
//   - error: 查询错误。
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

// ListFollowerRowsFromFollowing 从 following 表反向查询粉丝列表（向后兼容）。
//
// 功能：当 follower 表中没有数据时，作为降级策略从 following 表查询。
// 在旧版本数据中，只填充了 following 正向表而没有填充 follower 反向表，
// 此方法可以兼容这些旧数据。
//
// 参数：
//   - userID: uint64，目标用户 ID（作为被关注者，在 following 表的 to_user_id 字段）。
//   - limit: int，查询条数上限。
//   - offset: int，偏移量。
//
// 返回值：
//   - []FollowerRow: 整理后的粉丝记录行（from_user_id 作为粉丝 ID）。
//   - error: 查询错误。
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

// InsertOutbox 在 outbox 表中插入一条关系领域的事件消息。
//
// 功能：Transactional Outbox Pattern 的一部分。
// 在同一个事务中写入业务变更和 outbox 事件，由 Canal 组件消费到 Kafka。
//
// 参数：
//   - id: uint64，outbox 记录的主键（由随机数生成）。
//   - aggType: string，聚合类型，如 "following"。
//   - aggID: *uint64，关联的聚合根 ID（可为 nil，如取关事件可能没有关联 ID）。
//   - eventType: string，事件类型，如 "FollowCreated"、"FollowCanceled"。
//   - payload: string，JSON 序列化的事件载荷。
//
// 返回值：
//   - error: 数据库写入失败时的错误。
func (r *RelationRepository) InsertOutbox(id uint64, aggType string, aggID *uint64, eventType, payload string) error {
	_, err := r.db.Exec(
		"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
		id, aggType, aggID, eventType, payload,
	)
	return err
}

// NextID 为新行生成一个伪唯一的 ID。
//
// 功能：使用随机数生成一个非负的 int64 范围内的 ID，用于 following/follower 表的主键和 outbox 事件 ID。
// 这不是严格唯一的主键（有极小概率碰撞），但对于关系表这种数据量不大的场景已经足够。
//
// WHY 使用随机数而非雪花算法：
//   关系表的写入并发不高，随机 ID 碰撞概率极低。
//   使用随机数可以避免引入雪花算法库的额外依赖。
//
// 返回值：uint64，伪唯一的 63 位整数（最高位固定为 0）。
func NextID() uint64 {
	// 把 ID 控制在有符号 63 位范围内，
	// 这样 MySQL 与轻量测试驱动（尤其是 SQLite）都能稳定持久化。
	return uint64(rand.Int63n(math.MaxInt64))
}
