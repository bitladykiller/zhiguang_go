// Package relation 实现社交关系图能力：
// 包括关注/取关、基于令牌桶的限流、基于事务 outbox 的异步扩散
// （粉丝反向索引表与缓存失效），以及多级缓存
// （BigV 用户使用 L1 freecache，L2 为 Redis ZSet，L3 为 MySQL）。
//
// 数据结构说明：
//   - following（正向索引表）：from_user_id → to_user_id，表示关注关系。
//   - follower（反向索引表）：to_user_id → from_user_id，表示粉丝关系。
//     两者是同一关注操作的双向记录，通过事务确保同时写入。
//
// WHY 需要反向索引表：因为「查询某个用户的粉丝列表」和「查询某个用户关注了谁」
// 是两种访问模式，如果只用一张 following 表来实现粉丝查询（WHERE to_user_id = ?），
// 在 to_user_id 上建索引后性能尚可，但考虑到未来可能需要新增基于时间的排序、
// 过滤等复杂查询，使用独立的反向索引表更灵活。
package relation

import "time"

// Following 表示 following 表中的一行数据，即关注关系的正向索引。
type Following struct {
	ID         uint64    `db:"id" json:"id"`
	FromUserID uint64    `db:"from_user_id" json:"from_user_id"`
	ToUserID   uint64    `db:"to_user_id" json:"to_user_id"`
	RelStatus  int       `db:"rel_status" json:"rel_status"` // 1=有效，0=已取消
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

// Follower 表示 follower 表中的一行数据，即关注关系的反向索引。
type Follower struct {
	ID         uint64    `db:"id" json:"id"`
	ToUserID   uint64    `db:"to_user_id" json:"to_user_id"`
	FromUserID uint64    `db:"from_user_id" json:"from_user_id"`
	RelStatus  int       `db:"rel_status" json:"rel_status"` // 1=有效，0=已取消
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

// Outbox 是多个模块共用的事务消息外发表。
// 当知文、关系等模块需要将变更异步同步到搜索索引或缓存时，
// 会在同一个事务内写入 outbox 表，随后由 Canal 捕获并投递到 Kafka。
type Outbox struct {
	ID            uint64    `db:"id" json:"id"`
	AggregateType string    `db:"aggregate_type" json:"aggregate_type"`
	AggregateID   *uint64   `db:"aggregate_id" json:"aggregate_id,omitempty"`
	Type          string    `db:"type" json:"type"`       // 事件类型标识
	Payload       string    `db:"payload" json:"payload"` // 事件内容的 JSON 序列化
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}

// RelationEvent 是写入 Outbox 行中的 JSON 事件载荷（Payload）。
// 目前支持的事件类型：
//   - FollowCreated：新的关注关系创建
//   - FollowCanceled：关注关系取消
type RelationEvent struct {
	EventType  string  `json:"event_type"`
	FromUserID uint64  `json:"from_user_id"`
	ToUserID   uint64  `json:"to_user_id"`
	RelationID *uint64 `json:"relation_id,omitempty"`
}

// FollowingRow 与 FollowerRow 是列表查询使用的轻量投影结构。
type FollowingRow struct {
	ID         uint64    `db:"id" json:"id"`
	FromUserID uint64    `db:"from_user_id" json:"from_user_id"`
	ToUserID   uint64    `db:"to_user_id" json:"to_user_id"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}

type FollowerRow struct {
	ID         uint64    `db:"id" json:"id"`
	ToUserID   uint64    `db:"to_user_id" json:"to_user_id"`
	FromUserID uint64    `db:"from_user_id" json:"from_user_id"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
}
