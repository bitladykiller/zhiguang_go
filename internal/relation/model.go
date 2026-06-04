// Package relation 实现社交关系图能力：
// 包括关注/取关、基于令牌桶的限流、基于事务 outbox 的异步扩散
// （粉丝反向索引表与缓存失效），以及多级缓存
// （BigV 使用 L1 freecache，L2 为 Redis ZSet，L3 为 MySQL）。
package relation

import "time"

// Following 表示 following 表中的一行数据。
type Following struct {
	ID         uint64    `db:"id" json:"id"`
	FromUserID uint64    `db:"from_user_id" json:"from_user_id"`
	ToUserID   uint64    `db:"to_user_id" json:"to_user_id"`
	RelStatus  int       `db:"rel_status" json:"rel_status"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

// Follower 表示 follower 表中的一行数据，也就是关注关系的反向索引。
type Follower struct {
	ID         uint64    `db:"id" json:"id"`
	ToUserID   uint64    `db:"to_user_id" json:"to_user_id"`
	FromUserID uint64    `db:"from_user_id" json:"from_user_id"`
	RelStatus  int       `db:"rel_status" json:"rel_status"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at" json:"updated_at"`
}

// Outbox 是多个模块共用的事务消息外发表。
type Outbox struct {
	ID            uint64    `db:"id" json:"id"`
	AggregateType string    `db:"aggregate_type" json:"aggregate_type"`
	AggregateID   *uint64   `db:"aggregate_id" json:"aggregate_id,omitempty"`
	Type          string    `db:"type" json:"type"`
	Payload       string    `db:"payload" json:"payload"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}

// RelationEvent 是写入 Outbox 行中的 JSON 事件载荷。
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
