package model

import "time"

// User 映射到 users 表，是 auth 和 profile 两模块共享的用户模型。
type User struct {
	ID           uint64     `db:"id" json:"id"`
	ZgID         *string    `db:"zg_id" json:"zg_id,omitempty"`
	Phone        *string    `db:"phone" json:"phone,omitempty"`
	Email        *string    `db:"email" json:"email,omitempty"`
	PasswordHash *string    `db:"password_hash" json:"-"`
	Nickname     string     `db:"nickname" json:"nickname"`
	Avatar       *string    `db:"avatar" json:"avatar,omitempty"`
	TagsJSON     *string    `db:"tags_json" json:"tags_json,omitempty"`
	Birthday     *time.Time `db:"birthday" json:"birthday,omitempty"`
	Gender       *string    `db:"gender" json:"gender,omitempty"`
	Bio          *string    `db:"bio" json:"bio,omitempty"`
	School       *string    `db:"school" json:"school,omitempty"`
	CreatedAt    time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt    time.Time  `db:"updated_at" json:"updated_at"`
}