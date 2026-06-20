package model

type FanoutEvent struct {
	PostID    uint64 `json:"post_id"`
	CreatorID uint64 `json:"creator_id"`
	CreatedAt int64  `json:"created_at"`
}