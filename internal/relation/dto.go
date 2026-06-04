package relation

// FollowRequest 是关注与取关接口的请求体。
type FollowRequest struct {
	ToUserID uint64 `json:"to_user_id" binding:"required"`
}
