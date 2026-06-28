package counter

// ToggleRequest 是点赞、取消点赞、收藏、取消收藏接口共用的请求体。
type ToggleRequest struct {
	EntityType string `json:"entity_type" binding:"required"`
	EntityID   string `json:"entity_id" binding:"required"`
}

// LikersResponse 返回指定实体的点赞/收藏用户列表（分页）。
type LikersResponse struct {
	Items   []LikerItem `json:"items"`
	Cursor  uint64      `json:"cursor"`
	HasMore bool        `json:"has_more"`
}

// LikerItem 表示一个点赞/收藏用户。
type LikerItem struct {
	UserID  uint64 `json:"user_id"`
	LikedAt int64  `json:"liked_at"` // Unix 时间戳
}
