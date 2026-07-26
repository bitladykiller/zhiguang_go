package counter

// ToggleRequest 是点赞、取消点赞、收藏、取消收藏接口共用的请求体。
type ToggleRequest struct {
	EntityType string `json:"entity_type" binding:"required"`
	EntityID   string `json:"entity_id" binding:"required"`
}

// LikersResponse 返回指定实体的点赞/收藏用户列表（分页）。
//
// Cursor 是**不透明**的复合游标（形如 "t:{ts}:{uid}" 或 "u:{uid}"），
// 客户端原样回传即可；空串表示第一页。编码细节见 likers.go。
type LikersResponse struct {
	Items   []LikerItem `json:"items"`
	Cursor  string      `json:"cursor,omitempty"`
	HasMore bool        `json:"has_more"`
}

// LikerItem 表示一个点赞/收藏用户。
type LikerItem struct {
	UserID  uint64 `json:"user_id"`
	LikedAt int64  `json:"liked_at"` // Unix 时间戳
}
