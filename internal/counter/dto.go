package counter

// ToggleRequest 是点赞、取消点赞、收藏、取消收藏接口共用的请求体。
type ToggleRequest struct {
	EntityType string `json:"entity_type" binding:"required"`
	EntityID   string `json:"entity_id" binding:"required"`
}
