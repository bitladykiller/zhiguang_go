package model

// FeedItem 表示 feed 列表中的单个条目，是 knowpost 和 search 模块共享的领域模型。
//
// 设计意图：
//   - 避免 search 直接依赖 knowpost 的 DTO，打破模块间循环依赖。
//   - 作为跨模块的共享数据结构，只包含纯数据字段，不含业务逻辑。
//   - 各模块在返回 HTTP 响应时，将 FeedItem 映射到自己的 DTO 结构。
//
// 字段说明：
//   - ID：字符串类型，避免前端 JavaScript 精度丢失。
//   - Liked、Faved：用户态字段，只在读取时根据当前用户动态补齐，不会进入缓存。
//   - IsTop：仅"我的已发布"列表包含此字段，其他场景为 nil。
type FeedItem struct {
	ID             string   `json:"id"`
	Title          *string  `json:"title,omitempty"`
	Description    *string  `json:"description,omitempty"`
	CoverImage     *string  `json:"cover_image,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	AuthorAvatar   *string  `json:"author_avatar,omitempty"`
	AuthorNickname string   `json:"author_nickname"`
	TagJson        *string  `json:"tag_json,omitempty"`
	LikeCount      int64    `json:"like_count"`
	FavoriteCount  int64    `json:"favorite_count"`
	Liked          *bool    `json:"liked,omitempty"`
	Faved          *bool    `json:"faved,omitempty"`
	IsTop          *bool    `json:"is_top,omitempty"`
}
