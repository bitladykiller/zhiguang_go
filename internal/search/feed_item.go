package search

// FeedItem 是搜索结果中的单个内容条目。
//
// 它曾以 model.FeedItem 的名义放在 internal/model「供 knowpost 与 search 共享」，
// 但 knowpost 实际维护着自己的 FeedItemResponse（字段几乎逐一重复），
// 共享类型从未真正被共享——两份平行定义各自漂移，抽象只剩形式。
// 现将其归还给唯一的真实使用方：search 自己。
// 各模块的对外 DTO 独立演进，这正是模块边界应有的样子。
//
// 字段说明：
//   - ID：字符串类型，避免前端 JavaScript 精度丢失。
//   - Liked、Faved：用户态字段，只在读取时根据当前用户动态补齐，不进缓存。
type FeedItem struct {
	ID             string   `json:"id"`
	Title          *string  `json:"title,omitempty"`
	Description    *string  `json:"description,omitempty"`
	CoverImage     *string  `json:"cover_image,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	AuthorAvatar   *string  `json:"author_avatar,omitempty"`
	AuthorNickname string   `json:"author_nickname"`
	TagJSON        *string  `json:"tag_json,omitempty"`
	LikeCount      int64    `json:"like_count"`
	FavoriteCount  int64    `json:"favorite_count"`
	Liked          *bool    `json:"liked,omitempty"`
	Faved          *bool    `json:"faved,omitempty"`
	IsTop          *bool    `json:"is_top,omitempty"`
}
