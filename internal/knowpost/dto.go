package knowpost

import "time"

// ============================================================================
// 响应 DTO（Data Transfer Object）
// ============================================================================

// KnowPostDetailResponse 是 `GET /knowposts/:id` 的响应结构。
//
// 字段说明：
//   - ID、AuthorID：用字符串而非 uint64 表示，避免前端 JavaScript 精度丢失。
//   - Liked、Faved：用户态字段，只在读取时根据当前用户动态补齐，不会进入缓存。
//   - PublishTime：仅在 status 为 "published" 时有值。
type KnowPostDetailResponse struct {
	ID             string     `json:"id"`
	Title          *string    `json:"title"`
	Description    *string    `json:"description"`
	ContentUrl     *string    `json:"content_url"`
	Images         []string   `json:"images"`
	Tags           []string   `json:"tags"`
	AuthorID       string     `json:"author_id"`
	AuthorAvatar   *string    `json:"author_avatar"`
	AuthorNickname string     `json:"author_nickname"`
	AuthorTagJson  *string    `json:"author_tag_json"`
	LikeCount      int64      `json:"like_count"`
	FavoriteCount  int64      `json:"favorite_count"`
	Liked          *bool      `json:"liked,omitempty"`    // 当前用户是否已点赞（动态补齐，不入缓存）
	Faved          *bool      `json:"faved,omitempty"`    // 当前用户是否已收藏（动态补齐，不入缓存）
	IsTop          bool       `json:"is_top"`
	Visible        string     `json:"visible"`
	Type           string     `json:"type"`
	PublishTime    *time.Time `json:"publish_time"`
}

// FeedItemResponse 表示 feed 列表中的单个条目。
// CoverImage 取 img_urls 的第一张图，由 feed_service 在 mapRowsToItems 中填充。
type FeedItemResponse struct {
	ID             string   `json:"id"`
	Title          *string  `json:"title"`
	Description    *string  `json:"description"`
	CoverImage     *string  `json:"cover_image"`
	Tags           []string `json:"tags"`
	AuthorAvatar   *string  `json:"author_avatar"`
	AuthorNickname string   `json:"author_nickname"`
	TagJson        *string  `json:"tag_json"`
	LikeCount      int64    `json:"like_count"`
	FavoriteCount  int64    `json:"favorite_count"`
	Liked          *bool    `json:"liked,omitempty"`     // 当前用户是否点赞（动态补齐）
	Faved          *bool    `json:"faved,omitempty"`     // 当前用户是否收藏（动态补齐）
	IsTop          *bool    `json:"is_top,omitempty"`    // 仅"我的已发布"列表包含此字段
}

// FeedPageResponse 表示带分页信息的 feed 列表。
type FeedPageResponse struct {
	Items   []FeedItemResponse `json:"items"`
	Page    int                `json:"page"`
	Size    int                `json:"size"`
	HasMore bool               `json:"has_more"`
}

// ============================================================================
// 请求 DTO
// ============================================================================

type KnowPostContentConfirmRequest struct {
	ObjectKey string `json:"object_key" binding:"required"`
	Etag      string `json:"etag" binding:"required"`
	Size      uint64 `json:"size" binding:"required"`
	Sha256    string `json:"sha256" binding:"required"`
}

type KnowPostPatchRequest struct {
	Title       *string            `json:"title"`
	TagID       *uint64            `json:"tag_id"`
	Tags        []string           `json:"tags"`
	ImgUrls     []string           `json:"img_urls"`
	Visible     *KnowPostVisibility `json:"visible"`
	IsTop       *bool              `json:"is_top"`
	Description *string            `json:"description"`
}

type KnowPostVisibilityPatchRequest struct {
	Visible KnowPostVisibility `json:"visible" binding:"required"`
}

type KnowPostTopPatchRequest struct {
	IsTop bool `json:"is_top"`
}
