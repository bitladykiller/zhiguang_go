package knowpost

import "time"

// ============================================================================
// 响应 DTO
// ============================================================================

// KnowPostDetailResponse 是 `GET /knowposts/:id` 的响应结构。
// Liked 与 Faved 属于用户态字段，只在读取时动态补齐，不会进入缓存。
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
	Liked          *bool      `json:"liked,omitempty"`
	Faved          *bool      `json:"faved,omitempty"`
	IsTop          bool       `json:"is_top"`
	Visible        string     `json:"visible"`
	Type           string     `json:"type"`
	PublishTime    *time.Time `json:"publish_time"`
}

// FeedItemResponse 表示 feed 列表中的单个条目。
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
	Liked          *bool    `json:"liked,omitempty"`
	Faved          *bool    `json:"faved,omitempty"`
	IsTop          *bool    `json:"is_top,omitempty"`
}

// FeedPageResponse 表示分页后的 feed 列表。
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
	Title       *string  `json:"title"`
	TagID       *uint64  `json:"tag_id"`
	Tags        []string `json:"tags"`
	ImgUrls     []string `json:"img_urls"`
	Visible     *string  `json:"visible"`
	IsTop       *bool    `json:"is_top"`
	Description *string  `json:"description"`
}

type KnowPostVisibilityPatchRequest struct {
	Visible string `json:"visible" binding:"required"`
}

type KnowPostTopPatchRequest struct {
	IsTop bool `json:"is_top"`
}

type DescriptionSuggestRequest struct {
	Title   string `json:"title" binding:"required"`
	Content string `json:"content" binding:"required"`
}

type DescriptionSuggestResponse struct {
	Description string `json:"description"`
}

type RagQueryRequest struct {
	Question string `json:"question" binding:"required"`
}
