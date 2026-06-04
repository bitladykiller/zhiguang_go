// Package knowpost 实现知文领域模型与相关能力：
//   - 渐进式发布流程下的 CRUD（草稿 → 内容 → 元数据 → 发布）
//   - 基于三级缓存的 Feed 流（L1 freecache / L2 Redis 碎片缓存 / L3 DB）
//   - 使用 singleflight 防止缓存击穿
//   - 基于热点键的 TTL 动态延长
//   - 通过 outbox 事件驱动搜索索引同步
package knowpost

import "time"

// ============================================================================
// 数据模型
// ============================================================================

// KnowPost 映射 `know_posts` 表，主键 ID 由雪花算法生成，而不是数据库自增。
type KnowPost struct {
	ID               uint64     `db:"id" json:"id"`
	TagID            *uint64    `db:"tag_id" json:"tag_id,omitempty"`
	Tags             *string    `db:"tags" json:"tags,omitempty"`
	Title            *string    `db:"title" json:"title,omitempty"`
	Description      *string    `db:"description" json:"description,omitempty"`
	ContentUrl       *string    `db:"content_url" json:"content_url,omitempty"`
	ContentObjectKey *string    `db:"content_object_key" json:"content_object_key,omitempty"`
	ContentEtag      *string    `db:"content_etag" json:"content_etag,omitempty"`
	ContentSize      *uint64    `db:"content_size" json:"content_size,omitempty"`
	ContentSha256    *string    `db:"content_sha256" json:"content_sha256,omitempty"`
	CreatorID        uint64     `db:"creator_id" json:"creator_id"`
	IsTop            bool       `db:"is_top" json:"is_top"`
	Type             string     `db:"type" json:"type"`
	Visible          string     `db:"visible" json:"visible"`
	ImgUrls          *string    `db:"img_urls" json:"img_urls,omitempty"`
	VideoUrl         *string    `db:"video_url" json:"video_url,omitempty"`
	Status           string     `db:"status" json:"status"`
	CreateTime       time.Time  `db:"create_time" json:"create_time"`
	UpdateTime       time.Time  `db:"update_time" json:"update_time"`
	PublishTime      *time.Time `db:"publish_time" json:"publish_time,omitempty"`
}

// KnowPostDetailRow 是详情页使用的联表查询结果，额外包含作者信息。
type KnowPostDetailRow struct {
	ID             uint64     `db:"id" json:"id"`
	Title          *string    `db:"title" json:"title"`
	Description    *string    `db:"description" json:"description"`
	ContentUrl     *string    `db:"content_url" json:"content_url"`
	ImgUrls        *string    `db:"img_urls" json:"img_urls"`
	Tags           *string    `db:"tags" json:"tags"`
	CreatorID      uint64     `db:"creator_id" json:"creator_id"`
	AuthorAvatar   *string    `db:"author_avatar" json:"author_avatar"`
	AuthorNickname string     `db:"author_nickname" json:"author_nickname"`
	AuthorTagJson  *string    `db:"author_tag_json" json:"author_tag_json"`
	IsTop          bool       `db:"is_top" json:"is_top"`
	Visible        string     `db:"visible" json:"visible"`
	Type           string     `db:"type" json:"type"`
	Status         string     `db:"status" json:"status"`
	PublishTime    *time.Time `db:"publish_time" json:"publish_time"`
}

// KnowPostFeedRow 是 feed 列表使用的轻量查询结果。
type KnowPostFeedRow struct {
	ID             uint64  `db:"id" json:"id"`
	Title          *string `db:"title" json:"title"`
	Description    *string `db:"description" json:"description"`
	ImgUrls        *string `db:"img_urls" json:"img_urls"`
	Tags           *string `db:"tags" json:"tags"`
	AuthorAvatar   *string `db:"author_avatar" json:"author_avatar"`
	AuthorNickname string  `db:"author_nickname" json:"author_nickname"`
	AuthorTagJson  *string `db:"author_tag_json" json:"author_tag_json"`
	IsTop          bool    `db:"is_top" json:"is_top"`
}
