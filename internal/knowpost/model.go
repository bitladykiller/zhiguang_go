// Package knowpost 实现知文领域模型与相关能力：
//   - 渐进式发布流程下的 CRUD：草稿 → 内容上传 → 元数据编辑 → 发布
//   - 基于三级缓存的 Feed 流（L1 freecache / L2 Redis 碎片缓存 / L3 DB）
//   - 使用 Redis 看门狗分布式锁防止缓存击穿
//   - 基于热点键探测的 TTL 动态延长
//   - 通过事务内 outbox 事件驱动搜索索引异步同步
//
// 领域术语：
//   - 知文（KnowPost）：知识帖子的简称，是平台的核心内容载体。
//   - 草稿（Draft）：未发布的知文，支持多次编辑。
//   - Feed：知文的列表流，分为公共 feed 和「我的已发布」feed。
package knowpost

import "time"

// ============================================================================
// 知文状态与可见性强类型枚举
// ============================================================================

// KnowPostStatus 表示知文的生命周期状态。
type KnowPostStatus string

const (
	KnowPostStatusDraft     KnowPostStatus = "draft"
	KnowPostStatusPublished KnowPostStatus = "published"
	KnowPostStatusDeleted   KnowPostStatus = "deleted"
)

// KnowPostVisibility 表示知文的可见性范围。
type KnowPostVisibility string

const (
	KnowPostVisibilityPublic    KnowPostVisibility = "public"
	KnowPostVisibilityFollowers KnowPostVisibility = "followers"
	KnowPostVisibilitySchool    KnowPostVisibility = "school"
	KnowPostVisibilityPrivate   KnowPostVisibility = "private"
	KnowPostVisibilityUnlisted  KnowPostVisibility = "unlisted"
)

// ============================================================================
// 数据模型
// ============================================================================

// KnowPost 映射 know_posts 表，主键 ID 由雪花算法生成而非数据库自增。
//
// 设计决策：
//   - 使用雪花 ID 而非自增主键，可以提前确定 ID，简化 outbox 事件的引用逻辑。
//   - 指针字段（*string / *uint64）：对应数据库中允许 NULL 的列。
//   - Status 管理知文的生命周期状态：draft → published / deleted。
type KnowPost struct {
	ID               uint64            `db:"id" json:"id"`
	TagID            *uint64           `db:"tag_id" json:"tag_id,omitempty"`
	Tags             *string           `db:"tags" json:"tags,omitempty"`
	Title            *string           `db:"title" json:"title,omitempty"`
	Description      *string           `db:"description" json:"description,omitempty"`
	ContentUrl       *string           `db:"content_url" json:"content_url,omitempty"`
	ContentObjectKey *string           `db:"content_object_key" json:"content_object_key,omitempty"`
	ContentEtag      *string           `db:"content_etag" json:"content_etag,omitempty"`
	ContentSize      *uint64           `db:"content_size" json:"content_size,omitempty"`
	ContentSha256    *string           `db:"content_sha256" json:"content_sha256,omitempty"`
	CreatorID        uint64            `db:"creator_id" json:"creator_id"`
	IsTop            bool              `db:"is_top" json:"is_top"`
	Type             string            `db:"type" json:"type"`
	Visible          KnowPostVisibility `db:"visible" json:"visible"`
	ImgUrls          *string           `db:"img_urls" json:"img_urls,omitempty"`
	VideoUrl         *string           `db:"video_url" json:"video_url,omitempty"`
	Status           KnowPostStatus    `db:"status" json:"status"`
	CreateTime       time.Time         `db:"create_time" json:"create_time"`
	UpdateTime       time.Time         `db:"update_time" json:"update_time"`
	PublishTime      *time.Time        `db:"publish_time" json:"publish_time,omitempty"`
}

// KnowPostDetailRow 是详情页使用的联表查询结果，额外包含作者信息。
type KnowPostDetailRow struct {
	ID             uint64            `db:"id" json:"id"`
	Title          *string           `db:"title" json:"title"`
	Description    *string           `db:"description" json:"description"`
	ContentUrl     *string           `db:"content_url" json:"content_url"`
	ImgUrls        *string           `db:"img_urls" json:"img_urls"`
	Tags           *string           `db:"tags" json:"tags"`
	CreatorID      uint64            `db:"creator_id" json:"creator_id"`
	AuthorAvatar   *string           `db:"author_avatar" json:"author_avatar"`
	AuthorNickname string            `db:"author_nickname" json:"author_nickname"`
	AuthorTagJson  *string           `db:"author_tag_json" json:"author_tag_json"`
	IsTop          bool              `db:"is_top" json:"is_top"`
	Visible        KnowPostVisibility `db:"visible" json:"visible"`
	Type           string            `db:"type" json:"type"`
	Status         KnowPostStatus    `db:"status" json:"status"`
	PublishTime    *time.Time        `db:"publish_time" json:"publish_time"`
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
