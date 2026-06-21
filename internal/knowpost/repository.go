package knowpost

import (
	"context"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// KnowPostRepository 封装 know_posts 相关的全部数据库操作。
// 使用 sqlx.ExtContext 接口，同时支持 *sqlx.DB（普通连接）和 *sqlx.Tx（事务）。
type KnowPostRepository struct {
	db sqlx.ExtContext
}

func NewKnowPostRepository(db sqlx.ExtContext) *KnowPostRepository {
	return &KnowPostRepository{db: db}
}

// WithDB 克隆一个绑定到指定 sqlx 句柄的新仓储实例，用于事务上下文。
func (r *KnowPostRepository) WithDB(db sqlx.ExtContext) *KnowPostRepository {
	return &KnowPostRepository{db: db}
}

// InsertDraft 插入知文草稿，使用 sqlx.NamedExecContext 按结构体字段名绑定参数。
func (r *KnowPostRepository) InsertDraft(ctx context.Context, post *KnowPost) error {
	_, err := sqlx.NamedExecContext(ctx, r.db, `
INSERT INTO know_posts (
    id, tag_id, tags, title, description, content_url, content_object_key, content_etag, content_size, content_sha256,
    creator_id, is_top, type, visible, img_urls, video_url, status, create_time, update_time, publish_time
) VALUES (
    :id, :tag_id, :tags, :title, :description, :content_url, :content_object_key, :content_etag, :content_size, :content_sha256,
    :creator_id, :is_top, :type, :visible, :img_urls, :video_url, :status, :create_time, :update_time, :publish_time
)`, post)
	return err
}

// UpdateContent 更新内容元数据，WHERE id = ? AND creator_id = ? 确保权限。
func (r *KnowPostRepository) UpdateContent(ctx context.Context, post *KnowPost) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		`UPDATE know_posts
		 SET content_object_key = ?, content_etag = ?, content_size = ?, content_sha256 = ?, content_url = ?, update_time = ?
		 WHERE id = ? AND creator_id = ?`,
		post.ContentObjectKey, post.ContentEtag, post.ContentSize, post.ContentSha256, post.ContentUrl, time.Now(), post.ID, post.CreatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// UpdateMetadata 动态构建 SET 子句，仅更新非零/非空字段。
func (r *KnowPostRepository) UpdateMetadata(ctx context.Context, post *KnowPost) (int64, error) {
	sets := []string{"update_time = ?"}
	args := []interface{}{time.Now()}
	if post.Title != nil {
		sets = append(sets, "title = ?")
		args = append(args, *post.Title)
	}
	if post.TagID != nil {
		sets = append(sets, "tag_id = ?")
		args = append(args, *post.TagID)
	}
	if post.Tags != nil {
		sets = append(sets, "tags = ?")
		args = append(args, *post.Tags)
	}
	if post.ImgUrls != nil {
		sets = append(sets, "img_urls = ?")
		args = append(args, *post.ImgUrls)
	}
	if post.Visible != "" {
		sets = append(sets, "visible = ?")
		args = append(args, post.Visible)
	}
	if post.Description != nil {
		sets = append(sets, "description = ?")
		args = append(args, *post.Description)
	}
	if post.IsTop {
		sets = append(sets, "is_top = ?")
		args = append(args, 1)
	}

	args = append(args, post.ID, post.CreatorID)
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET "+strings.Join(sets, ", ")+" WHERE id = ? AND creator_id = ?",
		args...,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Publish 草稿→已发布，WHERE 含 AND status = 'draft' 防止重复发布。
func (r *KnowPostRepository) Publish(ctx context.Context, id, creatorID uint64) (int64, error) {
	now := time.Now()
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET status = ?, publish_time = ?, update_time = ? WHERE id = ? AND creator_id = ? AND status = ?",
		"published", now, now, id, creatorID, "draft",
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// UpdateTop 更新置顶标记。
func (r *KnowPostRepository) UpdateTop(ctx context.Context, id, creatorID uint64, isTop bool) (int64, error) {
	topVal := 0
	if isTop {
		topVal = 1
	}
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET is_top = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		topVal, time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// UpdateVisibility 更新可见性。
func (r *KnowPostRepository) UpdateVisibility(ctx context.Context, id, creatorID uint64, visible string) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET visible = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		visible, time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// SoftDelete 软删除（status → "deleted"）。
func (r *KnowPostRepository) SoftDelete(ctx context.Context, id, creatorID uint64) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET status = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		"deleted", time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// FindDetailByID 查询详情，LEFT JOIN users 获取作者信息，使用 sqlx.GetContext。
func (r *KnowPostRepository) FindDetailByID(ctx context.Context, id uint64) (*KnowPostDetailRow, error) {
	var row KnowPostDetailRow
	err := sqlx.GetContext(ctx, r.db, &row, `
SELECT
    know_posts.id,
    know_posts.title,
    know_posts.description,
    know_posts.content_url,
    know_posts.img_urls,
    know_posts.tags,
    know_posts.creator_id,
    know_posts.is_top,
    know_posts.visible,
    know_posts.type,
    know_posts.status,
    know_posts.publish_time,
    users.avatar AS author_avatar,
    users.nickname AS author_nickname,
    users.tags_json AS author_tag_json
FROM know_posts
LEFT JOIN users ON know_posts.creator_id = users.id
WHERE know_posts.id = ?
`, id)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// ListFeedPublic 分页查询已发布的公开知文，使用 sqlx.SelectContext。
func (r *KnowPostRepository) ListFeedPublic(ctx context.Context, limit, offset int) ([]KnowPostFeedRow, error) {
	var rows []KnowPostFeedRow
	err := sqlx.SelectContext(ctx, r.db, &rows, `
SELECT
    know_posts.id,
    know_posts.title,
    know_posts.description,
    know_posts.img_urls,
    know_posts.tags,
    users.avatar AS author_avatar,
    users.nickname AS author_nickname,
    users.tags_json AS author_tag_json
FROM know_posts
LEFT JOIN users ON know_posts.creator_id = users.id
WHERE know_posts.status = ? AND know_posts.visible = ?
ORDER BY know_posts.publish_time DESC
LIMIT ? OFFSET ?
`, "published", "public", limit, offset)
	return rows, err
}

// ListMyPublished 分页查询某用户的已发布知文，使用 sqlx.SelectContext。
func (r *KnowPostRepository) ListMyPublished(ctx context.Context, userID uint64, limit, offset int) ([]KnowPostFeedRow, error) {
	var rows []KnowPostFeedRow
	err := sqlx.SelectContext(ctx, r.db, &rows, `
SELECT
    know_posts.id,
    know_posts.title,
    know_posts.description,
    know_posts.img_urls,
    know_posts.tags,
    know_posts.is_top,
    users.avatar AS author_avatar,
    users.nickname AS author_nickname,
    users.tags_json AS author_tag_json
FROM know_posts
LEFT JOIN users ON know_posts.creator_id = users.id
WHERE know_posts.creator_id = ? AND know_posts.status != ?
ORDER BY know_posts.is_top DESC, know_posts.create_time DESC
LIMIT ? OFFSET ?
`, userID, "deleted", limit, offset)
	return rows, err
}
