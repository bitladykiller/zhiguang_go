package knowpost

import (
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// KnowPostRepository 封装 know_posts 相关的全部数据库操作。
//
// 提供的能力：
//   - 插入草稿（InsertDraft）
//   - 更新内容（UpdateContent）、元数据（UpdateMetadata）
//   - 状态流转：发布（Publish）、软删除（SoftDelete）
//   - 辅助更新：置顶（UpdateTop）、可见性（UpdateVisibility）
//   - 读取：详情查询（FindDetailByID）、公共 feed 分页（ListFeedPublic）、
//     我的已发布（ListMyPublished）
//   - 事务消息（InsertOutbox）
//
// 所有 Update* 方法都包含 WHERE creator_id = ? 条件，
// 确保用户只能修改自己的知文，安全性由数据库层保证。
type KnowPostRepository struct {
	db sqlx.Ext
}

func NewKnowPostRepository(db *sqlx.DB) *KnowPostRepository {
	return &KnowPostRepository{db: db}
}

// WithDB 把仓储重新绑定到指定 DB 句柄上，通常用于事务上下文。
func (r *KnowPostRepository) WithDB(db sqlx.Ext) *KnowPostRepository {
	return &KnowPostRepository{db: db}
}

func (r *KnowPostRepository) InsertDraft(post *KnowPost) error {
	_, err := sqlx.NamedExec(r.db, `
INSERT INTO know_posts (
    id, tag_id, tags, title, description, content_url, content_object_key, content_etag, content_size, content_sha256,
    creator_id, is_top, type, visible, img_urls, video_url, status, create_time, update_time, publish_time
) VALUES (
    :id, :tag_id, :tags, :title, :description, :content_url, :content_object_key, :content_etag, :content_size, :content_sha256,
    :creator_id, :is_top, :type, :visible, :img_urls, :video_url, :status, :create_time, :update_time, :publish_time
)`, post)
	return err
}

func (r *KnowPostRepository) UpdateContent(post *KnowPost) (int64, error) {
	result, err := r.db.Exec(
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

func (r *KnowPostRepository) UpdateMetadata(post *KnowPost) (int64, error) {
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
	result, err := r.db.Exec(
		"UPDATE know_posts SET "+strings.Join(sets, ", ")+" WHERE id = ? AND creator_id = ?",
		args...,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *KnowPostRepository) Publish(id, creatorID uint64) (int64, error) {
	now := time.Now()
	result, err := r.db.Exec(
		"UPDATE know_posts SET status = ?, publish_time = ?, update_time = ? WHERE id = ? AND creator_id = ? AND status = ?",
		"published", now, now, id, creatorID, "draft",
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *KnowPostRepository) UpdateTop(id, creatorID uint64, isTop bool) (int64, error) {
	topVal := 0
	if isTop {
		topVal = 1
	}
	result, err := r.db.Exec(
		"UPDATE know_posts SET is_top = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		topVal, time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *KnowPostRepository) UpdateVisibility(id, creatorID uint64, visible string) (int64, error) {
	result, err := r.db.Exec(
		"UPDATE know_posts SET visible = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		visible, time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *KnowPostRepository) SoftDelete(id, creatorID uint64) (int64, error) {
	result, err := r.db.Exec(
		"UPDATE know_posts SET status = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		"deleted", time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *KnowPostRepository) FindDetailByID(id uint64) (*KnowPostDetailRow, error) {
	var row KnowPostDetailRow
	err := sqlx.Get(r.db, &row, `
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

func (r *KnowPostRepository) ListFeedPublic(limit, offset int) ([]KnowPostFeedRow, error) {
	var rows []KnowPostFeedRow
	err := sqlx.Select(r.db, &rows, `
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

func (r *KnowPostRepository) ListMyPublished(userID uint64, limit, offset int) ([]KnowPostFeedRow, error) {
	var rows []KnowPostFeedRow
	err := sqlx.Select(r.db, &rows, `
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

func (r *KnowPostRepository) InsertOutbox(id uint64, aggType string, aggID *uint64, eventType, payload string) error {
	_, err := r.db.Exec(
		"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
		id, aggType, aggID, eventType, payload,
	)
	return err
}
