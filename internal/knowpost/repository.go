package knowpost

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// knowPostUpdateFields 是 UpdateMetadata 使用的字段白名单。
// 仅当字段名在此 map 中存在时才会被拼接到 SQL SET 子句，防止注入。
var knowPostUpdateFields = map[string]string{
	"title":       "title",
	"tag_id":      "tag_id",
	"tags":        "tags",
	"img_urls":    "img_urls",
	"visible":     "visible",
	"description": "description",
	"is_top":      "is_top",
}

// KnowPostRepository 封装 know_posts 相关的全部数据库操作。
// 使用 sqlx.ExtContext 接口，同时支持 *sqlx.DB（普通连接）和 *sqlx.Tx（事务）。
type KnowPostRepository struct {
	db sqlx.ExtContext
}

// NewKnowPostRepository 创建 KnowPostRepository 实例。
//
// 参数:
//   - db: sqlx.ExtContext，支持 *sqlx.DB 或 *sqlx.Tx
//
// 返回值:
//   - *KnowPostRepository: 已初始化的仓储实例
func NewKnowPostRepository(db sqlx.ExtContext) *KnowPostRepository {
	return &KnowPostRepository{db: db}
}

// WithDB 克隆一个绑定到指定 sqlx 句柄的新仓储实例，用于事务上下文。
func (r *KnowPostRepository) WithDB(db sqlx.ExtContext) Repo {
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
	if err != nil {
		return fmt.Errorf("insert draft: %w", err)
	}
	return nil
}

// UpdateContent 更新内容元数据，WHERE id = ? AND creator_id = ? 确保权限。
func (r *KnowPostRepository) UpdateContent(ctx context.Context, post *KnowPost) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		`UPDATE know_posts
		 SET content_object_key = ?, content_etag = ?, content_size = ?, content_sha256 = ?, content_url = ?, update_time = ?
		 WHERE id = ? AND creator_id = ?`,
		post.ContentObjectKey, post.ContentEtag, post.ContentSize, post.ContentSha256, post.ContentURL, time.Now(), post.ID, post.CreatorID,
	)
	if err != nil {
		return 0, fmt.Errorf("update content: %w", err)
	}
	return result.RowsAffected()
}

// UpdateMetadata 动态构建 SET 子句，仅更新非零/非空字段。
func (r *KnowPostRepository) UpdateMetadata(ctx context.Context, post *KnowPost) (int64, error) {
	// 先收集「哪些字段要更新」，再统一过白名单并拼 SET 子句。
	// 之前每个字段重复一遍「判空→查白名单→append×2」四行；收集与拼接分离后，
	// 白名单校验只写一处，新增字段只需加一行收集逻辑。
	type fieldUpdate struct {
		column string
		value  interface{}
	}
	var updates []fieldUpdate
	if post.Title != nil {
		updates = append(updates, fieldUpdate{"title", *post.Title})
	}
	if post.TagID != nil {
		updates = append(updates, fieldUpdate{"tag_id", *post.TagID})
	}
	if post.Tags != nil {
		updates = append(updates, fieldUpdate{"tags", *post.Tags})
	}
	if post.ImgUrls != nil {
		updates = append(updates, fieldUpdate{"img_urls", *post.ImgUrls})
	}
	if post.Visible != "" {
		updates = append(updates, fieldUpdate{"visible", post.Visible})
	}
	if post.Description != nil {
		updates = append(updates, fieldUpdate{"description", *post.Description})
	}
	if post.IsTop {
		updates = append(updates, fieldUpdate{"is_top", 1})
	}

	sets := []string{"update_time = ?"}
	args := []interface{}{time.Now()}
	for _, u := range updates {
		if _, ok := knowPostUpdateFields[u.column]; !ok {
			return 0, fmt.Errorf("unknown field: %s", u.column)
		}
		sets = append(sets, u.column+" = ?")
		args = append(args, u.value)
	}

	args = append(args, post.ID, post.CreatorID)
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET "+strings.Join(sets, ", ")+" WHERE id = ? AND creator_id = ?",
		args...,
	)
	if err != nil {
		return 0, fmt.Errorf("update metadata: %w", err)
	}
	return result.RowsAffected()
}

// Publish 草稿→已发布，WHERE 含 AND status = 'draft' 防止重复发布。
func (r *KnowPostRepository) Publish(ctx context.Context, id, creatorID uint64) (int64, error) {
	now := time.Now()
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET status = ?, publish_time = ?, update_time = ? WHERE id = ? AND creator_id = ? AND status = ?",
		KnowPostStatusPublished, now, now, id, creatorID, KnowPostStatusDraft,
	)
	if err != nil {
		return 0, fmt.Errorf("publish: %w", err)
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
		return 0, fmt.Errorf("update top: %w", err)
	}
	return result.RowsAffected()
}

// UpdateVisibility 更新可见性。
func (r *KnowPostRepository) UpdateVisibility(ctx context.Context, id, creatorID uint64, visible KnowPostVisibility) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET visible = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		visible, time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, fmt.Errorf("update visibility: %w", err)
	}
	return result.RowsAffected()
}

// SoftDelete 软删除（status → "deleted"）。
func (r *KnowPostRepository) SoftDelete(ctx context.Context, id, creatorID uint64) (int64, error) {
	result, err := r.db.ExecContext(ctx,
		"UPDATE know_posts SET status = ?, update_time = ? WHERE id = ? AND creator_id = ?",
		KnowPostStatusDeleted, time.Now(), id, creatorID,
	)
	if err != nil {
		return 0, fmt.Errorf("soft delete: %w", err)
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
		return nil, fmt.Errorf("find detail by id: %w", err)
	}
	return &row, nil
}

// FindByIDs 根据 ID 批量查询已发布的公开知文，返回 FeedRow。
func (r *KnowPostRepository) FindByIDs(ctx context.Context, ids []uint64) ([]KnowPostFeedRow, error) {
	return r.FindFeedRowsByIDs(ctx, ids, []KnowPostVisibility{KnowPostVisibilityPublic})
}

// FindFeedRowsByIDs 按 ID 批量取 Feed 行，可见性档位由调用方按场景给定。
//
// WHY 需要档位参数：公共 Feed 只应含 public；而**关注流**按定义只包含
// 「我关注的作者」，followers-only 内容对关注者应当可见——
// 早期关注流复用 public-only 的查询，该可见性等级对粉丝也形同虚设。
func (r *KnowPostRepository) FindFeedRowsByIDs(ctx context.Context, ids []uint64, visibilities []KnowPostVisibility) ([]KnowPostFeedRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(visibilities) == 0 {
		visibilities = []KnowPostVisibility{KnowPostVisibilityPublic}
	}
	query, args, err := sqlx.In(`
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
WHERE know_posts.id IN (?)
  AND know_posts.status = ?
  AND know_posts.visible IN (?)
ORDER BY know_posts.publish_time DESC
`, ids, KnowPostStatusPublished, visibilities)
	if err != nil {
		return nil, fmt.Errorf("find by ids: build query: %w", err)
	}
	query = r.db.Rebind(query)
	var rows []KnowPostFeedRow
	if err := sqlx.SelectContext(ctx, r.db, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("find by ids: select: %w", err)
	}
	return rows, nil
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
`, KnowPostStatusPublished, KnowPostVisibilityPublic, limit, offset)
	if err != nil {
		return rows, fmt.Errorf("list feed public: %w", err)
	}
	return rows, nil
}

// ListIDsForBloom 按 id 游标扫描未删除知文 ID，供详情 Bloom 预热。
//
// WHY 用 lastID 游标而非 OFFSET：全表预热时 OFFSET 越深越慢，游标稳定且可中断续跑。
func (r *KnowPostRepository) ListIDsForBloom(ctx context.Context, lastID uint64, limit int) ([]uint64, error) {
	if limit <= 0 {
		limit = 1000
	}
	var ids []uint64
	err := sqlx.SelectContext(ctx, r.db, &ids, `
SELECT id FROM know_posts
WHERE id > ? AND status != ?
ORDER BY id ASC
LIMIT ?
`, lastID, KnowPostStatusDeleted, limit)
	if err != nil {
		return nil, fmt.Errorf("list ids for bloom: %w", err)
	}
	return ids, nil
}

// ListMyPublished 分页查询某用户的**已发布**知文。
//
// 语义修正：早期条件是 status != deleted，草稿会混进「我的已发布」——
// 仅本人可见故不泄露，但接口名与返回内容不一致；草稿应走独立的草稿箱接口。
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
WHERE know_posts.creator_id = ? AND know_posts.status = ?
ORDER BY know_posts.is_top DESC, know_posts.create_time DESC
LIMIT ? OFFSET ?
`, userID, KnowPostStatusPublished, limit, offset)
	if err != nil {
		return rows, fmt.Errorf("list my published: %w", err)
	}
	return rows, nil
}
