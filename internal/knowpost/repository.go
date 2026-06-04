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

// InsertDraft 插入一条新的知文草稿记录。
//
// 功能：使用 sqlx.NamedExec 执行 INSERT，通过结构体字段名（:tag 语法）绑定参数。
//
// sqlx.NamedExec 说明：
//   - sqlx 扩展提供按名称绑定的查询执行方法。
//   - SQL 语句中使用 `:field_name` 占位符，函数会查找传入结构体的对应 db 标签字段，
//     自动映射到参数值。
//   - 相较于普通的 ? 占位符，NamedExec 更适合插入字段较多的场景，
//     避免因参数顺序错位导致的 Bug。
//
// 参数：
//   - post: *KnowPost，包含要插入的全部字段值。
//
// 返回值：
//   - error: 数据库执行失败时返回错误。
//
// 设计决策：
//   使用 NamedExec 而非手动构建 SQL 的原因：
//   - know_posts 表有 20 个字段，手动排列 ? 非常容易出错。
//   - NamedExec 允许直接用结构体字段名映射，编译器可检查字段类型。
//   - 代价是每次调用会多一次结构体到 map 的转换，但 insert 操作频率较低，性能影响可忽略。
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

// UpdateContent 更新知文的内容元数据（OSS 对象键、哈希值、URL 等）。
//
// 功能：在用户确认上传 OSS 内容后，记录对象元数据到 know_posts 表。
// 使用 WHERE id = ? AND creator_id = ? 确保只有作者本人可以更新。
//
// 参数：
//   - post: *KnowPost，包含 ID、CreatorID 和要更新的内容字段。
//
// 返回值：
//   - int64: 影响的行数（0 表示未找到对应记录或权限不足）。
//   - error: 数据库执行失败时的错误。
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

// UpdateMetadata 动态更新知文的元数据字段。
//
// 功能：根据 KnowPost 结构体中非零/非空指针字段，动态构建 SET 子句。
// 仅更新调用方指定的字段，未指定的字段保持不变。
//
// 动态 SQL 构建策略：
//   - 遍历 KnowPost 结构的可选字段（Title、TagID、Tags、ImgUrls、Visible、Description）。
//   - 只有非 nil 的指针字段会被加入 SET 子句。
//   - 最后加上 WHERE id = ? AND creator_id = ?。
//
// 参数：
//   - post: *KnowPost，包含 ID、CreatorID 和需要更新的字段。
//
// 返回值：
//   - int64: 影响的行数（调用方需要检查是否 > 0 以判断操作是否实际生效）。
//   - error: 数据库执行失败时的错误。
//
// 边界情况：
//   - post 中所有可选字段都为 nil：只更新 update_time，生成一个无实际作用的"空更新"。
//     调用方（UpdateMetadata 业务层）已经校验过请求是否包含有效字段，因此不会出现此情况。
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

// Publish 将知文从草稿状态发布为已发布状态。
//
// 功能：同时更新 status、publish_time 和 update_time。
// WHERE 条件包含 AND status = 'draft'，确保只有草稿状态的知文可以被发布，
// 防止重复发布。
//
// 参数：
//   - id: uint64，知文 ID。
//   - creatorID: uint64，作者 ID（用于鉴权）。
//
// 返回值：
//   - int64: 影响的行数。0 表示草稿不存在、无权操作或已发布。
//   - error: 数据库执行失败时的错误。
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

// UpdateTop 更新知文的置顶标记。
//
// 功能：将 is_top 字段设置为 1（置顶）或 0（取消置顶）。
// 使用 WHERE id = ? AND creator_id = ? 确保权限安全。
//
// 参数：
//   - id: uint64，知文 ID。
//   - creatorID: uint64，作者 ID。
//   - isTop: bool，true 置顶，false 取消置顶。
//
// 返回值：
//   - int64: 影响的行数。
//   - error: 数据库执行失败时的错误。
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

// UpdateVisibility 更新知文的可见性设置。
//
// 功能：将 visible 字段更新为指定值（public/followers/school/private/unlisted）。
// 参数中的 visible 值应该在调用前经由 isValidVisible 校验。
//
// 参数：
//   - id: uint64，知文 ID。
//   - creatorID: uint64，作者 ID。
//   - visible: string，新的可见性值。
//
// 返回值：
//   - int64: 影响的行数。
//   - error: 数据库执行失败时的错误。
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

// SoftDelete 对知文执行软删除（将 status 设为 "deleted"）。
//
// 功能：软删除相比物理删除的优势：
//   - 可恢复性：管理员或用户可以在一定时间内恢复被删除的内容。
//   - 引用完整性：其他表可能通过外键引用该知文，软删除不会破坏约束。
//   - 数据审计：保留操作历史。
//
// 参数：
//   - id: uint64，知文 ID。
//   - creatorID: uint64，作者 ID。
//
// 返回值：
//   - int64: 影响的行数。0 表示知文不存在或无权删除。
//   - error: 数据库执行失败时的错误。
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

// FindDetailByID 根据知文 ID 查询详情，同时 JOIN users 表获取作者信息。
//
// 功能：这是详情页的最终数据源查询（L3）。
// 使用 LEFT JOIN users，即使 users 表中的作者记录被删除（不应发生），
// 知文详情仍然可以正常返回。
//
// sqlx.Get 说明：
//   - sqlx.Get 用于查询单行记录，自动将查询结果映射到结构体字段。
//   - 结构体字段需标注 db 标签（如 `db:"title"`）以匹配列名。
//   - 如果查询返回 0 行，sqlx.Get 返回 sql.ErrNoRows。
//
// 参数：
//   - id: uint64，知文 ID。
//
// 返回值：
//   - *KnowPostDetailRow: 查询成功时返回包含知文基本信息 + 作者信息的行。
//     如果查询返回 0 行，返回 nil 和 sql.ErrNoRows。
//   - error: 查询失败时的错误。
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

// ListFeedPublic 分页查询已发布的公开知文列表（公共 Feed 的数据源）。
//
// 功能：从 know_posts 表中筛选 status = 'published' 且 visible = 'public' 的知文，
// 按 publish_time 降序排列，LEFT JOIN users 表获取作者信息。
//
// sqlx.Select 说明：
//   - sqlx.Select 用于查询多行记录，将结果集自动映射到结构体切片。
//   - 内部自动处理 rows 迭代和关闭。
//
// 参数：
//   - limit: int，最多返回多少行（调用方传入 size+1 以检测是否有下一页）。
//   - offset: int，从第几行开始（(page-1) * size）。
//
// 返回值：
//   - []KnowPostFeedRow: 查询结果行。列表长度可能小于 limit（已到末尾）。
//   - error: 查询失败时的错误。
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

// ListMyPublished 分页查询某个用户自己已发布的知文列表（我的 Feed 的数据源）。
//
// 功能：查询指定用户的全部知文（不包含已删除的），
// 按 is_top DESC、create_time DESC 排列。
// 这意味着置顶的知文会排在最前面，同一级排序内新创建的排在前面。
//
// 与 ListFeedPublic 的区别：
//   - 没有 visible 条件：用户可以看到自己的全部已发布知文，包括非公开的。
//   - 包含 is_top 字段：排序中 is_top 优先级最高。
//   - 使用 status != 'deleted' 而非 status = 'published'：
//     用户可以在"我的 Feed"看到草稿（draft）状态的内容。
//
// 参数：
//   - userID: uint64，作者用户 ID。
//   - limit: int，返回的最大行数。
//   - offset: int，偏移量。
//
// 返回值：
//   - []KnowPostFeedRow: 查询结果行。
//   - error: 查询失败时的错误。
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

// InsertOutbox 在 outbox 表中插入一条事务消息。
//
// 功能：Transactional Outbox Pattern 的核心写入方法。
// 将事件元数据（聚合类型、聚合 ID、事件类型、JSON 载荷）持久化到 outbox 表。
// 后续由 Canal 组件读取 outbox 表的变化并发布到 Kafka。
//
// 参数：
//   - id: uint64，outbox 记录的主键（由雪花算法生成）。
//   - aggType: string，聚合类型，如 "knowpost"、"following"。
//   - aggID: *uint64，关联的聚合根 ID（可为 nil，例如某些取关事件没有关联的 relation ID）。
//   - eventType: string，事件类型，如 "KnowPostPublished"、"FollowCreated"。
//   - payload: string，JSON 序列化的事件载荷。
//
// 返回值：
//   - error: 数据库写入失败时的错误。
func (r *KnowPostRepository) InsertOutbox(id uint64, aggType string, aggID *uint64, eventType, payload string) error {
	_, err := r.db.Exec(
		"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
		id, aggType, aggID, eventType, payload,
	)
	return err
}
