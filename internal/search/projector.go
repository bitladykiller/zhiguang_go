package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// CounterReader 定义搜索索引投影过程中所需的计数读取接口子集。
//
// WHY：只声明 GetCounts 一个方法，而不是直接引用 counter.CounterService。
// 这样可以避免 search 包和 counter 包之间产生稳定的编译依赖，
// 同时也是对接口隔离原则（ISP）的实践——投影器只需要知道如何读取计数，
// 不需要知道切换开关状态的细节。
type CounterReader interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
}

// searchIndexSourceRow 是索引投影时从 know_posts JOIN users 查回来的原始行。
// 包含搜索引擎索引所需的全部字段，从 MySQL 联表查询一次性加载。
type searchIndexSourceRow struct {
	ID             uint64     `db:"id"`
	TagID          *uint64    `db:"tag_id"`
	Title          *string    `db:"title"`
	Description    *string    `db:"description"`
	ImgURLs        *string    `db:"img_urls"`
	Tags           *string    `db:"tags"`
	CreatorID      uint64     `db:"creator_id"`
	AuthorAvatar   *string    `db:"author_avatar"`
	AuthorNickname string     `db:"author_nickname"`
	AuthorTagJSON  *string    `db:"author_tag_json"`
	Status         string     `db:"status"`
	Visible        string     `db:"visible"`
	IsTop          bool       `db:"is_top"`
	PublishTime    *time.Time `db:"publish_time"`
}

// payloadEnvelope 是从 outbox 事件的 Payload 字段解析出的通用信封结构。
// Entity 标识聚合类型（如 knowpost），Op 表示操作类型（upsert/delete），
// ID 是聚合根 ID。投影器根据 Op 值决定执行 upsert 还是软删除。
type payloadEnvelope struct {
	Entity string `json:"entity"`
	Op     string `json:"op"`
	ID     uint64 `json:"id"`
}

// KnowPostProjector 负责把 knowpost 事件投影到搜索索引。
//
// 接收从 canal-outbox 主题消费到的 outbox 事件，
// 解析 payload 后执行 upsert 或 delete 操作更新 ES 索引。
// 每次投影都会重新从 MySQL 查询完整的数据并补充实时计数，
// 确保 ES 索引中的数据是最终一致的（eventual consistency）。
type KnowPostProjector struct {
	db        *sqlx.DB
	searchSvc *SearchService
	counter   CounterReader
}

// NewKnowPostProjector 创建搜索索引投影器实例。
//
// 参数：
//   - db: MySQL 数据库连接，用于查询知文详情的原始数据
//   - searchSvc: Elasticsearch 搜索服务，用于索引文档
//   - counter: 计数器读取接口，用于在索引时附带实时计数
//
// 如果 db 或 searchSvc 为 nil，则返回 nil（投影器不可用）。
func NewKnowPostProjector(db *sqlx.DB, searchSvc *SearchService, counter CounterReader) *KnowPostProjector {
	if db == nil || searchSvc == nil {
		return nil
	}
	return &KnowPostProjector{
		db:        db,
		searchSvc: searchSvc,
		counter:   counter,
	}
}

// ProjectPayload 解析 outbox 事件的 payload JSON，执行搜索索引的 upsert 或 delete 操作。
//
// 参数：
//   - raw: outbox 事件 Payload 字段的原始 JSON 字节
//
// 流程：
//  1. 解析 payloadEnvelope（含 entity、op、id 三个字段）。
//  2. 如果 entity != "knowpost" 或 id == 0，跳过。
//  3. 如果 op == "delete"，执行软删除（将 ES 文档的 status 标记为 "deleted"）。
//  4. 否则执行 upsert：从 MySQL 查询完整数据后索引到 ES。
func (p *KnowPostProjector) ProjectPayload(ctx context.Context, raw []byte) error {
	if p == nil {
		return nil
	}

	var payload payloadEnvelope
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	if payload.Entity != "knowpost" || payload.ID == 0 {
		return nil
	}
	if strings.EqualFold(payload.Op, "delete") {
		return p.SoftDeleteKnowPost(ctx, payload.ID)
	}
	return p.UpsertKnowPost(ctx, payload.ID)
}

// UpsertKnowPost 从 MySQL 查询知文数据，索引到 Elasticsearch。
//
// 流程：
//  1. 调用 buildSearchDocument 查询数据库并构建 ES 文档。
//  2. 如果查询结果为空（sql.ErrNoRows），说明该知文可能已被物理删除，
//     在 ES 中将其标记为 "deleted"。
//  3. 否则调用 searchSvc.IndexDocument 索引到 ES。
//
// 参数：
//   - postID: 知文的雪花 ID
func (p *KnowPostProjector) UpsertKnowPost(ctx context.Context, postID uint64) error {
	doc, err := p.buildSearchDocument(ctx, postID)
	if err != nil {
		if err == sql.ErrNoRows {
			return p.SoftDeleteKnowPost(ctx, postID)
		}
		return err
	}
	return p.searchSvc.IndexDocument(ctx, doc)
}

// SoftDeleteKnowPost 在搜索索引中将知文标记为 "deleted"。
//
// 这会索引一个只包含 ID 和 Status 的最小文档，覆盖之前的全部字段。
// 搜索时通过 filter { term: { status: "published" } } 过滤掉已删除的文档。
func (p *KnowPostProjector) SoftDeleteKnowPost(ctx context.Context, postID uint64) error {
	return p.searchSvc.IndexDocument(ctx, &SearchIndexDoc{
		ID:     strconv.FormatUint(postID, 10),
		Status: "deleted",
	})
}

// buildSearchDocument 从 MySQL 查询知文完整数据，构建 ES 索引文档。
//
// 查询会 JOIN users 表获取作者信息，并调用 counter 服务获取实时计数。
// 如果 p.counter 为 nil，计数部分将返回 0。
//
// 函数调用说明：
//   - p.db.GetContext(ctx, &row, sql, args...):
//     sqlx 的方法，查询单行并映射到结构体。
//     Go 的标准 database/sql 的 QueryRow 的扩展版本。
//   - time.RFC3339: 时间格式常量 "2006-01-02T15:04:05Z07:00"，
//     用于格式化发布时间的 ISO 8601 字符串。
func (p *KnowPostProjector) buildSearchDocument(ctx context.Context, postID uint64) (*SearchIndexDoc, error) {
	var row searchIndexSourceRow
	err := p.db.GetContext(ctx, &row, `
SELECT
    know_posts.id,
    know_posts.tag_id,
    know_posts.title,
    know_posts.description,
    know_posts.img_urls,
    know_posts.tags,
    know_posts.creator_id,
    know_posts.status,
    know_posts.visible,
    know_posts.is_top,
    know_posts.publish_time,
    users.avatar AS author_avatar,
    users.nickname AS author_nickname,
    users.tags_json AS author_tag_json
FROM know_posts
LEFT JOIN users ON know_posts.creator_id = users.id
WHERE know_posts.id = ?
`, postID)
	if err != nil {
		return nil, err
	}

	metrics := map[string]int32{}
	if p.counter != nil {
		counts, err := p.counter.GetCounts(ctx, "knowpost", strconv.FormatUint(postID, 10), []string{"like", "fav"})
		if err == nil && counts != nil {
			metrics = counts
		}
	}

	doc := &SearchIndexDoc{
		ID:            strconv.FormatUint(row.ID, 10),
		Title:         strings.TrimSpace(strValue(row.Title)),
		Description:   strings.TrimSpace(strValue(row.Description)),
		Body:          strings.TrimSpace(strValue(row.Description)),
		TagID:         row.TagID,
		Tags:          parseJSONTags(row.Tags),
		AuthorID:      strconv.FormatUint(row.CreatorID, 10),
		AuthorAvatar:  row.AuthorAvatar,
		AuthorName:    row.AuthorNickname,
		AuthorTagJSON: row.AuthorTagJSON,
		ImgURLs:       parseJSONTags(row.ImgURLs),
		LikeCount:     int64(metrics["like"]),
		FavCount:      int64(metrics["fav"]),
		ViewCount:     0,
		IsTop:         row.IsTop,
		Status:        row.Status,
		Visible:       row.Visible,
		TitleSuggest:  strings.TrimSpace(strValue(row.Title)),
		Suggest:       buildSuggestField(row.Title, row.Tags),
	}
	if row.PublishTime != nil {
		value := row.PublishTime.UTC().Format(time.RFC3339)
		doc.PublishTime = &value
	}
	return doc, nil
}

// parseJSONTags 解析 JSON 字符串数组为 Go []string。
//
// 参数：
//   - raw: 指向 JSON 字符串的指针（可能为 nil）
//
// 返回空切片而非 nil，避免序列化时为 null。
func parseJSONTags(raw *string) []string {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return []string{}
	}

	var tags []string
	if err := json.Unmarshal([]byte(*raw), &tags); err != nil {
		return []string{}
	}
	return tags
}

// buildSuggestField 构建 ES completion suggester 字段，包含标题和标签。
//
// 参数：
//   - title: 知文章节的标题指针
//   - tags: 知文章节的标签 JSON 数组指针
//
// 返回值：
//   - *SuggestField: 包含标题和标签作为 completion 输入的字段。
//     如果没有有效输入则返回 nil（不索引该字段）。
func buildSuggestField(title *string, tags *string) *SuggestField {
	inputs := make([]string, 0, 1)
	if text := strings.TrimSpace(strValue(title)); text != "" {
		inputs = append(inputs, text)
	}
	for _, tag := range parseJSONTags(tags) {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			inputs = append(inputs, tag)
		}
	}
	if len(inputs) == 0 {
		return nil
	}
	return &SuggestField{Input: inputs}
}

// strValue 安全地解引用 *string，nil 指针返回空字符串。
func strValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
