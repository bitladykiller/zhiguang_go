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
type CounterReader interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
}

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

type payloadEnvelope struct {
	Entity string `json:"entity"`
	Op     string `json:"op"`
	ID     uint64 `json:"id"`
}

// KnowPostProjector 负责把 knowpost 事件投影到搜索索引。
type KnowPostProjector struct {
	db        *sqlx.DB
	searchSvc *SearchService
	counter   CounterReader
}

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

func (p *KnowPostProjector) SoftDeleteKnowPost(ctx context.Context, postID uint64) error {
	return p.searchSvc.IndexDocument(ctx, &SearchIndexDoc{
		ID:     strconv.FormatUint(postID, 10),
		Status: "deleted",
	})
}

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

func strValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
