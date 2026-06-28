package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/jsonutil"
)

// CounterReader defines the subset of counter read interface needed during search index projection.
//
// WHY: Only declares the GetCounts method, rather than directly referencing counter.CounterService.
// This avoids a stable compile-time dependency between the search and counter packages,
// and also follows the Interface Segregation Principle (ISP) — the projector only needs to know
// how to read counts, not the details of toggle state management.
type CounterReader interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
}

// searchIndexSourceRow is the raw row fetched from know_posts JOIN users during index projection.
// Contains all fields needed by the search engine index, loaded from MySQL in a single joined query.
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

// payloadEnvelope is the generic envelope structure parsed from the Payload field of an outbox event.
// Entity identifies the aggregate type (e.g., knowpost), Op denotes the operation type (upsert/delete),
// and ID is the aggregate root ID. The projector decides whether to execute upsert or soft delete based on Op.
type payloadEnvelope struct {
	Entity string `json:"entity"`
	Op     string `json:"op"`
	ID     uint64 `json:"id"`
}

// KnowPostProjector is responsible for projecting knowpost events into the search index.
//
// It receives outbox events consumed from the canal-outbox topic,
// parses the payload, and performs upsert or delete operations to update the ES index.
// Each projection re-queries the full data from MySQL and supplements real-time counts,
// ensuring eventual consistency of the ES index data.
type KnowPostProjector struct {
	db        sqlx.ExtContext
	searchSvc *SearchService
	counter   CounterReader
	logger    *zap.Logger
}

// NewKnowPostProjector creates a search index projector instance.
func NewKnowPostProjector(db sqlx.ExtContext, searchSvc *SearchService, counter CounterReader, logger *zap.Logger) *KnowPostProjector {
	if db == nil || searchSvc == nil {
		return nil
	}
	return &KnowPostProjector{
		db:        db,
		searchSvc: searchSvc,
		counter:   counter,
		logger:    logger,
	}
}

// ProjectPayload parses the payload JSON from an outbox event and performs upsert or delete on the search index.
//
// Parameters:
//   - raw: raw JSON bytes of the outbox event Payload field
//
// Flow:
//  1. Parse payloadEnvelope (containing entity, op, and id fields).
//  2. If entity != "knowpost" or id == 0, skip.
//  3. If op == "delete", perform soft delete (mark ES document status as "deleted").
//  4. Otherwise, perform upsert: query full data from MySQL and index to ES.
func (p *KnowPostProjector) ProjectPayload(ctx context.Context, raw []byte) error {
	if p == nil {
		return nil
	}

	var payload payloadEnvelope
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("project payload: unmarshal: %w", err)
	}
	if payload.Entity != "knowpost" || payload.ID == 0 {
		return nil
	}
	if strings.EqualFold(payload.Op, "delete") {
		return p.SoftDeleteKnowPost(ctx, payload.ID)
	}
	return p.UpsertKnowPost(ctx, payload.ID)
}

// UpsertKnowPost queries knowpost data from MySQL and indexes it into Elasticsearch.
//
// Flow:
//  1. Call buildSearchDocument to query the database and build the ES document.
//  2. If the query result is empty (sql.ErrNoRows), the knowpost may have been physically deleted,
//     so mark it as "deleted" in ES.
//  3. Otherwise, call searchSvc.IndexDocument to index into ES.
//
// Parameters:
//   - postID: snowflake ID of the knowpost
func (p *KnowPostProjector) UpsertKnowPost(ctx context.Context, postID uint64) error {
	doc, err := p.buildSearchDocument(ctx, postID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p.SoftDeleteKnowPost(ctx, postID)
		}
		return err
	}
	return p.searchSvc.IndexDocument(ctx, doc)
}

// SoftDeleteKnowPost marks a knowpost as "deleted" in the search index.
//
// This indexes a minimal document containing only ID and Status, overwriting all previous fields.
// Search filters out deleted documents using filter { term: { status: "published" } }.
func (p *KnowPostProjector) SoftDeleteKnowPost(ctx context.Context, postID uint64) error {
	return p.searchSvc.IndexDocument(ctx, &SearchIndexDoc{
		ID:     strconv.FormatUint(postID, 10),
		Status: "deleted",
	})
}

// buildSearchDocument queries the full knowpost data from MySQL and constructs the ES index document.
//
// The query JOINs the users table to get author info, and calls the counter service for real-time counts.
// If p.counter is nil, count fields will return 0.
//
// Function call notes:
//   - sqlx.GetContext(ctx, p.db, &row, sql, args...):
//     sqlx package-level function, queries a single row and maps to the struct.
//   - time.RFC3339: time format constant "2006-01-02T15:04:05Z07:00",
//     used to format publish time as ISO 8601 string.
func (p *KnowPostProjector) buildSearchDocument(ctx context.Context, postID uint64) (*SearchIndexDoc, error) {
	var row searchIndexSourceRow
	err := sqlx.GetContext(ctx, p.db, &row, `
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
		return nil, fmt.Errorf("build search document: db query: %w", err)
	}

	metrics := map[string]int32{}
	if p.counter != nil {
		counts, err := p.counter.GetCounts(ctx, "knowpost", strconv.FormatUint(postID, 10), []string{"like", "fav"})
		if err != nil {
			p.logger.Warn("failed to get counts for search index", zap.Error(err))
		} else if counts != nil {
			metrics = counts
		}
	}

	doc := &SearchIndexDoc{
		ID:            strconv.FormatUint(row.ID, 10),
		Title:         strings.TrimSpace(strValue(row.Title)),
		Description:   strings.TrimSpace(strValue(row.Description)),
		Body:          strings.TrimSpace(strValue(row.Description)),
		TagID:         row.TagID,
		Tags:          jsonutil.ParseStringArray(row.Tags),
		AuthorID:      strconv.FormatUint(row.CreatorID, 10),
		AuthorAvatar:  row.AuthorAvatar,
		AuthorName:    row.AuthorNickname,
		AuthorTagJSON: row.AuthorTagJSON,
		ImgURLs:       jsonutil.ParseStringArray(row.ImgURLs),
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

// buildSuggestField builds the ES completion suggester field, containing the title and tags.
//
// Parameters:
//   - title: pointer to the knowpost title string
//   - tags:  pointer to the knowpost tags JSON array
//
// Returns:
//   - *SuggestField: contains title and tags as completion inputs.
//     Returns nil if there is no valid input (the field is not indexed).
func buildSuggestField(title *string, tags *string) *SuggestField {
	inputs := make([]string, 0, 1)
	if text := strings.TrimSpace(strValue(title)); text != "" {
		inputs = append(inputs, text)
	}
	for _, tag := range jsonutil.ParseStringArray(tags) {
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

// strValue safely dereferences a *string, returning empty string for nil.
func strValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
