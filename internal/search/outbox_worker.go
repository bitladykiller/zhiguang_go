package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

const (
	knowPostAggregateType       = "knowpost"
	defaultOutboxBatchSize      = 50
	defaultOutboxPollInterval   = 2 * time.Second
	eventTypeKnowPostMetadata   = "KnowPostMetadataUpdated"
	eventTypeKnowPostPublished  = "KnowPostPublished"
	eventTypeKnowPostDeleted    = "KnowPostDeleted"
	eventTypeKnowPostVisibility = "KnowPostVisibilityUpdated"
	eventTypeKnowPostTop        = "KnowPostTopUpdated"
)

// CounterReader 定义搜索索引投影过程中所需的计数读取接口子集。
type CounterReader interface {
	GetCounts(ctx context.Context, entityType, entityID string, metrics []string) (map[string]int32, error)
}

type outboxRow struct {
	ID            uint64    `db:"id"`
	AggregateType string    `db:"aggregate_type"`
	AggregateID   *uint64   `db:"aggregate_id"`
	Type          string    `db:"type"`
	Payload       string    `db:"payload"`
	CreatedAt     time.Time `db:"created_at"`
}

type searchIndexSourceRow struct {
	ID             uint64     `db:"id"`
	TagID          *uint64    `db:"tag_id"`
	Title          *string    `db:"title"`
	Description    *string    `db:"description"`
	Tags           *string    `db:"tags"`
	CreatorID      uint64     `db:"creator_id"`
	AuthorNickname string     `db:"author_nickname"`
	Status         string     `db:"status"`
	Visible        string     `db:"visible"`
	IsTop          bool       `db:"is_top"`
	PublishTime    *time.Time `db:"publish_time"`
}

// OutboxSyncWorker 轮询 knowpost outbox 记录，并把它们投影到 Elasticsearch。
// WHY：仓储层已经把变更写入 outbox 表；增加这个轮询 worker，
// 可以补上最终一致性的闭环，而不需要再引入额外的强依赖外部服务。
type OutboxSyncWorker struct {
	db           *sqlx.DB
	searchSvc    *SearchService
	counter      CounterReader
	logger       *zap.Logger
	pollInterval time.Duration
	batchSize    int
}

func NewOutboxSyncWorker(db *sqlx.DB, searchSvc *SearchService, counter CounterReader, logger *zap.Logger) *OutboxSyncWorker {
	if searchSvc == nil {
		return nil
	}
	return &OutboxSyncWorker{
		db:           db,
		searchSvc:    searchSvc,
		counter:      counter,
		logger:       logger,
		pollInterval: defaultOutboxPollInterval,
		batchSize:    defaultOutboxBatchSize,
	}
}

// Start 启动轮询循环，直到上下文被取消。
func (w *OutboxSyncWorker) Start(ctx context.Context) {
	if w == nil {
		return
	}

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	if err := w.ProcessOnce(ctx); err != nil && w.logger != nil {
		w.logger.Warn("initial search outbox sync failed", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ProcessOnce(ctx); err != nil && w.logger != nil {
				w.logger.Warn("search outbox sync failed", zap.Error(err))
			}
		}
	}
}

// ProcessOnce 处理一批 outbox 事件。
// 该方法对测试和定向维护任务开放。
func (w *OutboxSyncWorker) ProcessOnce(ctx context.Context) error {
	if w == nil || w.searchSvc == nil {
		return nil
	}

	for i := 0; i < w.batchSize; i++ {
		processed, err := w.processOneLockedEvent(ctx)
		if err != nil {
			return err
		}
		if !processed {
			return nil
		}
	}

	return nil
}

func (w *OutboxSyncWorker) processOneLockedEvent(ctx context.Context) (bool, error) {
	tx, err := w.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}

	var event outboxRow
	query := `
SELECT id, aggregate_type, aggregate_id, type, payload, created_at
FROM outbox
WHERE aggregate_type = ?
ORDER BY created_at ASC, id ASC
LIMIT 1`
	if tx.DriverName() == "mysql" {
		query += " FOR UPDATE SKIP LOCKED"
	}
	err = tx.GetContext(ctx, &event, query, knowPostAggregateType)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return false, nil
	}
	if err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := w.processEvent(ctx, tx, event); err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (w *OutboxSyncWorker) processEvent(ctx context.Context, tx *sqlx.Tx, event outboxRow) error {
	switch event.Type {
	case eventTypeKnowPostDeleted:
		if event.AggregateID != nil {
			if err := w.searchSvc.DeleteDocument(ctx, strconv.FormatUint(*event.AggregateID, 10)); err != nil {
				return err
			}
		}
	default:
		postID, err := w.extractPostID(event)
		if err != nil {
			return err
		}
		doc, err := w.buildSearchDocument(ctx, postID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if delErr := w.searchSvc.DeleteDocument(ctx, strconv.FormatUint(postID, 10)); delErr != nil {
					return delErr
				}
			} else {
				return err
			}
		} else {
			if err := w.searchSvc.IndexDocument(ctx, doc); err != nil {
				return err
			}
		}
	}

	return w.deleteOutboxRow(ctx, tx, event.ID)
}

func (w *OutboxSyncWorker) extractPostID(event outboxRow) (uint64, error) {
	if event.AggregateID != nil {
		return *event.AggregateID, nil
	}

	var payload struct {
		ID uint64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
		return 0, err
	}
	if payload.ID == 0 {
		return 0, sql.ErrNoRows
	}
	return payload.ID, nil
}

func (w *OutboxSyncWorker) buildSearchDocument(ctx context.Context, postID uint64) (*SearchIndexDoc, error) {
	var row searchIndexSourceRow
	err := w.db.GetContext(ctx, &row, `
SELECT
    know_posts.id,
    know_posts.tag_id,
    know_posts.title,
    know_posts.description,
    know_posts.tags,
    know_posts.creator_id,
    know_posts.status,
    know_posts.visible,
    know_posts.is_top,
    know_posts.publish_time,
    users.nickname AS author_nickname
FROM know_posts
LEFT JOIN users ON know_posts.creator_id = users.id
WHERE know_posts.id = ?
`, postID)
	if err != nil {
		return nil, err
	}

	metrics := map[string]int32{}
	if w.counter != nil {
		counts, err := w.counter.GetCounts(ctx, "knowpost", strconv.FormatUint(postID, 10), []string{"like", "fav"})
		if err == nil && counts != nil {
			metrics = counts
		}
	}

	doc := &SearchIndexDoc{
		ID:          strconv.FormatUint(row.ID, 10),
		Title:       strings.TrimSpace(strValue(row.Title)),
		Description: strings.TrimSpace(strValue(row.Description)),
		TagID:       row.TagID,
		Tags:        parseJSONTags(row.Tags),
		AuthorID:    strconv.FormatUint(row.CreatorID, 10),
		AuthorName:  row.AuthorNickname,
		LikeCount:   int64(metrics["like"]),
		FavCount:    int64(metrics["fav"]),
		IsTop:       row.IsTop,
		Status:      row.Status,
		Visible:     row.Visible,
		Suggest:     buildSuggestField(row.Title, row.Tags),
	}
	if row.PublishTime != nil {
		value := row.PublishTime.UTC().Format(time.RFC3339)
		doc.PublishTime = &value
	}

	return doc, nil
}

func (w *OutboxSyncWorker) deleteOutboxRow(ctx context.Context, tx *sqlx.Tx, id uint64) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM outbox WHERE id = ?", id)
	return err
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
