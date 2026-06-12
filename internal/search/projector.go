package search

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

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
	db      sqlx.ExtContext
	indexer DocumentIndexer
	counter CounterReader
}

// NewKnowPostProjector 创建搜索索引投影器实例。
func NewKnowPostProjector(db sqlx.ExtContext, indexer DocumentIndexer, counter CounterReader) *KnowPostProjector {
	if db == nil || indexer == nil {
		return nil
	}
	return &KnowPostProjector{
		db:      db,
		indexer: indexer,
		counter: counter,
	}
}

// ProjectPayload 解析 outbox 事件的 payload JSON，执行搜索索引的 upsert 或 delete 操作。
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

// UpsertKnowPost 从 MySQL 查询知文数据，索引到搜索引擎。
func (p *KnowPostProjector) UpsertKnowPost(ctx context.Context, postID uint64) error {
	doc, err := p.buildSearchDocument(ctx, postID)
	if err != nil {
		if err == sql.ErrNoRows {
			return p.SoftDeleteKnowPost(ctx, postID)
		}
		return err
	}
	return p.indexer.IndexDocument(ctx, doc)
}

// SoftDeleteKnowPost 在搜索索引中将知文标记为 "deleted"。
func (p *KnowPostProjector) SoftDeleteKnowPost(ctx context.Context, postID uint64) error {
	return p.indexer.IndexDocument(ctx, &SearchIndexDoc{
		ID:     strconv.FormatUint(postID, 10),
		Status: "deleted",
	})
}
