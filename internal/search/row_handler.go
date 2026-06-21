package search

import (
	"context"

	"github.com/zhiguang/app/internal/outbox"
)

// SearchRowHandler 实现 outbox.RowHandler，将 outbox 事件投影到 ES 索引。
//
// 这是 search 包对通用 outbox 消费框架的适配器。
// 只关心 AggregateType 为 "knowpost" 的行，其他类型静默跳过。
type SearchRowHandler struct {
	Projector *KnowPostProjector
}

// HandleRow 处理单条 outbox 行，调用 projector 执行 ES 索引更新。
func (h *SearchRowHandler) HandleRow(ctx context.Context, row outbox.Row) error {
	if row.AggregateType != "knowpost" {
		return nil
	}
	return h.Projector.ProjectPayload(ctx, row.Payload)
}
