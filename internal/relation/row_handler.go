package relation

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/zhiguang/app/internal/outbox"
)

// RelationRowHandler 实现 outbox.RowHandler，将 outbox 事件路由到 EventProcessor。
//
// 这是 relation 包对通用 outbox 消费框架的适配器。
// 只关心 AggregateType 为 "following" 且 Type 为 FollowCreated/FollowCanceled 的行。
type RelationRowHandler struct {
	Processor *EventProcessor
}

// HandleRow 处理单条 outbox 行，解析 RelationEvent 并调用 EventProcessor。
func (h *RelationRowHandler) HandleRow(ctx context.Context, row outbox.Row) error {
	if row.AggregateType != "following" {
		return nil
	}
	if row.Type != "FollowCreated" && row.Type != "FollowCanceled" {
		return nil
	}

	var evt RelationEvent
	if err := json.Unmarshal(row.Payload, &evt); err != nil {
		return fmt.Errorf("row handler: unmarshal event: %w", err)
	}
	return h.Processor.Process(ctx, evt)
}
