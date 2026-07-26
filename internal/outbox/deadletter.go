package outbox

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// DeadLetterRepository 把重试耗尽的 outbox 消息持久化到失败表。
//
// WHY 需要它：
//
//	Consumer 的策略是「重试 3 次 → 记录死信 → commit 跳过」，避免单条坏消息
//	阻塞整个分区。但此前 SetFailedMessageRecorder 从无调用方注入——
//	死信只剩一行 Warn 日志，消息本体随 commit 永久消失，没有任何补偿入口。
//	与此同时 counter 链路有自己的失败表（counter_failed_messages）。
//	同一个问题（消费失败怎么办）存在两种答案、其中一种是死的，是明显的设计残缺。
//
// 落点选择：复用 counter_failed_messages 表。
//
//	该表的列（stage/topic/message_key/payload/error_message/status）本就通用，
//	stage 用 "outbox" 区分来源；为一张语义相同的表再造一份 DDL 徒增维护面。
//	表名带 counter_ 前缀是历史遗留，重命名需要迁移，收益不抵；在此显式注明。
type DeadLetterRepository struct {
	db *sqlx.DB
}

// NewDeadLetterRepository 创建死信仓储；db 为 nil 时返回 nil（Consumer 会退回仅日志）。
func NewDeadLetterRepository(db *sqlx.DB) *DeadLetterRepository {
	if db == nil {
		return nil
	}
	return &DeadLetterRepository{db: db}
}

// Create 实现 FailedMessageRecorder。
func (r *DeadLetterRepository) Create(ctx context.Context, topic string, messageKey string, payload []byte, cause error) error {
	if r == nil || r.db == nil {
		return nil
	}
	errMsg := ""
	if cause != nil {
		errMsg = cause.Error()
	}
	_, err := r.db.ExecContext(ctx, `
INSERT INTO counter_failed_messages
    (stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status)
VALUES ('outbox', ?, ?, '', '', '', 0, ?, ?, 0, 'pending')`,
		topic, messageKey, string(payload), errMsg)
	if err != nil {
		return fmt.Errorf("record outbox dead letter: %w", err)
	}
	return nil
}

// 编译期断言。
var _ FailedMessageRecorder = (*DeadLetterRepository)(nil)
