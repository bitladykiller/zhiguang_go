// Package outbox 提供事务内 Outbox 模式的通用抽象。
//
// 核心函数 RunInTx 封装了"在数据库事务内执行业务变更 + 写入 outbox 事件"的完整流程，
// 被 knowpost 和 relation 模块共享使用。
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

// OutboxEvent 表示一条待写入 outbox 表的事件。
type OutboxEvent struct {
	ID            uint64      // outbox 行 ID（雪花算法生成）
	AggregateType string      // 聚合类型，如 "knowpost"、"following"
	AggregateID   *uint64     // 聚合根 ID，可为 nil
	EventType     string      // 事件类型，如 "KnowPostPublished"、"FollowCreated"
	Payload       interface{} // 业务载荷，会被 json.Marshal 序列化
}

// RunInTx 在数据库事务内执行业务变更，并将 outbox 事件写入同一事务。
//
// 流程：
//  1. 开启数据库事务
//  2. 调用 mutations 执行业务变更（接收事务句柄 tx，调用方应使用 s.repo.WithDB(tx)
//     把自己的仓储绑定到事务上，确保业务变更与 outbox 写入在同一事务内）
//  3. 对每个 OutboxEvent 序列化 Payload 并通过 tx 写入 outbox 表
//  4. 提交事务
//
// 任何步骤失败都会回滚事务。mutations 中 panic 会被 recover 并回滚事务后再重新抛出。
//
// 参数：
//   - ctx: 上下文
//   - db: 数据库连接池
//   - mutations: 业务变更函数，接收事务句柄 *sqlx.Tx
//   - events: 需要写入 outbox 表的事件列表
func RunInTx(
	ctx context.Context,
	db *sqlx.DB,
	mutations func(tx *sqlx.Tx) error,
	events []OutboxEvent,
) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("outbox: rollback after panic failed: %v", rbErr)
			}
			panic(r) // 重新抛出，让上层 recover 处理
		}
	}()

	if err := mutations(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Printf("outbox: rollback after mutation error failed: %v", rbErr)
		}
		return err
	}

	for _, evt := range events {
		payload, err := json.Marshal(evt.Payload)
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("outbox: rollback after marshal error failed: %v", rbErr)
			}
			return fmt.Errorf("outbox: marshal event %s: %w", evt.EventType, err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
			evt.ID, evt.AggregateType, evt.AggregateID, evt.EventType, string(payload),
		); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("outbox: rollback after insert error failed: %v", rbErr)
			}
			return err
		}
	}

	return tx.Commit()
}
