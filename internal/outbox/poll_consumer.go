package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/contextutil"
)

// PollConsumer 是 Canal 不可用时的降级方案：直接从 MySQL outbox 表轮询事件。
//
// 当 Canal 桥接器未启用时，Relation / Search 等模块仍然需要消费 outbox 事件。
// PollConsumer 通过 SQL 轮询替代 Kafka + Canal 链路，保证事件不会堆积在 outbox 表。
//
// 消费流程：
//  1. 按 topic 轮询 outbox 表中未处理的事件（按 ID 顺序读取）
//  2. 对每行调用 handler.HandleRow
//  3. 处理成功后删除该行
//  4. 处理失败记录日志，继续处理下一行（不阻塞轮询循环）
//
// 设计决策：
//   - 先处理再删除（而非先删除再处理），避免处理成功但删除失败的数据丢失。
//     若删除失败，下次轮询会再次读取并处理同一行（至少一次语义，需要业务幂等）。
//   - 每次轮询都是独立事务，不跨行持有锁。
type PollConsumer struct {
	db          *sqlx.DB
	topic       string
	handler     RowHandler
	logger      *zap.Logger
	pollDelay   time.Duration
	batchSize   int
}

// NewPollConsumer 创建 outbox 轮询消费者实例。
//
// 参数：
//   - db: 数据库连接池
//   - topic: 要消费的 outbox 事件类型（如 "following"、"knowpost"）
//   - handler: 行处理器，实现 RowHandler 接口
//   - logger: 日志记录器，可为 nil
//   - pollDelay: 轮询间隔，无事件时等待此时间后重试
//   - batchSize: 每次轮询的最大行数
//
// 返回值：*PollConsumer
func NewPollConsumer(db *sqlx.DB, topic string, handler RowHandler, logger *zap.Logger, pollDelay time.Duration, batchSize int) *PollConsumer {
	if db == nil || handler == nil {
		return nil
	}
	if pollDelay <= 0 {
		pollDelay = time.Second
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	return &PollConsumer{
		db:        db,
		topic:     topic,
		handler:   handler,
		logger:    logger,
		pollDelay: pollDelay,
		batchSize: batchSize,
	}
}

// Start 启动轮询消费循环，阻塞直到 ctx 取消。
func (c *PollConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}

	var lastID uint64
	for {
		rows, err := c.poll(ctx, lastID)
		if err != nil {
			c.logWarn("poll outbox rows failed", err)
			if !contextutil.Sleep(ctx, c.pollDelay) {
				return
			}
			continue
		}

		if len(rows) == 0 {
			if !contextutil.Sleep(ctx, c.pollDelay) {
				return
			}
			continue
		}

		for _, row := range rows {
			if ctx.Err() != nil {
				return
			}

			r := Row{
				AggregateType: row.AggregateType,
				AggregateID:   row.AggregateID,
				Type:          row.Type,
				Payload:       []byte(row.Payload),
			}
			if handleErr := c.handler.HandleRow(ctx, r); handleErr != nil {
				c.logWarn(fmt.Sprintf("handle outbox row %d failed", row.ID), handleErr)
				continue
			}

			if deleteErr := c.delete(ctx, row.ID); deleteErr != nil {
				c.logWarn(fmt.Sprintf("delete outbox row %d failed", row.ID), deleteErr)
			}
			lastID = row.ID
		}
	}
}

type outboxRow struct {
	ID            uint64 `db:"id"`
	AggregateType string `db:"aggregate_type"`
	AggregateID   string `db:"aggregate_id"`
	Type          string `db:"type"`
	Payload       string `db:"payload"`
}

func (c *PollConsumer) poll(ctx context.Context, afterID uint64) ([]outboxRow, error) {
	var rows []outboxRow
	err := c.db.SelectContext(ctx, &rows,
		`SELECT id, aggregate_type, aggregate_id, type, payload
		 FROM outbox
		 WHERE type = ? AND id > ?
		 ORDER BY id
		 LIMIT ?`,
		c.topic, afterID, c.batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("poll outbox: %w", err)
	}
	return rows, nil
}

func (c *PollConsumer) delete(ctx context.Context, id uint64) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM outbox WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete outbox row %d: %w", id, err)
	}
	return nil
}

func (c *PollConsumer) logWarn(msg string, err error) {
	if c.logger != nil {
		c.logger.Warn(msg, zap.Error(err))
	}
}