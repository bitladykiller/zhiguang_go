package outbox

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/zhiguang/app/pkg/contextutil"
)

// DirectPollConsumer 将 outbox 表中的事件行轮询拉取出来交给 RowHandler 处理。
type DirectPollConsumer struct {
	db           *sqlx.DB
	topicName    string
	batchSize    int
	pollInterval time.Duration
	handler      RowHandler
	logger       *zap.Logger
}

func NewDirectPollConsumer(
	db *sqlx.DB,
	topicName string,
	batchSize int,
	pollInterval time.Duration,
	handler RowHandler,
	logger *zap.Logger,
) *DirectPollConsumer {
	if db == nil || handler == nil {
		return nil
	}
	return &DirectPollConsumer{
		db:           db,
		topicName:    topicName,
		batchSize:    batchSize,
		pollInterval: pollInterval,
		handler:      handler,
		logger:       logger,
	}
}

func (c *DirectPollConsumer) Start(ctx context.Context) {
	if c == nil {
		return
	}

	query := "SELECT id, uuid, topic, payload, created_at FROM outbox WHERE topic = ? AND id > ? ORDER BY id LIMIT ?"
	deleteQuery := "DELETE FROM outbox WHERE id = ?"

	var lastID uint64

	for {
		if !contextutil.Sleep(ctx, c.pollInterval) {
			c.logInfo("direct poll consumer stopped")
			return
		}

		if err := c.pollOnce(ctx, query, deleteQuery, &lastID); err != nil {
			c.logWarn("direct poll iteration failed", err)
		}
	}
}

func (c *DirectPollConsumer) pollOnce(ctx context.Context, query, deleteQuery string, lastID *uint64) error {
	rows, err := c.db.QueryxContext(ctx, query, c.topicName, *lastID, c.batchSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id uint64
		var uuid, topic string
		var payload []byte
		var createdAt time.Time

		if err := rows.Scan(&id, &uuid, &topic, &payload, &createdAt); err != nil {
			c.logWarn("scan outbox row failed", err)
			continue
		}

		row := Row{
			Payload: payload,
		}

		if err := c.handler.HandleRow(ctx, row); err != nil {
			c.logWarn("handle outbox row failed, will retry next poll", err)
			continue
		}

		if _, err := c.db.ExecContext(ctx, deleteQuery, id); err != nil {
			c.logWarn("delete outbox row failed", err)
		}

		*lastID = id
	}

	return rows.Err()
}

func (c *DirectPollConsumer) logWarn(msg string, err error) {
	if c.logger != nil {
		c.logger.Warn(msg, zap.String("topic", c.topicName), zap.Error(err))
	}
}

func (c *DirectPollConsumer) logInfo(msg string) {
	if c.logger != nil {
		c.logger.Info(msg, zap.String("topic", c.topicName))
	}
}
