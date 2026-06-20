package counter

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	counterFailureStagePublish  = "publish"
	counterFailureStageFlush    = "flush"
	counterFailureStatusPending = "pending"
)

// CounterFailedMessage 记录计数链路中未成功投递/处理的消息，供后续排障或补偿使用。
type CounterFailedMessage struct {
	ID           uint64    `db:"id"`
	Stage        string    `db:"stage"`
	Topic        string    `db:"topic"`
	MessageKey   string    `db:"message_key"`
	EntityType   string    `db:"entity_type"`
	EntityID     string    `db:"entity_id"`
	Metric       string    `db:"metric"`
	Delta        int       `db:"delta"`
	Payload      string    `db:"payload"`
	ErrorMessage string    `db:"error_message"`
	RetryCount   int       `db:"retry_count"`
	Status       string    `db:"status"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// CounterFailureRecorder 抽象失败消息持久化能力，便于测试时注入 stub。
type CounterFailureRecorder interface {
	Create(ctx context.Context, message *CounterFailedMessage) error
	CreateBatch(ctx context.Context, messages []*CounterFailedMessage) error
	ListPending(ctx context.Context, limit, offset int) ([]*CounterFailedMessage, error)
	UpdateStatus(ctx context.Context, id uint64, status, errorMessage string) error
}

// CounterFailedMessageRepository 将 counter 失败消息写入 MySQL。
type CounterFailedMessageRepository struct {
	db *sqlx.DB
}

// NewCounterFailedMessageRepository 创建 CounterFailedMessageRepository 实例。
//
// 参数:
//   - db: *sqlx.DB，用于写入失败消息的数据库连接
//
// 返回值:
//   - *CounterFailedMessageRepository: 已初始化的仓储实例；db 为 nil 时返回 nil
func NewCounterFailedMessageRepository(db *sqlx.DB) *CounterFailedMessageRepository {
	if db == nil {
		return nil
	}
	return &CounterFailedMessageRepository{db: db}
}

func (r *CounterFailedMessageRepository) Create(ctx context.Context, message *CounterFailedMessage) error {
	if r == nil || r.db == nil || message == nil {
		return nil
	}
	_, err := sqlx.NamedExecContext(ctx, r.db, `
INSERT INTO counter_failed_messages (
    stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status
) VALUES (
    :stage, :topic, :message_key, :entity_type, :entity_id, :metric, :delta, :payload, :error_message, :retry_count, :status
)`, message)
	return err
}

func (r *CounterFailedMessageRepository) CreateBatch(ctx context.Context, messages []*CounterFailedMessage) error {
	if r == nil || r.db == nil || len(messages) == 0 {
		return nil
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			if rbErr := tx.Rollback(); rbErr != nil {
				// Rollback 失败时仅记录，不影响主流程
				_ = fmt.Errorf("failure recorder rollback failed: %w", rbErr)
			}
		}
	}()

	for _, message := range messages {
		if message == nil {
			continue
		}
		if _, err := sqlx.NamedExecContext(ctx, tx, `
INSERT INTO counter_failed_messages (
    stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status
) VALUES (
    :stage, :topic, :message_key, :entity_type, :entity_id, :metric, :delta, :payload, :error_message, :retry_count, :status
)`, message); err != nil {
			return err
		}
	}

	committed = true
	return tx.Commit()
}

// ListPending 查询指定数量的 pending 状态失败记录。
//
// 参数:
//   - ctx: context.Context，上下文
//   - limit: int，查询数量上限
//   - offset: int，偏移量
//
// 返回值:
//   - []*CounterFailedMessage: 失败记录列表
//   - error: 查询失败时返回错误
func (r *CounterFailedMessageRepository) ListPending(ctx context.Context, limit, offset int) ([]*CounterFailedMessage, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	var messages []*CounterFailedMessage
	if err := r.db.SelectContext(ctx, &messages, `
SELECT id, stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status, created_at, updated_at
FROM counter_failed_messages
WHERE status = 'pending'
ORDER BY id ASC
LIMIT ? OFFSET ?
`, limit, offset); err != nil {
		return nil, err
	}
	return messages, nil
}

// UpdateStatus 更新失败记录的状态和错误信息。
//
// 参数:
//   - ctx: context.Context，上下文
//   - id: uint64，记录主键
//   - status: string，新状态（"recovered" / "failed"）
//   - errorMessage: string，错误信息，空字符串表示清空
//
// 返回值:
//   - error: 更新失败时返回错误
func (r *CounterFailedMessageRepository) UpdateStatus(ctx context.Context, id uint64, status, errorMessage string) error {
	if r == nil || r.db == nil {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `
UPDATE counter_failed_messages
SET status = ?, error_message = ?, retry_count = retry_count + 1, updated_at = NOW()
WHERE id = ?
`, status, errorMessage, id)
	return err
}
