package counter

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	counterFailureStagePublish  = "publish"
	counterFailureStageApply    = "apply"
	counterFailureStageFlush    = "flush" // 兼容旧数据，新的失败任务统一写 apply。
	counterFailureStatusPending = "pending"
	counterFailureStatusWorking = "working"
	counterFailureStatusDone    = "done"
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
	NextRetryAt  time.Time `db:"next_retry_at"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// CounterFailureRecorder 抽象失败消息持久化能力，便于测试时注入 stub。
type CounterFailureRecorder interface {
	Create(ctx context.Context, message *CounterFailedMessage) error
	CreateBatch(ctx context.Context, messages []*CounterFailedMessage) error
}

// CounterFailureTaskStore 提供失败任务扫描与状态更新能力。
//
// 当前设计中：
//   - publish 失败任务：后台补发 Kafka
//   - apply 失败任务：后台按 entity + metric 从 bitmap 做定点修复
type CounterFailureTaskStore interface {
	ClaimPending(ctx context.Context, limit int) ([]*CounterFailedMessage, error)
	MarkDone(ctx context.Context, id uint64) error
	MarkRetry(ctx context.Context, id uint64, retryCount int, nextRetryAt time.Time, errorMessage string) error
	DeleteDoneBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}

// CounterFailedMessageRepository 将 counter 失败消息写入 MySQL。
type CounterFailedMessageRepository struct {
	db *sqlx.DB
}

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
    stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status, next_retry_at
) VALUES (
    :stage, :topic, :message_key, :entity_type, :entity_id, :metric, :delta, :payload, :error_message, :retry_count, :status, :next_retry_at
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
	defer tx.Rollback()

	for _, message := range messages {
		if message == nil {
			continue
		}
		if _, err := sqlx.NamedExecContext(ctx, tx, `
INSERT INTO counter_failed_messages (
    stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status, next_retry_at
) VALUES (
    :stage, :topic, :message_key, :entity_type, :entity_id, :metric, :delta, :payload, :error_message, :retry_count, :status, :next_retry_at
)`, message); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *CounterFailedMessageRepository) ClaimPending(ctx context.Context, limit int) ([]*CounterFailedMessage, error) {
	if r == nil || r.db == nil || limit <= 0 {
		return nil, nil
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tasks := make([]CounterFailedMessage, 0, limit)
	if err := tx.SelectContext(ctx, &tasks, `
SELECT id, stage, topic, message_key, entity_type, entity_id, metric, delta, payload, error_message, retry_count, status, next_retry_at, created_at, updated_at
FROM counter_failed_messages
WHERE status = ? AND next_retry_at <= CURRENT_TIMESTAMP(3)
ORDER BY id ASC
LIMIT ?
FOR UPDATE SKIP LOCKED
`, counterFailureStatusPending, limit); err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, tx.Commit()
	}

	query, args, err := sqlx.In(`
UPDATE counter_failed_messages
SET status = ?, updated_at = CURRENT_TIMESTAMP(3)
WHERE id IN (?)
`, counterFailureStatusWorking, extractCounterFailureIDs(tasks))
	if err != nil {
		return nil, err
	}
	query = tx.Rebind(query)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return nil, err
	}

	result := make([]*CounterFailedMessage, 0, len(tasks))
	for i := range tasks {
		tasks[i].Status = counterFailureStatusWorking
		task := tasks[i]
		result = append(result, &task)
	}
	return result, tx.Commit()
}

func (r *CounterFailedMessageRepository) MarkDone(ctx context.Context, id uint64) error {
	if r == nil || r.db == nil || id == 0 {
		return nil
	}

	_, err := r.db.ExecContext(ctx, `
UPDATE counter_failed_messages
SET status = ?, updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
`, counterFailureStatusDone, id)
	return err
}

func (r *CounterFailedMessageRepository) MarkRetry(ctx context.Context, id uint64, retryCount int, nextRetryAt time.Time, errorMessage string) error {
	if r == nil || r.db == nil || id == 0 {
		return nil
	}

	_, err := r.db.ExecContext(ctx, `
UPDATE counter_failed_messages
SET retry_count = ?, next_retry_at = ?, error_message = ?, status = ?, updated_at = CURRENT_TIMESTAMP(3)
WHERE id = ?
`, retryCount, nextRetryAt, errorMessage, counterFailureStatusPending, id)
	return err
}

func (r *CounterFailedMessageRepository) DeleteDoneBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	if r == nil || r.db == nil || limit <= 0 {
		return 0, nil
	}

	result, err := r.db.ExecContext(ctx, `
DELETE FROM counter_failed_messages
WHERE status = ? AND updated_at < ?
ORDER BY id
LIMIT ?
`, counterFailureStatusDone, before, limit)
	if err != nil {
		return 0, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func extractCounterFailureIDs(tasks []CounterFailedMessage) []uint64 {
	ids := make([]uint64, 0, len(tasks))
	for _, task := range tasks {
		if task.ID == 0 {
			continue
		}
		ids = append(ids, task.ID)
	}
	return ids
}
