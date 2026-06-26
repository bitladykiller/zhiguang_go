package counter

import (
	"context"
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
	defer func() {
		if err != nil {
			_ = tx.Rollback()
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

	return tx.Commit()
}
