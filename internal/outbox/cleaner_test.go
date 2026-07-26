package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

func newMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	raw, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return sqlx.NewDb(raw, "mysql"), mock
}

// TestCleaner_BatchesUntilDrained 验证分批删除直到不足一批为止。
//
// 标准部署下 outbox 表的消费者是 binlog，行插入后永不被读取或删除；
// Cleaner 是唯一的回收机制。分批（LIMIT）避免一条大事务长时间持锁。
func TestCleaner_BatchesUntilDrained(t *testing.T) {
	db, mock := newMockDB(t)
	c := NewCleaner(db, CleanerConfig{Retention: time.Hour, Interval: time.Hour, Batch: 2}, zap.NewNop())

	// 第一批删满 2 行 → 继续；第二批删 1 行（< batch）→ 停止
	mock.ExpectExec(`DELETE FROM outbox WHERE created_at < \? LIMIT \?`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM outbox WHERE created_at < \? LIMIT \?`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	c.cleanOnce(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// TestCleaner_StopsOnError 验证删除失败时本轮终止（下轮重试），不 panic 不死循环。
func TestCleaner_StopsOnError(t *testing.T) {
	db, mock := newMockDB(t)
	c := NewCleaner(db, CleanerConfig{Batch: 100}, zap.NewNop())

	mock.ExpectExec(`DELETE FROM outbox`).WillReturnError(errors.New("lock wait timeout"))

	c.cleanOnce(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// TestCleaner_NilDB 验证 db 为 nil 时构造器返回 nil（bootstrap 跳过注册）。
func TestCleaner_NilDB(t *testing.T) {
	if c := NewCleaner(nil, CleanerConfig{}, nil); c != nil {
		t.Fatal("nil db should yield nil cleaner")
	}
	var c *Cleaner
	c.Start(context.Background()) // nil 接收者安全
}

// TestCleaner_StartHonorsContext 验证 Start 阻塞语义与停机响应（BackgroundRunner 契约）。
func TestCleaner_StartHonorsContext(t *testing.T) {
	db, mock := newMockDB(t)
	// 启动即清一轮
	mock.ExpectExec(`DELETE FROM outbox`).WillReturnResult(sqlmock.NewResult(0, 0))

	c := NewCleaner(db, CleanerConfig{Interval: time.Hour, Batch: 10}, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { c.Start(ctx); close(done) }()

	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

// TestDeadLetterRepository_Create 验证死信落库的字段与语义。
func TestDeadLetterRepository_Create(t *testing.T) {
	db, mock := newMockDB(t)
	r := NewDeadLetterRepository(db)

	mock.ExpectExec(`INSERT INTO counter_failed_messages`).
		WithArgs("canal-outbox", "key-1", `{"id":42}`, "boom").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := r.Create(context.Background(), "canal-outbox", "key-1", []byte(`{"id":42}`), errors.New("boom")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}

	// nil 仓储 / nil db 安全
	var nilRepo *DeadLetterRepository
	if err := nilRepo.Create(context.Background(), "t", "k", nil, nil); err != nil {
		t.Fatalf("nil repo: %v", err)
	}
	if NewDeadLetterRepository(nil) != nil {
		t.Fatal("nil db should yield nil repository")
	}
}
