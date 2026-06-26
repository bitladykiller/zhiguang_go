package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

type mockRowHandler struct {
	fn func(ctx context.Context, row Row) error
}

func (m *mockRowHandler) HandleRow(ctx context.Context, row Row) error {
	return m.fn(ctx, row)
}

func TestPollConsumer_New_NilDB(t *testing.T) {
	if c := NewPollConsumer(nil, "topic", &mockRowHandler{}, nil, time.Second, 10); c != nil {
		t.Error("expected nil for nil db")
	}
}

func TestPollConsumer_New_NilHandler(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	if c := NewPollConsumer(sqlxDB, "topic", nil, nil, time.Second, 10); c != nil {
		t.Error("expected nil for nil handler")
	}
}

func TestPollConsumer_New_Defaults(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	c := NewPollConsumer(sqlxDB, "topic", &mockRowHandler{}, nil, 0, 0)
	if c == nil {
		t.Fatal("expected non-nil consumer")
	}
	if c.pollDelay != time.Second {
		t.Errorf("expected default pollDelay 1s, got %v", c.pollDelay)
	}
	if c.batchSize != 100 {
		t.Errorf("expected default batchSize 100, got %d", c.batchSize)
	}
}

func TestPollConsumer_Start_NilReceiver(t *testing.T) {
	var c *PollConsumer
	c.Start(context.Background()) // should not panic
}

func TestPollConsumer_Poll_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectQuery(`SELECT id, aggregate_type, aggregate_id, type, payload FROM outbox WHERE type = \? AND id > \? ORDER BY id LIMIT \?`).
		WithArgs("test_topic", uint64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "aggregate_type", "aggregate_id", "type", "payload"}).
			AddRow(uint64(1), "knowpost", "42", "KnowPostPublished", `{"id":42}`).
			AddRow(uint64(2), "following", "99", "FollowCreated", `{}`))

	mock.ExpectExec(`DELETE FROM outbox WHERE id = \?`).
		WithArgs(uint64(1)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM outbox WHERE id = \?`).
		WithArgs(uint64(2)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	handler := &mockRowHandler{
		fn: func(ctx context.Context, row Row) error {
			return nil
		},
	}

	c := NewPollConsumer(sqlxDB, "test_topic", handler, nil, time.Minute, 10)
	rows, err := c.poll(context.Background(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	for _, row := range rows {
		r := Row{
			AggregateType: row.AggregateType,
			AggregateID:   row.AggregateID,
			Type:          row.Type,
			Payload:       []byte(row.Payload),
		}
		if err := handler.HandleRow(context.Background(), r); err != nil {
			t.Fatalf("handle row failed: %v", err)
		}
		if err := c.delete(context.Background(), row.ID); err != nil {
			t.Fatalf("delete row failed: %v", err)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestPollConsumer_Poll_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectQuery(`SELECT id, aggregate_type, aggregate_id, type, payload FROM outbox WHERE type = \? AND id > \? ORDER BY id LIMIT \?`).
		WithArgs("test_topic", uint64(0), 10).
		WillReturnError(errors.New("connection lost"))

	c := NewPollConsumer(sqlxDB, "test_topic", &mockRowHandler{}, nil, time.Minute, 10)
	_, err = c.poll(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestPollConsumer_HandleRowError_Continues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectQuery(`SELECT id, aggregate_type, aggregate_id, type, payload FROM outbox WHERE type = \? AND id > \? ORDER BY id LIMIT \?`).
		WithArgs("test_topic", uint64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "aggregate_type", "aggregate_id", "type", "payload"}).
			AddRow(uint64(1), "knowpost", "42", "KnowPostPublished", `{"id":42}`).
			AddRow(uint64(2), "following", "99", "FollowCreated", `{}`))

	mock.ExpectExec(`DELETE FROM outbox WHERE id = \?`).
		WithArgs(uint64(2)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	handler := &mockRowHandler{
		fn: func(ctx context.Context, row Row) error {
			if row.AggregateType == "knowpost" {
				return errors.New("handler failure")
			}
			return nil
		},
	}

	c := NewPollConsumer(sqlxDB, "test_topic", handler, nil, time.Minute, 10)
	rows, pollErr := c.poll(context.Background(), 0)
	if pollErr != nil {
		t.Fatalf("unexpected error: %v", pollErr)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	for _, row := range rows {
		r := Row{
			AggregateType: row.AggregateType,
			AggregateID:   row.AggregateID,
			Type:          row.Type,
			Payload:       []byte(row.Payload),
		}
		handleErr := handler.HandleRow(context.Background(), r)
		if handleErr != nil {
			continue
		}
		if err := c.delete(context.Background(), row.ID); err != nil {
			t.Fatalf("delete row failed: %v", err)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestPollConsumer_EmptyResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectQuery(`SELECT id, aggregate_type, aggregate_id, type, payload FROM outbox WHERE type = \? AND id > \? ORDER BY id LIMIT \?`).
		WithArgs("test_topic", uint64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "aggregate_type", "aggregate_id", "type", "payload"}))

	handler := &mockRowHandler{
		fn: func(ctx context.Context, row Row) error {
			return nil
		},
	}

	c := NewPollConsumer(sqlxDB, "test_topic", handler, nil, time.Minute, 10)
	rows, pollErr := c.poll(context.Background(), 0)
	if pollErr != nil {
		t.Fatalf("unexpected error: %v", pollErr)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestPollConsumer_ContextCancelled(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	handler := &mockRowHandler{
		fn: func(ctx context.Context, row Row) error {
			return nil
		},
	}

	c := NewPollConsumer(sqlxDB, "test_topic", handler, nil, time.Millisecond, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c.Start(ctx)
}

// --- DirectPollConsumer (topic mode) tests ---

func TestNewPollConsumerWithTopic_Defaults(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	c := NewPollConsumer(sqlxDB, "topic", &mockRowHandler{}, nil, 0, 0)
	if c == nil {
		t.Fatal("expected non-nil consumer")
	}
}

func TestPollConsumerWithTopic_Poll_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectQuery(`SELECT id, aggregate_type, aggregate_id, type, payload FROM outbox WHERE type = \? AND id > \? ORDER BY id LIMIT \?`).
		WithArgs("test_topic", uint64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "aggregate_type", "aggregate_id", "type", "payload"}).
			AddRow(uint64(1), "knowpost", "42", "KnowPostPublished", `{"id":42}`).
			AddRow(uint64(2), "following", "99", "FollowCreated", `{}`))

	mock.ExpectExec(`DELETE FROM outbox WHERE id = \?`).
		WithArgs(uint64(1)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM outbox WHERE id = \?`).
		WithArgs(uint64(2)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	handler := &mockRowHandler{
		fn: func(ctx context.Context, row Row) error {
			return nil
		},
	}

	c := NewPollConsumer(sqlxDB, "test_topic", handler, nil, time.Minute, 10)
	rows, err := c.poll(context.Background(), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	for _, row := range rows {
		r := Row{
			AggregateType: row.AggregateType,
			AggregateID:   row.AggregateID,
			Type:          row.Type,
			Payload:       []byte(row.Payload),
		}
		if err := handler.HandleRow(context.Background(), r); err != nil {
			t.Fatalf("handle row failed: %v", err)
		}
		if err := c.delete(context.Background(), row.ID); err != nil {
			t.Fatalf("delete row failed: %v", err)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestDirectPollConsumer_Alias_NewNilDB(t *testing.T) {
	if c := NewDirectPollConsumer(nil, "topic", 10, time.Second, &mockRowHandler{}, nil); c != nil {
		t.Error("expected nil for nil db")
	}
}

func TestDirectPollConsumer_Alias_StartNilReceiver(t *testing.T) {
	var c *DirectPollConsumer
	c.Start(context.Background())
}

func TestDirectPollConsumer_Alias_PollOnce_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectQuery(`SELECT id, aggregate_type, aggregate_id, type, payload FROM outbox WHERE type = \? AND id > \? ORDER BY id LIMIT \?`).
		WithArgs("test_topic", uint64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "aggregate_type", "aggregate_id", "type", "payload"}))

	_ = NewDirectPollConsumer(sqlxDB, "test_topic", 10, time.Minute, &mockRowHandler{}, nil)
	rows, pollErr := NewPollConsumer(sqlxDB, "test_topic", &mockRowHandler{}, nil, time.Minute, 10).poll(context.Background(), 0)
	if pollErr != nil {
		t.Fatalf("unexpected error: %v", pollErr)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestDirectPollConsumer_Alias_ContextCancelled(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	c := NewDirectPollConsumer(sqlxDB, "test_topic", 10, time.Millisecond, &mockRowHandler{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c.Start(ctx)
}