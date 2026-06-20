package outbox

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestRunInTx_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO outbox \(id, aggregate_type, aggregate_id, type, payload\) VALUES \(\?, \?, \?, \?, \?\)`).
		WithArgs(uint64(1), "knowpost", uint64Ptr(42), "KnowPostPublished", `{"id":42}`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	payload, _ := json.Marshal(map[string]int{"id": 42})
	aggID := uint64(42)
	err = RunInTx(context.Background(), sqlxDB, func(tx *sqlx.Tx) error {
		return nil
	}, []OutboxEvent{
		{ID: 1, AggregateType: "knowpost", AggregateID: &aggID, EventType: "KnowPostPublished", Payload: json.RawMessage(payload)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestRunInTx_MutationsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectRollback()

	mutationErr := errors.New("mutation failed")
	err = RunInTx(context.Background(), sqlxDB, func(tx *sqlx.Tx) error {
		return mutationErr
	}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestRunInTx_MarshalError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectRollback()

	// channel cannot be marshalled to JSON -> marshal error
	err = RunInTx(context.Background(), sqlxDB, func(tx *sqlx.Tx) error {
		return nil
	}, []OutboxEvent{
		{ID: 1, EventType: "BadPayload", Payload: json.RawMessage("bad")},
	})
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestRunInTx_DBErrorOnBegin(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin().WillReturnError(errors.New("connection lost"))

	err = RunInTx(context.Background(), sqlxDB, nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestRunInTx_DBErrorOnInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO outbox`).
		WithArgs(uint64(1), "", nil, "Test", `"ok"`).
		WillReturnError(errors.New("duplicate entry"))
	mock.ExpectRollback()

	err = RunInTx(context.Background(), sqlxDB, func(tx *sqlx.Tx) error {
		return nil
	}, []OutboxEvent{
		{ID: 1, AggregateType: "", AggregateID: nil, EventType: "Test", Payload: json.RawMessage(`"ok"`)},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestRunInTx_DBErrorOnCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("commit failed"))

	err = RunInTx(context.Background(), sqlxDB, func(tx *sqlx.Tx) error {
		return nil
	}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestRunInTx_NilAggregateID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO outbox`).
		WithArgs(uint64(1), "following", nil, "FollowCreated", "{}").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = RunInTx(context.Background(), sqlxDB, func(tx *sqlx.Tx) error {
		return nil
	}, []OutboxEvent{
		{ID: 1, AggregateType: "following", AggregateID: nil, EventType: "FollowCreated", Payload: json.RawMessage("{}")},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}

// --- driver.Value type assertion for sqlmock ---
func init() {
	var _ driver.Value
}