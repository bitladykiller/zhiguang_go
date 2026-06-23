package relation

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func newMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	sqlxDB := sqlx.NewDb(db, "mysql")
	return sqlxDB, mock
}

func TestUpsertFollowing(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO following (id, from_user_id, to_user_id, rel_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)")).
		WithArgs(uint64(1), uint64(10), uint64(20), 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := repo.UpsertFollowing(context.Background(), 1, 10, 20, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpsertFollowing_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO following")).
		WillReturnError(timeoutError("connection timeout"))

	err := repo.UpsertFollowing(context.Background(), 1, 10, 20, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpsertFollower(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO follower (id, to_user_id, from_user_id, rel_status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE rel_status = VALUES(rel_status), updated_at = VALUES(updated_at)")).
		WithArgs(uint64(1), uint64(20), uint64(10), 1, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := repo.UpsertFollower(context.Background(), 1, 20, 10, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpsertFollower_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO follower")).
		WillReturnError(timeoutError("connection timeout"))

	err := repo.UpsertFollower(context.Background(), 1, 20, 10, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCancelFollowing_Success(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE following SET rel_status = 0, updated_at = ? WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1")).
		WithArgs(sqlmock.AnyArg(), uint64(10), uint64(20)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	affected, err := repo.CancelFollowing(context.Background(), 10, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", affected)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCancelFollowing_NoRows(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE following SET rel_status = 0, updated_at = ? WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1")).
		WithArgs(sqlmock.AnyArg(), uint64(10), uint64(20)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	affected, err := repo.CancelFollowing(context.Background(), 10, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 0 {
		t.Fatalf("expected 0 affected row, got %d", affected)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCancelFollowing_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE following")).
		WillReturnError(timeoutError("connection timeout"))

	_, err := repo.CancelFollowing(context.Background(), 10, 20)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCancelFollower_Success(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE follower SET rel_status = 0, updated_at = ? WHERE to_user_id = ? AND from_user_id = ? AND rel_status = 1")).
		WithArgs(sqlmock.AnyArg(), uint64(20), uint64(10)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	affected, err := repo.CancelFollower(context.Background(), 20, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", affected)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCancelFollower_NoRows(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE follower SET rel_status = 0, updated_at = ? WHERE to_user_id = ? AND from_user_id = ? AND rel_status = 1")).
		WithArgs(sqlmock.AnyArg(), uint64(20), uint64(10)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	affected, err := repo.CancelFollower(context.Background(), 20, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 0 {
		t.Fatalf("expected 0 affected row, got %d", affected)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCancelFollower_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE follower")).
		WillReturnError(timeoutError("connection timeout"))

	_, err := repo.CancelFollower(context.Background(), 20, 10)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestExistsFollowing_Found(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	rows := sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(1)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM following WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1")).
		WithArgs(uint64(10), uint64(20)).
		WillReturnRows(rows)

	count, err := repo.ExistsFollowing(context.Background(), 10, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestExistsFollowing_NotFound(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	rows := sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(0)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM following WHERE from_user_id = ? AND to_user_id = ? AND rel_status = 1")).
		WithArgs(uint64(10), uint64(20)).
		WillReturnRows(rows)

	count, err := repo.ExistsFollowing(context.Background(), 10, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestExistsFollowing_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM following")).
		WillReturnError(timeoutError("connection timeout"))

	_, err := repo.ExistsFollowing(context.Background(), 10, 20)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowingRows_Success(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "from_user_id", "to_user_id", "created_at"}).
		AddRow(uint64(1), uint64(10), uint64(20), now).
		AddRow(uint64(2), uint64(10), uint64(30), now)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, from_user_id, to_user_id, created_at FROM following WHERE from_user_id = ? AND rel_status = 1 ORDER BY created_at DESC LIMIT ? OFFSET ?")).
		WithArgs(uint64(10), 10, 0).
		WillReturnRows(rows)

	result, err := repo.ListFollowingRows(context.Background(), 10, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0].ToUserID != 20 || result[1].ToUserID != 30 {
		t.Fatalf("unexpected user IDs: %v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowingRows_Empty(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	rows := sqlmock.NewRows([]string{"id", "from_user_id", "to_user_id", "created_at"})
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, from_user_id, to_user_id, created_at FROM following")).
		WithArgs(uint64(10), 10, 0).
		WillReturnRows(rows)

	result, err := repo.ListFollowingRows(context.Background(), 10, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowingRows_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, from_user_id, to_user_id, created_at FROM following")).
		WillReturnError(timeoutError("connection timeout"))

	_, err := repo.ListFollowingRows(context.Background(), 10, 10, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowerRows_Success(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "to_user_id", "from_user_id", "created_at"}).
		AddRow(uint64(1), uint64(20), uint64(10), now).
		AddRow(uint64(2), uint64(20), uint64(15), now)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, to_user_id, from_user_id, created_at FROM follower WHERE to_user_id = ? AND rel_status = 1 ORDER BY created_at DESC LIMIT ? OFFSET ?")).
		WithArgs(uint64(20), 10, 0).
		WillReturnRows(rows)

	result, err := repo.ListFollowerRows(context.Background(), 20, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0].FromUserID != 10 || result[1].FromUserID != 15 {
		t.Fatalf("unexpected from_user_ids: %v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowerRows_Empty(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	rows := sqlmock.NewRows([]string{"id", "to_user_id", "from_user_id", "created_at"})
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, to_user_id, from_user_id, created_at FROM follower")).
		WithArgs(uint64(20), 10, 0).
		WillReturnRows(rows)

	result, err := repo.ListFollowerRows(context.Background(), 20, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowerRows_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, to_user_id, from_user_id, created_at FROM follower")).
		WillReturnError(timeoutError("connection timeout"))

	_, err := repo.ListFollowerRows(context.Background(), 20, 10, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowerRowsFromFollowing_Success(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	now := time.Now()
	rows := sqlmock.NewRows([]string{"id", "to_user_id", "from_user_id", "created_at"}).
		AddRow(uint64(1), uint64(20), uint64(30), now).
		AddRow(uint64(2), uint64(20), uint64(40), now)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, to_user_id, from_user_id, created_at FROM following WHERE to_user_id = ? AND rel_status = 1 ORDER BY created_at DESC LIMIT ? OFFSET ?")).
		WithArgs(uint64(20), 10, 0).
		WillReturnRows(rows)

	result, err := repo.ListFollowerRowsFromFollowing(context.Background(), 20, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0].FromUserID != 30 || result[1].FromUserID != 40 {
		t.Fatalf("unexpected from_user_ids: %v", result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowerRowsFromFollowing_Empty(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	rows := sqlmock.NewRows([]string{"id", "to_user_id", "from_user_id", "created_at"})
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, to_user_id, from_user_id, created_at FROM following WHERE to_user_id = ? AND rel_status = 1")).
		WithArgs(uint64(20), 10, 0).
		WillReturnRows(rows)

	result, err := repo.ListFollowerRowsFromFollowing(context.Background(), 20, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFollowerRowsFromFollowing_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, to_user_id, from_user_id, created_at FROM following WHERE to_user_id = ?")).
		WillReturnError(timeoutError("connection timeout"))

	_, err := repo.ListFollowerRowsFromFollowing(context.Background(), 20, 10, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestNewRelationRepository(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)
	if repo == nil {
		t.Fatal("expected non-nil repository")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWithDB(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	repo := NewRelationRepository(db)
	newRepo := repo.WithDB(db)
	if newRepo == nil {
		t.Fatal("expected non-nil new repository")
	}
	if newRepo == repo {
		t.Fatal("expected a different instance")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

type timeoutError string

func (e timeoutError) Error() string {
	return string(e)
}