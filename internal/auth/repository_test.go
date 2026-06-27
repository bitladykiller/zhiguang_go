package auth

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

func TestNewAuthRepository(t *testing.T) {
	t.Run("with logger", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())
		if repo == nil {
			t.Fatal("repo is nil")
		}
	})
}

func TestWithDB(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "mysql")
	repo := NewAuthRepository(sqlxDB, zap.NewNop())
	newRepo := repo.WithDB(sqlxDB)
	if newRepo == repo {
		t.Fatal("expected new instance")
	}
	if newRepo.db != sqlxDB {
		t.Fatal("db not set correctly")
	}
}

func TestIdentifierExists(t *testing.T) {
	t.Run("phone exists", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		mock.ExpectQuery("SELECT COUNT\\(1\\) FROM users WHERE phone = \\?").
			WithArgs("13800138000").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		exists := repo.IdentifierExists(context.Background(), IdentifierPhone, "13800138000")
		if !exists {
			t.Fatal("expected true")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("email exists", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		mock.ExpectQuery("SELECT COUNT\\(1\\) FROM users WHERE email = \\?").
			WithArgs("test@example.com").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		exists := repo.IdentifierExists(context.Background(), IdentifierEmail, "test@example.com")
		if !exists {
			t.Fatal("expected true")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("not exists", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		mock.ExpectQuery("SELECT COUNT\\(1\\) FROM users WHERE phone = \\?").
			WithArgs("13800138001").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

		exists := repo.IdentifierExists(context.Background(), IdentifierPhone, "13800138001")
		if exists {
			t.Fatal("expected false")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		exists := repo.IdentifierExists(context.Background(), "UNKNOWN", "test")
		if exists {
			t.Fatal("expected false for unknown type")
		}
	})

	t.Run("db error returns false", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		mock.ExpectQuery("SELECT COUNT\\(1\\) FROM users WHERE phone = \\?").
			WithArgs("13800138002").
			WillReturnError(sql.ErrConnDone)

		exists := repo.IdentifierExists(context.Background(), IdentifierPhone, "13800138002")
		if exists {
			t.Fatal("expected false on db error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestRecordLoginLog(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		uid := uint64(1)
		ip := "127.0.0.1"
		ua := "test-agent"
		log := &LoginLog{
			UserID:     &uid,
			Identifier: "13800138000",
			Channel:    ChannelPassword,
			IP:         &ip,
			UserAgent:  &ua,
			Status:     LoginStatusSuccess,
		}

		mock.ExpectExec("INSERT INTO login_logs").
			WillReturnResult(sqlmock.NewResult(1, 1))

		repo.RecordLoginLog(context.Background(), log)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("db error is swallowed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		sqlxDB := sqlx.NewDb(db, "mysql")
		repo := NewAuthRepository(sqlxDB, zap.NewNop())

		uid := uint64(1)
		log := &LoginLog{
			UserID:     &uid,
			Identifier: "13800138000",
			Channel:    ChannelPassword,
			Status:     LoginStatusSuccess,
		}

		mock.ExpectExec("INSERT INTO login_logs").
			WillReturnError(sql.ErrConnDone)

		repo.RecordLoginLog(context.Background(), log)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}