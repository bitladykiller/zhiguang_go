package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

const schemaMigrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(64) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

func RunMigrations(db *sqlx.DB, logger *zap.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, schemaMigrationsTable); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	migrationDir := "db/migrations"
	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migration dir: %w", err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, f := range files {
		version := strings.TrimSuffix(f, ".sql")

		var count int
		if err := db.GetContext(ctx, &count, "SELECT COUNT(1) FROM schema_migrations WHERE version = ?", version); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if count > 0 {
			continue
		}

		path := filepath.Join(migrationDir, f)
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", f, err)
		}

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", version, err)
		}
		committed := false
		defer func() {
			if r := recover(); r != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					logger.Warn("migration rollback on panic failed", zap.String("version", version), zap.Error(rbErr))
				}
				panic(r) // re-panic after rollback
			}
			if !committed {
				if rbErr := tx.Rollback(); rbErr != nil {
					logger.Warn("migration rollback failed", zap.String("version", version), zap.Error(rbErr))
				}
			}
		}()

		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("apply migration %s: %w", version, err)
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("record migration %s: %w", version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
		committed = true

		logger.Info("applied database migration", zap.String("version", version))
	}

	return nil
}