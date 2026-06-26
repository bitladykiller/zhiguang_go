package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type OutboxEvent struct {
	ID            uint64
	AggregateType string
	AggregateID   *uint64
	EventType     string
	Payload       interface{}
}

func RunInTx(
	ctx context.Context,
	db *sqlx.DB,
	mutations func(tx *sqlx.Tx) error,
	events []OutboxEvent,
) (err error) {
	tx, txErr := db.BeginTxx(ctx, nil)
	if txErr != nil {
		return txErr
	}

	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			err = fmt.Errorf("outbox: panic in mutations: %v", r)
		}
	}()

	if err = mutations(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	for _, evt := range events {
		payload, marshalErr := json.Marshal(evt.Payload)
		if marshalErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("outbox: marshal event %s: %w", evt.EventType, marshalErr)
		}
		if _, execErr := tx.ExecContext(ctx,
			"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
			evt.ID, evt.AggregateType, evt.AggregateID, evt.EventType, string(payload),
		); execErr != nil {
			_ = tx.Rollback()
			return execErr
		}
	}

	return tx.Commit()
}
