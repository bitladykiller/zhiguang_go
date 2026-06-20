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
	Payload       json.RawMessage
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
			rbErr := tx.Rollback()
			if rbErr != nil {
				err = fmt.Errorf("outbox: panic: %v (rollback: %v, mutations original: %w)", r, rbErr, err)
			} else {
				err = fmt.Errorf("outbox: panic in mutations: %v (mutations original: %w)", r, err)
			}
		}
	}()

	if err = mutations(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("outbox: rollback after mutations error: %w (original: %v)", rbErr, err)
		}
		return err
	}

	for _, evt := range events {
		payload, marshalErr := json.Marshal(evt.Payload)
		if marshalErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				return fmt.Errorf("outbox: rollback after marshal error: %w (original: %w)", rbErr, marshalErr)
			}
			return fmt.Errorf("outbox: marshal event %s: %w", evt.EventType, marshalErr)
		}
		if _, execErr := tx.ExecContext(ctx,
			"INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload) VALUES (?, ?, ?, ?, ?)",
			evt.ID, evt.AggregateType, evt.AggregateID, evt.EventType, string(payload),
		); execErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				return fmt.Errorf("outbox: rollback after exec error: %w (original: %v)", rbErr, execErr)
			}
			return execErr
		}
	}

	return tx.Commit()
}
