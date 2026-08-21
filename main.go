package calls

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CallState string

const (
	StateQueued   CallState = "queued"
	StateDialing  CallState = "dialing"
	StateAnswered CallState = "answered"
	StateBridged  CallState = "bridged"
	StateEnded    CallState = "ended"
)

type Event struct {
	ID        string
	CallID    string
	Type      string
	Timestamp time.Time
}

type Service struct {
	db *pgxpool.Pool
}

func NewService(db *pgxpool.Pool) *Service {
	return &Service{db: db}
}

func (s *Service) HandleEvent(ctx context.Context, event Event) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Idempotency:
	// the same NATS/SIP event can safely be delivered more than once.
	tag, err := tx.Exec(
		ctx,
		`
		INSERT INTO processed_events (event_id, call_id, processed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (event_id) DO NOTHING
		`,
		event.ID,
		event.CallID,
	)
	if err != nil {
		return fmt.Errorf("record event: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var current CallState

	err = tx.QueryRow(
		ctx,
		`
		SELECT state
		FROM calls
		WHERE id = $1
		FOR UPDATE
		`,
		event.CallID,
	).Scan(&current)

	if err != nil {
		return fmt.Errorf("load call: %w", err)
	}

	next, err := transition(current, event.Type)
	if err != nil {
		return err
	}

	if next == current {
		return tx.Commit(ctx)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE calls
		SET state = $2,
		    updated_at = NOW()
		WHERE id = $1
		`,
		event.CallID,
		next,
	)
	if err != nil {
		return fmt.Errorf("update call: %w", err)
	}

	return tx.Commit(ctx)
}

func transition(current CallState, eventType string) (CallState, error) {
	switch eventType {

	case "call.started":
		if current == StateQueued {
			return StateDialing, nil
		}

	case "call.answered":
		if current == StateDialing {
			return StateAnswered, nil
		}

	case "call.bridged":
		if current == StateAnswered {
			return StateBridged, nil
		}

	case "call.ended":
		return StateEnded, nil
	}

	// Late/out-of-order telephony event.
	// Ignore transitions that would move the call backwards.
	if current == StateEnded {
		return current, nil
	}

	return current, errors.New("invalid state transition")
}
