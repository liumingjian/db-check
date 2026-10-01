// Package sessions owns sign-in sessions: opaque random tokens of which only
// the SHA-256 is stored. A session expires Lifetime after it was issued;
// activity does not extend it.
package sessions

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
)

const Lifetime = 7 * 24 * time.Hour

// ErrNoSession answers a token that is unknown, signed out, or expired.
var ErrNoSession = apierr.Unauthorized("登录已失效，请重新登录")

// Issue opens a session for a user and returns its token.
func Issue(ctx context.Context, q store.Querier, userID string, now time.Time) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	// Hex keeps the token valid as a WebSocket subprotocol, which is how
	// the browser passes it on WS connections.
	token := hex.EncodeToString(raw)
	// Expired sessions are useless; pruning here keeps the table small.
	if _, err := q.ExecContext(ctx, "DELETE FROM sessions WHERE expires_at <= ?", store.FormatTime(now)); err != nil {
		return "", fmt.Errorf("prune sessions: %w", err)
	}
	_, err := q.ExecContext(ctx,
		"INSERT INTO sessions (token_hash, user_id, issued_at, expires_at) VALUES (?, ?, ?, ?)",
		hashToken(token), userID, store.FormatTime(now), store.FormatTime(now.Add(Lifetime)))
	if err != nil {
		return "", fmt.Errorf("issue session: %w", err)
	}
	return token, nil
}

// Resolve returns the user ID behind a live session token, or ErrNoSession.
func Resolve(ctx context.Context, q store.Querier, token string, now time.Time) (string, error) {
	var userID, expiresAt string
	err := q.QueryRowContext(ctx, "SELECT user_id, expires_at FROM sessions WHERE token_hash = ?", hashToken(token)).
		Scan(&userID, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNoSession
	}
	if err != nil {
		return "", err
	}
	if store.FormatTime(now) >= expiresAt {
		return "", ErrNoSession
	}
	return userID, nil
}

// Revoke ends one session; an unknown token is not an error.
func Revoke(ctx context.Context, q store.Querier, token string) error {
	_, err := q.ExecContext(ctx, "DELETE FROM sessions WHERE token_hash = ?", hashToken(token))
	return err
}

// RevokeAll ends every session of a user, as disabling the user or resetting
// their password must.
func RevokeAll(ctx context.Context, q store.Querier, userID string) error {
	_, err := q.ExecContext(ctx, "DELETE FROM sessions WHERE user_id = ?", userID)
	return err
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
