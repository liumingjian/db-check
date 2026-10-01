package releases

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
)

// Action is an admin's release status action.
type Action string

const (
	ActionPromote   Action = "promote"
	ActionDeprecate Action = "deprecate"
	ActionRevoke    Action = "revoke"
	ActionRestore   Action = "restore"
)

// transitions is the release status table
// (docs/specs/collector-and-user-management.md): the statuses each action
// starts from, and where it ends. It mirrors RELEASE_TRANSITIONS in
// web/src/lib/api/releases/contract.ts.
var transitions = map[Action]struct {
	from []Status
	to   Status
}{
	ActionPromote:   {[]Status{StatusPreRelease, StatusDeprecated}, StatusLatest},
	ActionDeprecate: {[]Status{StatusLatest, StatusDeprecated, StatusPreRelease}, StatusDeprecated},
	ActionRevoke:    {[]Status{StatusPreRelease, StatusLatest, StatusDeprecated}, StatusRevoked},
	ActionRestore:   {[]Status{StatusRevoked}, StatusDeprecated},
}

// ErrUnknownAction is returned for an action outside the status table.
var ErrUnknownAction = apierr.NotFound("不支持的版本操作")

// StatusChange is an action on one release; Reason is for revoke only.
type StatusChange struct {
	Version string
	Action  Action
	Reason  string
}

// ChangeStatus applies one row of the status table to a release. Promoting
// demotes the previous latest release; revoking needs a reason, kept as the
// revocation reason; leaving the platform without a latest release is
// valid. Run it in a transaction: the demotion and the change go together.
func ChangeStatus(ctx context.Context, q store.Querier, c StatusChange) error {
	version := c.Version
	t, ok := transitions[c.Action]
	if !ok {
		return ErrUnknownAction
	}
	reason := strings.TrimSpace(c.Reason)
	if c.Action == ActionRevoke && reason == "" {
		return apierr.Invalid("请填写撤回原因")
	}
	if c.Action != ActionRevoke {
		reason = ""
	}

	var current Status
	err := q.QueryRowContext(ctx, "SELECT status FROM releases WHERE version = ?", version).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return notFound(version)
	}
	if err != nil {
		return err
	}
	if !slices.Contains(t.from, current) {
		return apierr.Invalid(fmt.Sprintf("版本 %s 当前状态不允许此操作", version))
	}
	if t.to == StatusLatest {
		if err := demoteLatest(ctx, q); err != nil {
			return err
		}
	}
	_, err = q.ExecContext(ctx, "UPDATE releases SET status = ?, revoke_reason = ? WHERE version = ?", t.to, store.NullIfEmpty(reason), version)
	return err
}
