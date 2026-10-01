package testserver

import (
	"context"
	"time"

	"dbcheck/reporter/internal/downloads"
	"dbcheck/reporter/internal/store"
)

func seedDownloads(ctx context.Context, tx store.Querier, f fixture, now time.Time) error {
	var seed []downloads.Record
	if err := f.section("downloadRecords", &seed); err != nil {
		return err
	}
	for _, r := range seed {
		at, err := seedTime(r.At, now)
		if err != nil {
			return err
		}
		r.At = store.FormatTime(at)
		if err := downloads.Insert(ctx, tx, r); err != nil {
			return err
		}
	}
	return nil
}
