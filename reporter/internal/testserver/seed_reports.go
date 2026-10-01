package testserver

import (
	"context"
	"time"

	"dbcheck/reporter/internal/reports"
	"dbcheck/reporter/internal/store"
)

type seedReportTask struct {
	ID          string           `json:"id"`
	SubmitterID string           `json:"submitterId"`
	Status      reports.Status   `json:"status"`
	CreatedAt   string           `json:"createdAt"`
	Items       []seedReportItem `json:"items"`
}

type seedReportItem struct {
	FileName         string  `json:"fileName"`
	DBType           string  `json:"dbType"`
	CollectorVersion *string `json:"collectorVersion"`
	Outcome          struct {
		Status reports.Status `json:"status"`
		Reason string         `json:"reason"`
	} `json:"outcome"`
}

// seedReportTasks loads the fixture's finished report tasks. A seeded task
// has no uploads, so one still processing (task-seed-003) is left out: the
// worker would claim it after a restart and fail it. The mock keeps it.
func seedReportTasks(ctx context.Context, tx store.Querier, f fixture, now time.Time) error {
	var seed []seedReportTask
	if err := f.section("reportTasks", &seed); err != nil {
		return err
	}
	for _, s := range seed {
		if !s.Status.Finished() {
			continue
		}
		createdAt, err := seedTime(s.CreatedAt, now)
		if err != nil {
			return err
		}
		task := reports.Task{ID: s.ID, SubmitterID: s.SubmitterID, Status: s.Status, CreatedAt: createdAt}
		for i, it := range s.Items {
			task.Items = append(task.Items, reports.Item{
				Position: i + 1, FileName: it.FileName, DBType: it.DBType, CollectorVersion: it.CollectorVersion,
				Status: it.Outcome.Status, Reason: it.Outcome.Reason,
			})
		}
		if err := reports.Insert(ctx, tx, task); err != nil {
			return err
		}
	}
	return nil
}
