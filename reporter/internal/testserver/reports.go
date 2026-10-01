package testserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"

	"dbcheck/reporter/internal/reports"
	"dbcheck/reporter/internal/store"
)

// reportTasks is the test server's side of report generation: the stub
// pipeline, and the tasks submitted since the last reset, which a clock
// move waits for.
type reportTasks struct {
	stub     *stubPipeline
	tasksDir string

	mu        sync.Mutex
	submitted []string
}

// routes wraps the API's report routes: watching a task hurries it, and a
// submission is remembered so a clock move can wait for it.
func (rt *reportTasks) routes(mux *http.ServeMux, api http.Handler) {
	mux.HandleFunc("GET /api/reports/ws/{id}", func(w http.ResponseWriter, r *http.Request) {
		rt.stub.hurry(r.PathValue("id"))
		api.ServeHTTP(w, r)
	})
	mux.HandleFunc("POST /api/reports/generate", func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, r)
		var body struct {
			TaskID string `json:"task_id"`
		}
		if rec.Code == http.StatusOK && json.Unmarshal(rec.Body.Bytes(), &body) == nil {
			rt.mu.Lock()
			rt.submitted = append(rt.submitted, body.TaskID)
			rt.mu.Unlock()
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	})
}

// reset runs after the store was emptied: it removes every task's files and
// releases the stub's waiting items, so the worker drops the removed tasks.
func (rt *reportTasks) reset() error {
	rt.mu.Lock()
	rt.submitted = nil
	rt.mu.Unlock()
	entries, err := os.ReadDir(rt.tasksDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(rt.tasksDir, e.Name())); err != nil {
			return err
		}
	}
	rt.stub.reset()
	return nil
}

// settle runs after the clock moved: it wakes the stub and waits until
// every submitted task whose items are all due has finished, so the next
// request sees it done.
func (rt *reportTasks) settle(ctx context.Context, q store.Querier, now time.Time) error {
	rt.stub.wake()
	rt.mu.Lock()
	ids := append([]string(nil), rt.submitted...)
	rt.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range ids {
		for {
			task, err := reports.Get(ctx, q, id)
			if err != nil {
				return err
			}
			allDue := !now.Before(task.CreatedAt.Add(time.Duration(len(task.Items)) * stubItemDuration))
			if task.Status.Finished() || !allDue {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("report task %s did not finish after the clock moved", id)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return nil
}
