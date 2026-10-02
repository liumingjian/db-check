package testserver

import (
	"archive/zip"
	"bytes"
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
	"dbcheck/reporter/internal/web"
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

// reset runs after the store was reseeded: it removes every task's files,
// releases the stub's waiting items so the worker drops the removed tasks,
// and gives each seeded done task that has not expired a result ZIP.
func (rt *reportTasks) reset(ctx context.Context, q store.Querier, now time.Time) error {
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
	return rt.writeSeededResults(ctx, q, now)
}

// writeSeededResults writes a placeholder result ZIP for every done task
// that has not expired. Expired ones keep none, as retention leaves them.
func (rt *reportTasks) writeSeededResults(ctx context.Context, q store.Querier, now time.Time) error {
	tasks, err := reports.List(ctx, q, reports.Filter{}, now)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task.Status != string(reports.StatusDone) || task.Expired {
			continue
		}
		if err := writePlaceholderZip(web.ResultZipPath(filepath.Join(rt.tasksDir, task.ID), task.ID), task.ID); err != nil {
			return err
		}
	}
	return nil
}

func writePlaceholderZip(path, taskID string) error {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("README.txt")
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "测试服务器种子任务 %s 的占位报告\n", taskID)
	if err := zw.Close(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
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
