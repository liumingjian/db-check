package testserver

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"dbcheck/reporter/internal/web"
)

// stubItemDuration is how long the stub takes per report item on the
// pinned clock, as GENERATION_MS_PER_ITEM in web/src/lib/api/reports/mock.ts.
const stubItemDuration = 5 * time.Second

// StubFailReason is the reason a stub item fails with.
const StubFailReason = "模拟生成失败"

// stubPipeline generates report items without the collector toolchain.
//
// Outcome by file-name convention: a ZIP whose name contains "fail" fails
// with StubFailReason; any other item gets a small report document.
//
// Timing follows the mock: item n of a task finishes once the pinned clock
// reaches the task's creation time plus n × stubItemDuration, or at once
// when someone watches the task (its WebSocket opens). Until then the task
// stays processing, so "still generating" is observable.
type stubPipeline struct {
	clock *pinnedClock

	mu sync.Mutex
	// changed is closed and replaced whenever a waiting item may be ready.
	changed chan struct{}
	hurried map[string]bool
}

func newStubPipeline(clock *pinnedClock) *stubPipeline {
	return &stubPipeline{clock: clock, changed: make(chan struct{}), hurried: map[string]bool{}}
}

func (p *stubPipeline) RunItem(_ context.Context, job web.ItemJob, onLog func(web.LogEvent)) web.ItemResult {
	position, _ := strconv.Atoi(job.Input.ID)
	due := job.TaskCreatedAt.Add(time.Duration(position) * stubItemDuration)
	for {
		p.mu.Lock()
		ready := p.hurried[job.TaskID] || !p.clock.Now().Before(due)
		changed := p.changed
		p.mu.Unlock()
		if _, err := os.Stat(job.TaskDir); err != nil {
			// A reset removed the task.
			return web.ItemResult{ID: job.Input.ID, Status: web.ItemFailed, Error: "测试服务器已重置"}
		}
		if ready {
			break
		}
		<-changed
	}
	onLog(web.LogEvent{Stream: web.LogStdout, Line: "渲染 report.docx..."})
	if strings.Contains(job.Input.Name, "fail") {
		return web.ItemResult{ID: job.Input.ID, Status: web.ItemFailed, Error: StubFailReason}
	}
	path := web.ItemReportPath(job.TaskDir, job.Input.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return web.ItemResult{ID: job.Input.ID, Status: web.ItemFailed, Error: err.Error()}
	}
	if err := os.WriteFile(path, []byte("stub report for "+job.Input.Name), 0o644); err != nil {
		return web.ItemResult{ID: job.Input.ID, Status: web.ItemFailed, Error: err.Error()}
	}
	return web.ItemResult{ID: job.Input.ID, Status: web.ItemDone, ReportDocx: path}
}

// wake lets waiting items look at the clock again.
func (p *stubPipeline) wake() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.changed)
	p.changed = make(chan struct{})
}

// hurry finishes a watched task's items without waiting for the clock.
func (p *stubPipeline) hurry(taskID string) {
	p.mu.Lock()
	p.hurried[taskID] = true
	p.mu.Unlock()
	p.wake()
}

// reset forgets watched tasks and releases items of removed tasks.
func (p *stubPipeline) reset() {
	p.mu.Lock()
	p.hurried = map[string]bool{}
	p.mu.Unlock()
	p.wake()
}
