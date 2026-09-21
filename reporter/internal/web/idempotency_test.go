package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTaskLifecycleReturnsRetainedTaskForIdenticalKeyedRetry(t *testing.T) {
	lifecycle, _ := newStartedIdempotencyLifecycle(t, Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})

	first, err := lifecycle.Submit(context.Background(), keyedSubmission("retry-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("first Submit failed: %v", err)
	}
	// Simulate a successful response that was lost before the browser received it.
	retry, err := lifecycle.Submit(context.Background(), keyedSubmission("retry-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("retry Submit failed: %v", err)
	}
	if retry.ID != first.ID {
		t.Fatalf("retry created task %q instead of retained task %q", retry.ID, first.ID)
	}
	if first.IdempotencyKey != "retry-key" || !validPayloadDigest(first.PayloadDigest) {
		t.Fatalf("key metadata was not persisted: %#v", first)
	}
	ids, err := lifecycle.store.ListIDs()
	if err != nil {
		t.Fatalf("ListIDs failed: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one published task, got %#v", ids)
	}
	assertEmptyStaging(t, lifecycle.store)
}

func TestTaskLifecycleSerializesConcurrentKeyedRetries(t *testing.T) {
	lifecycle, _ := newStartedIdempotencyLifecycle(t, Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})

	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	leaderResult := make(chan struct {
		task Task
		err  error
	}, 1)
	go func() {
		task, err := lifecycle.Submit(context.Background(), SubmissionRequest{
			Key: "concurrent-key",
			Materialize: func(context.Context) (ReportSubmission, error) {
				close(leaderStarted)
				<-releaseLeader
				return testSubmission("collector.zip", "same"), nil
			},
		})
		leaderResult <- struct {
			task Task
			err  error
		}{task, err}
	}()

	select {
	case <-leaderStarted:
	case <-time.After(time.Second):
		t.Fatal("leader did not start materializing")
	}

	followerResult := make(chan struct {
		task Task
		err  error
	}, 1)
	go func() {
		task, err := lifecycle.Submit(context.Background(), keyedSubmission("concurrent-key", testSubmission("collector.zip", "same")))
		followerResult <- struct {
			task Task
			err  error
		}{task, err}
	}()

	close(releaseLeader)
	var leader, follower struct {
		task Task
		err  error
	}
	select {
	case leader = <-leaderResult:
	case <-time.After(time.Second):
		t.Fatal("leader did not finish")
	}
	select {
	case follower = <-followerResult:
	case <-time.After(time.Second):
		t.Fatal("follower did not finish")
	}
	if leader.err != nil || follower.err != nil {
		t.Fatalf("unexpected submission errors: leader=%v follower=%v", leader.err, follower.err)
	}
	if leader.task.ID != follower.task.ID {
		t.Fatalf("concurrent retries returned different tasks: leader=%q follower=%q", leader.task.ID, follower.task.ID)
	}
}

func TestTaskLifecycleBoundsConcurrentRetainedKeyProbes(t *testing.T) {
	lifecycle, _ := newStartedIdempotencyLifecycle(t, Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
	if _, err := lifecycle.Submit(context.Background(), keyedSubmission("probe-key", testSubmission("collector.zip", "same"))); err != nil {
		t.Fatalf("initial Submit failed: %v", err)
	}

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		_, err := lifecycle.Submit(context.Background(), SubmissionRequest{
			Key: "probe-key",
			Materialize: func(context.Context) (ReportSubmission, error) {
				close(firstStarted)
				<-releaseFirst
				return testSubmission("collector.zip", "same"), nil
			},
		})
		firstResult <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first retained probe did not start")
	}

	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	secondResult := make(chan error, 1)
	go func() {
		_, err := lifecycle.Submit(context.Background(), SubmissionRequest{
			Key: "probe-key",
			Materialize: func(context.Context) (ReportSubmission, error) {
				close(secondStarted)
				<-releaseSecond
				return testSubmission("collector.zip", "same"), nil
			},
		})
		secondResult <- err
	}()
	waitForKeyLeases(t, lifecycle, "probe-key", 2)
	select {
	case <-secondStarted:
		t.Fatal("second retained probe bypassed the configured probe bound")
	default:
	}

	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second retained probe did not start after a slot was released")
	}
	close(releaseSecond)
	for _, result := range []<-chan error{firstResult, secondResult} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("retained probe failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("retained probe did not finish")
		}
	}
	assertEmptyStaging(t, lifecycle.store)
}

func TestTaskLifecycleRejectsConflictingKeyedRetryEvenAtCapacity(t *testing.T) {
	lifecycle, _ := newStartedIdempotencyLifecycle(t, Config{DataDir: t.TempDir(), MaxAcceptedTasks: 1})
	first, err := lifecycle.Submit(context.Background(), keyedSubmission("conflict-key", testSubmission("collector.zip", "first")))
	if err != nil {
		t.Fatalf("first Submit failed: %v", err)
	}

	if _, err := lifecycle.Submit(context.Background(), keyedSubmission("conflict-key", testSubmission("collector.zip", "changed"))); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected key conflict, got %v", err)
	}
	retained, err := lifecycle.Get(context.Background(), first.ID)
	if err != nil {
		t.Fatalf("load retained task: %v", err)
	}
	if retained.PayloadDigest != first.PayloadDigest || retained.Items[0].Name != "collector.zip" {
		t.Fatalf("conflicting retry changed retained task: %#v", retained)
	}

	retry, err := lifecycle.Submit(context.Background(), keyedSubmission("conflict-key", testSubmission("collector.zip", "first")))
	if err != nil {
		t.Fatalf("same payload retry at capacity failed: %v", err)
	}
	if retry.ID != first.ID {
		t.Fatalf("same payload retry returned %q, want %q", retry.ID, first.ID)
	}
	if _, err := lifecycle.Submit(context.Background(), keyedSubmission("new-key", testSubmission("collector.zip", "new"))); !errors.Is(err, ErrTaskCapacity) {
		t.Fatalf("new key at capacity returned %v, want capacity error", err)
	}
}

func TestSubmissionDigestBindsNamesPairingAndOrder(t *testing.T) {
	base := ReportSubmission{Items: []ReportItemSubmission{
		{
			Zip:  testSubmissionFile("first.zip", "first"),
			AWR:  submissionFilePointer(testSubmissionFile("first.html", "awr-one")),
			WDRs: []SubmissionFile{testSubmissionFile("first-wdr.html", "wdr-one"), testSubmissionFile("second-wdr.html", "wdr-two")},
		},
		{
			Zip: testSubmissionFile("second.zip", "second"),
		},
	}}

	baseDigest := stagedDigest(t, base)
	for name, changed := range map[string]ReportSubmission{
		"original name": {
			Items: []ReportItemSubmission{
				{Zip: testSubmissionFile("renamed.zip", "first"), AWR: submissionFilePointer(testSubmissionFile("first.html", "awr-one")), WDRs: []SubmissionFile{testSubmissionFile("first-wdr.html", "wdr-one"), testSubmissionFile("second-wdr.html", "wdr-two")}},
				{Zip: testSubmissionFile("second.zip", "second")},
			},
		},
		"optional pairing": {
			Items: []ReportItemSubmission{
				{Zip: testSubmissionFile("first.zip", "first")},
				{Zip: testSubmissionFile("second.zip", "second"), AWR: submissionFilePointer(testSubmissionFile("first.html", "awr-one")), WDRs: []SubmissionFile{testSubmissionFile("first-wdr.html", "wdr-one"), testSubmissionFile("second-wdr.html", "wdr-two")}},
			},
		},
		"wdr order": {
			Items: []ReportItemSubmission{
				{Zip: testSubmissionFile("first.zip", "first"), AWR: submissionFilePointer(testSubmissionFile("first.html", "awr-one")), WDRs: []SubmissionFile{testSubmissionFile("second-wdr.html", "wdr-two"), testSubmissionFile("first-wdr.html", "wdr-one")}},
				{Zip: testSubmissionFile("second.zip", "second")},
			},
		},
		"item order": {
			Items: []ReportItemSubmission{
				{Zip: testSubmissionFile("second.zip", "second")},
				{Zip: testSubmissionFile("first.zip", "first"), AWR: submissionFilePointer(testSubmissionFile("first.html", "awr-one")), WDRs: []SubmissionFile{testSubmissionFile("first-wdr.html", "wdr-one"), testSubmissionFile("second-wdr.html", "wdr-two")}},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if digest := stagedDigest(t, changed); digest == baseDigest {
				t.Fatalf("digest did not change for %s", name)
			}
		})
	}
}

func TestTaskLifecycleRebuildsKeyIndexAndLeavesLegacyTasksUnkeyed(t *testing.T) {
	dataDir := t.TempDir()
	first, closeFirst := newStartedIdempotencyLifecycle(t, Config{DataDir: dataDir, MaxAcceptedTasks: 3})
	accepted, err := first.Submit(context.Background(), keyedSubmission("restart-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if _, err := first.store.Create(Task{ID: "legacy-task", Status: TaskDone}); err != nil {
		t.Fatalf("create legacy task: %v", err)
	}
	closeFirst()

	restarted, _ := newStartedIdempotencyLifecycle(t, Config{DataDir: dataDir, MaxAcceptedTasks: 3})
	retry, err := restarted.Submit(context.Background(), keyedSubmission("restart-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("keyed retry after restart failed: %v", err)
	}
	if retry.ID != accepted.ID {
		t.Fatalf("restart retry returned %q, want %q", retry.ID, accepted.ID)
	}

	firstLegacy, err := restarted.Submit(context.Background(), materializedSubmission(testSubmission("legacy.zip", "same")))
	if err != nil {
		t.Fatalf("first unkeyed legacy submission failed: %v", err)
	}
	secondLegacy, err := restarted.Submit(context.Background(), materializedSubmission(testSubmission("legacy.zip", "same")))
	if err != nil {
		t.Fatalf("second unkeyed legacy submission failed: %v", err)
	}
	if firstLegacy.ID == secondLegacy.ID || firstLegacy.IdempotencyKey != "" || secondLegacy.IdempotencyKey != "" {
		t.Fatalf("unkeyed submissions unexpectedly deduplicated: %#v %#v", firstLegacy, secondLegacy)
	}
}

func TestTaskLifecycleStartRebuildsRetainedKeyIndex(t *testing.T) {
	dataDir := t.TempDir()
	first, closeFirst := newStartedIdempotencyLifecycle(t, Config{DataDir: dataDir, MaxAcceptedTasks: 1})
	accepted, err := first.Submit(context.Background(), keyedSubmission("start-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	accepted.Status = TaskDone
	if _, err := first.store.Update(accepted); err != nil {
		t.Fatalf("mark task terminal: %v", err)
	}
	closeFirst()

	restarted, err := newTaskLifecycle(Config{DataDir: dataDir, MaxAcceptedTasks: 1}, func() (*Pipeline, error) {
		return controlledLifecyclePipeline(), nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	if err := restarted.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := restarted.Close(ctx); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	retry, err := restarted.Submit(context.Background(), keyedSubmission("start-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("retained retry after Start failed: %v", err)
	}
	if retry.ID != accepted.ID {
		t.Fatalf("retained retry after Start returned %q, want %q", retry.ID, accepted.ID)
	}
}

func TestTaskLifecycleRetentionWaitsForKeyProbeThenExpiresKey(t *testing.T) {
	dataDir := t.TempDir()
	lifecycle, _ := newStartedIdempotencyLifecycle(t, Config{DataDir: dataDir, MaxAcceptedTasks: 2, RetentionTTL: time.Hour})
	accepted, err := lifecycle.Submit(context.Background(), keyedSubmission("expiry-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	waitForTaskStatus(t, lifecycle, accepted.ID, TaskProcessing)

	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	lifecycle.store.now = func() time.Time { return now.Add(-2 * time.Hour) }
	accepted.Status = TaskDone
	if _, err := lifecycle.store.Update(accepted); err != nil {
		t.Fatalf("mark task done: %v", err)
	}

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	retryResult := make(chan error, 1)
	go func() {
		_, err := lifecycle.Submit(context.Background(), SubmissionRequest{
			Key: "expiry-key",
			Materialize: func(context.Context) (ReportSubmission, error) {
				close(probeStarted)
				<-releaseProbe
				return testSubmission("collector.zip", "same"), nil
			},
		})
		retryResult <- err
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("retained retry did not start its probe")
	}

	deleted, err := lifecycle.cleanupExpiredRetainedTasks(func() time.Time { return now })
	if err != nil {
		t.Fatalf("cleanup during active probe failed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("cleanup deleted %d active retained task(s)", deleted)
	}
	if _, err := os.Stat(lifecycle.store.taskDir(accepted.ID)); err != nil {
		t.Fatalf("cleanup removed task protected by a key lease: %v", err)
	}

	close(releaseProbe)
	select {
	case err := <-retryResult:
		if err != nil {
			t.Fatalf("retained retry failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retained retry did not finish")
	}
	deleted, err = lifecycle.cleanupExpiredRetainedTasks(func() time.Time { return now })
	if err != nil {
		t.Fatalf("cleanup after probe failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("cleanup deleted %d tasks, want 1", deleted)
	}
	if _, err := lifecycle.store.Load(accepted.ID); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expired task still exists: %v", err)
	}

	replacement, err := lifecycle.Submit(context.Background(), keyedSubmission("expiry-key", testSubmission("collector.zip", "same")))
	if err != nil {
		t.Fatalf("expired key could not create a new task: %v", err)
	}
	if replacement.ID == accepted.ID {
		t.Fatalf("expired key returned deleted task %q", replacement.ID)
	}
}

func TestGenerateHTTPIdempotencyHeaderReturnsConflictForChangedPayload(t *testing.T) {
	cfg := Config{
		DataDir:          t.TempDir(),
		AllowedOrigins:   []string{"http://example.com"},
		APIToken:         defaultAPIToken,
		MaxUploadBytes:   0,
		MaxAcceptedTasks: 1,
	}
	blocked := make(chan struct{})
	handler := newStartedTestAPIHandler(t, cfg, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(string, string) error {
			<-blocked
			return nil
		}
		return pipeline, nil
	})
	t.Cleanup(func() { close(blocked) })

	first := performGenerate(t, handler.handler(), "api-key", "same")
	if first.Code != http.StatusOK {
		t.Fatalf("first generate status=%d body=%s", first.Code, first.Body.String())
	}
	var firstBody struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstBody); err != nil {
		t.Fatalf("decode first response: %v", err)
	}

	retry := performGenerate(t, handler.handler(), "api-key", "same")
	if retry.Code != http.StatusOK {
		t.Fatalf("same payload retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	var retryBody struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(retry.Body.Bytes(), &retryBody); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	if retryBody.TaskID != firstBody.TaskID {
		t.Fatalf("retry returned %q, want %q", retryBody.TaskID, firstBody.TaskID)
	}

	conflict := performGenerate(t, handler.handler(), "api-key", "changed")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("changed payload status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	var conflictBody struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(conflict.Body.Bytes(), &conflictBody); err != nil {
		t.Fatalf("decode conflict response: %v", err)
	}
	if conflictBody.Code != "idempotency_key_conflict" {
		t.Fatalf("unexpected conflict code %q", conflictBody.Code)
	}

	preflight := httptest.NewRequest(http.MethodOptions, "http://example.com/api/reports/generate", nil)
	preflight.Header.Set("Origin", "http://example.com")
	preflightRecorder := httptest.NewRecorder()
	handler.handler().ServeHTTP(preflightRecorder, preflight)
	if got := preflightRecorder.Header().Get("Access-Control-Allow-Headers"); got != "Authorization,Content-Type,Idempotency-Key" {
		t.Fatalf("CORS did not allow idempotency header: %q", got)
	}
}

func keyedSubmission(key string, submission ReportSubmission) SubmissionRequest {
	request := materializedSubmission(submission)
	request.Key = key
	return request
}

func newStartedIdempotencyLifecycle(t *testing.T, cfg Config) (*TaskLifecycle, func()) {
	t.Helper()
	releasePipeline := make(chan struct{})
	lifecycle, err := newTaskLifecycle(cfg, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(string, string) error {
			<-releasePipeline
			return nil
		}
		return pipeline, nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	var closeOnce sync.Once
	closeLifecycle := func() {
		closeOnce.Do(func() {
			close(releasePipeline)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := lifecycle.Close(ctx); err != nil {
				t.Errorf("Close failed: %v", err)
			}
		})
	}
	t.Cleanup(closeLifecycle)
	return lifecycle, closeLifecycle
}

func testSubmissionFile(name, content string) SubmissionFile {
	return SubmissionFile{
		Name: name,
		Open: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(content))), nil
		},
	}
}

func submissionFilePointer(file SubmissionFile) *SubmissionFile {
	return &file
}

func stagedDigest(t *testing.T, submission ReportSubmission) string {
	t.Helper()
	store, err := NewTaskStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTaskStore failed: %v", err)
	}
	id, err := newTaskID()
	if err != nil {
		t.Fatalf("newTaskID failed: %v", err)
	}
	stagingDir, err := store.CreateStaging(id)
	if err != nil {
		t.Fatalf("CreateStaging failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(stagingDir) })
	_, digest, err := stageSubmission(context.Background(), stagingDir, submission)
	if err != nil {
		t.Fatalf("stageSubmission failed: %v", err)
	}
	return digest
}

func waitForKeyLeases(t *testing.T, lifecycle *TaskLifecycle, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		lifecycle.mu.Lock()
		leases := 0
		if retained := lifecycle.keyIndex[key]; retained != nil {
			leases = retained.leases
		}
		lifecycle.mu.Unlock()
		if leases == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("idempotency key %q did not reach %d active leases", key, want)
}

func performGenerate(t *testing.T, handler http.Handler, key, contents string) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("zips", "collector.zip")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	if _, err := part.Write([]byte(contents)); err != nil {
		t.Fatalf("write multipart file: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "http://example.com/api/reports/generate", &body)
	request.Header.Set("Authorization", "Bearer "+defaultAPIToken)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("Idempotency-Key", key)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}
