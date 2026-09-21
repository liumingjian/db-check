package web

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTaskHubSnapshotAndSubscribeDoesNotMissTransitionDuringSetup(t *testing.T) {
	hub := newTaskHub(0)
	enteredLoad := make(chan struct{})
	releaseLoad := make(chan struct{})

	type watchResult struct {
		task    Task
		version int64
		updates <-chan []byte
		cancel  func()
		err     error
	}
	resultCh := make(chan watchResult, 1)
	go func() {
		task, version, _, updates, _, cancel, err := hub.snapshotAndSubscribe("t1", func() (Task, error) {
			close(enteredLoad)
			<-releaseLoad
			return Task{ID: "t1", Status: TaskProcessing}, nil
		})
		resultCh <- watchResult{task: task, version: version, updates: updates, cancel: cancel, err: err}
	}()

	<-enteredLoad
	emitted := make(chan struct{})
	go func() {
		hub.emitDone("t1", "/api/reports/download/t1")
		close(emitted)
	}()
	close(releaseLoad)

	watch := <-resultCh
	if watch.err != nil {
		t.Fatalf("snapshotAndSubscribe failed: %v", watch.err)
	}
	defer watch.cancel()
	if watch.task.Status != TaskProcessing || watch.version != 0 {
		t.Fatalf("unexpected initial snapshot: %#v version=%d", watch.task, watch.version)
	}
	<-emitted

	select {
	case update := <-watch.updates:
		var message wsDoneMessage
		if err := json.Unmarshal(update, &message); err != nil {
			t.Fatalf("decode update failed: %v", err)
		}
		if message.Type != "done" || message.DownloadURL != "/api/reports/download/t1" {
			t.Fatalf("unexpected terminal update: %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal transition was missed during watch setup")
	}
}

func TestTaskHubOverflowDisconnectsSubscriber(t *testing.T) {
	hub := newTaskHub(0)
	_, _, _, updates, overflow, cancel, err := hub.snapshotAndSubscribe("t1", func() (Task, error) {
		return Task{ID: "t1", Status: TaskProcessing}, nil
	})
	if err != nil {
		t.Fatalf("snapshotAndSubscribe failed: %v", err)
	}
	defer cancel()

	for i := 0; i <= wsSubscriberBuffer; i++ {
		hub.emitProgress("t1", i, wsSubscriberBuffer+1, "input.zip")
	}

	select {
	case <-overflow:
	case <-time.After(time.Second):
		t.Fatal("overflow did not disconnect the subscriber")
	}

	hub.mu.Lock()
	remaining := len(hub.state("t1").subs)
	hub.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("overflowed subscriber remained registered: %d", remaining)
	}

	for range updates {
	}
}
