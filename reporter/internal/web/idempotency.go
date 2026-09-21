package web

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

const maxIdempotencyKeyLength = 256

type retainedSubmission struct {
	taskID string
	digest string
	leases int
}

type keyedSubmissionFlight struct {
	done chan struct{}
}

func normalizeIdempotencyKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", nil
	}
	if len(key) > maxIdempotencyKeyLength {
		return "", invalidSubmission(fmt.Errorf("idempotency key exceeds %d bytes", maxIdempotencyKeyLength))
	}
	return key, nil
}

func validPayloadDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func (l *TaskLifecycle) submitKeyed(ctx context.Context, request SubmissionRequest, key string) (Task, error) {
	for {
		retained, flight, leader, err := l.beginKeyedSubmission(key)
		if err != nil {
			return Task{}, err
		}
		if retained != nil {
			return l.resolveRetainedSubmission(ctx, key, retained, request)
		}
		if leader {
			task, err := l.submitReserved(ctx, request, key)
			l.finishKeyedSubmission(key, flight)
			return task, err
		}

		digest, err := l.probeSubmission(ctx, request)
		if err != nil {
			return Task{}, err
		}
		select {
		case <-flight.done:
		case <-ctx.Done():
			return Task{}, ctx.Err()
		}

		// A failed leader leaves no durable key record, so retry admission from
		// the beginning. A published leader is resolved from the durable index.
		if retained := l.acquireRetainedSubmission(key); retained != nil {
			return l.loadRetainedSubmission(key, retained, digest)
		}
	}
}

func (l *TaskLifecycle) beginKeyedSubmission(key string) (*retainedSubmission, *keyedSubmissionFlight, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, nil, false, ErrLifecycleClosed
	}
	if retained := l.keyIndex[key]; retained != nil {
		retained.leases++
		return retained, nil, false, nil
	}
	if flight := l.keyFlights[key]; flight != nil {
		return nil, flight, false, nil
	}
	if l.acceptedUnfinished+l.reservations >= l.cfg.MaxAcceptedTasks {
		return nil, nil, false, ErrTaskCapacity
	}
	l.reservations++
	flight := &keyedSubmissionFlight{done: make(chan struct{})}
	l.keyFlights[key] = flight
	return nil, flight, true, nil
}

func (l *TaskLifecycle) finishKeyedSubmission(key string, flight *keyedSubmissionFlight) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.keyFlights[key] != flight {
		return
	}
	delete(l.keyFlights, key)
	close(flight.done)
}

func (l *TaskLifecycle) acquireRetainedSubmission(key string) *retainedSubmission {
	l.mu.Lock()
	defer l.mu.Unlock()
	retained := l.keyIndex[key]
	if retained != nil {
		retained.leases++
	}
	return retained
}

func (l *TaskLifecycle) resolveRetainedSubmission(ctx context.Context, key string, retained *retainedSubmission, request SubmissionRequest) (Task, error) {
	digest, err := l.probeSubmission(ctx, request)
	if err != nil {
		l.releaseRetainedSubmission(key, retained)
		return Task{}, err
	}
	return l.loadRetainedSubmission(key, retained, digest)
}

func (l *TaskLifecycle) loadRetainedSubmission(key string, retained *retainedSubmission, digest string) (Task, error) {
	defer l.releaseRetainedSubmission(key, retained)
	if digest != retained.digest {
		return Task{}, ErrIdempotencyConflict
	}
	task, err := l.store.Load(retained.taskID)
	if err != nil {
		return Task{}, err
	}
	return task, nil
}

func (l *TaskLifecycle) releaseRetainedSubmission(key string, retained *retainedSubmission) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.keyIndex[key] == retained && retained.leases > 0 {
		retained.leases--
	}
}

func (l *TaskLifecycle) probeSubmission(ctx context.Context, request SubmissionRequest) (digest string, err error) {
	select {
	case l.probeSlots <- struct{}{}:
		defer func() { <-l.probeSlots }()
	case <-ctx.Done():
		return "", ctx.Err()
	}

	probeID, err := newTaskID()
	if err != nil {
		return "", err
	}
	stagingDir, err := l.store.CreateStaging(probeID)
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(stagingDir) }()

	submission, err := request.Materialize(ctx)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	_, digest, err = stageSubmission(ctx, stagingDir, submission)
	return digest, err
}

func (l *TaskLifecycle) rebuildKeyIndex() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rebuildKeyIndexLocked()
}

func (l *TaskLifecycle) rebuildKeyIndexLocked() error {
	tasks, err := l.store.ListTasks()
	if err != nil {
		return err
	}
	index := make(map[string]*retainedSubmission)
	for _, task := range tasks {
		key, keyErr := normalizeIdempotencyKey(task.IdempotencyKey)
		digest := strings.TrimSpace(task.PayloadDigest)
		if key == "" && digest == "" {
			continue
		}
		if keyErr != nil || key == "" || !validPayloadDigest(digest) {
			return fmt.Errorf("task %s has invalid idempotency metadata", task.ID)
		}
		if prior := index[key]; prior != nil {
			return fmt.Errorf("tasks %s and %s share idempotency key %q", prior.taskID, task.ID, key)
		}
		index[key] = &retainedSubmission{taskID: task.ID, digest: digest}
	}
	l.keyIndex = index
	return nil
}

func (l *TaskLifecycle) cleanupExpiredRetainedTasks(now func() time.Time) (int, error) {
	if l.cfg.RetentionTTL <= 0 {
		return 0, nil
	}
	if now == nil {
		now = time.Now
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	tasks, err := l.store.ListTasks()
	if err != nil {
		return 0, err
	}

	cutoff := now().Add(-l.cfg.RetentionTTL)
	deleted := 0
	for _, task := range tasks {
		if task.Status != TaskDone && task.Status != TaskFailed {
			continue
		}
		if task.UpdatedAt.After(cutoff) {
			continue
		}

		key, _ := normalizeIdempotencyKey(task.IdempotencyKey)
		retained := l.keyIndex[key]
		if retained != nil && retained.taskID == task.ID && retained.leases > 0 {
			continue
		}
		if err := os.RemoveAll(l.store.taskDir(task.ID)); err != nil {
			return deleted, fmt.Errorf("remove task dir failed: %w", err)
		}
		if retained != nil && retained.taskID == task.ID {
			delete(l.keyIndex, key)
		}
		deleted++
	}
	return deleted, nil
}
