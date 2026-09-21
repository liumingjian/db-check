package web

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func (l *TaskLifecycle) recoverTasks() error {
	if err := l.store.RemoveStaging(); err != nil {
		return err
	}
	tasks, err := l.store.ListTasks()
	if err != nil {
		return fmt.Errorf("load published tasks: %w", err)
	}

	acceptedUnfinished := 0
	var maxAcceptedOrder uint64
	for _, task := range tasks {
		if err := validateRecoveredTask(task); err != nil {
			return fmt.Errorf("invalid task metadata for %q: %w", task.ID, err)
		}
		if task.AcceptedOrder > maxAcceptedOrder {
			maxAcceptedOrder = task.AcceptedOrder
		}

		switch task.Status {
		case TaskQueued, TaskProcessing:
			if err := validateCompletedItemArtifacts(l.store.taskDir(task.ID), task); err != nil {
				if err := l.failRecoveredTask(&task, err); err != nil {
					return err
				}
				continue
			}
			if _, err := loadTaskInputs(l.store.taskDir(task.ID), task); err != nil {
				if err := l.failRecoveredTask(&task, err); err != nil {
					return err
				}
				continue
			}
			if task.Status == TaskProcessing {
				task.Status = TaskQueued
				task.CurrentFile = ""
				updated, err := l.store.Update(task)
				if err != nil {
					return fmt.Errorf("persist queued recovery state for task %q: %w", task.ID, err)
				}
				task = updated
			}
			acceptedUnfinished++
			l.hub.emitLog(task.ID, "info", "服务重启，任务已恢复到队列")
		case TaskDone:
			if err := validateCompletedTaskArtifacts(l.store.taskDir(task.ID), task); err != nil {
				if err := l.failRecoveredTask(&task, err); err != nil {
					return err
				}
			}
		case TaskFailed:
			// A persisted task failure is terminal and must not be retried during recovery.
		}
	}
	if maxAcceptedOrder == ^uint64(0) {
		return fmt.Errorf("accepted order is exhausted")
	}

	l.mu.Lock()
	l.acceptedUnfinished = acceptedUnfinished
	l.nextAcceptedOrder = maxAcceptedOrder + 1
	l.mu.Unlock()
	return nil
}

// resumeTasks remains a narrow test seam for recovering persisted task records.
func (l *TaskLifecycle) resumeTasks() error {
	return l.recoverTasks()
}

func validateRecoveredTask(task Task) error {
	if err := validateTaskID(task.ID); err != nil {
		return err
	}
	switch task.Status {
	case TaskQueued, TaskProcessing, TaskDone, TaskFailed:
		return nil
	default:
		return fmt.Errorf("unknown task status %q", task.Status)
	}
}

func (l *TaskLifecycle) failRecoveredTask(task *Task, reason error) error {
	task.Status = TaskFailed
	task.Error = fmt.Sprintf("recovery failed: %v", reason)
	task.CurrentFile = ""
	updated, err := l.store.Update(*task)
	if err != nil {
		return fmt.Errorf("persist recovery failure for task %q: %w", task.ID, err)
	}
	*task = updated
	l.hub.emitError(task.ID, task.Error)
	return nil
}

func validateCompletedTaskArtifacts(taskDir string, task Task) error {
	if err := validateCompletedItemArtifacts(taskDir, task); err != nil {
		return err
	}
	if task.Status != TaskDone {
		return nil
	}
	return validateZipArtifact(taskDir, filepath.Join(taskDir, fmt.Sprintf("reports-%s.zip", task.ID)))
}

func validateCompletedItemArtifacts(taskDir string, task Task) error {
	for _, item := range task.Items {
		if item.Status != string(ItemDone) {
			continue
		}
		if err := validateZipArtifact(taskDir, item.ReportDocx); err != nil {
			return fmt.Errorf("completed report for item %q is unavailable: %w", item.ID, err)
		}
	}
	return nil
}

func validateZipArtifact(taskDir, artifactPath string) error {
	if strings.TrimSpace(artifactPath) == "" {
		return fmt.Errorf("missing file")
	}
	root, err := filepath.EvalSymlinks(taskDir)
	if err != nil {
		return fmt.Errorf("resolve task directory: %w", err)
	}
	artifact, err := filepath.EvalSymlinks(artifactPath)
	if err != nil {
		return fmt.Errorf("resolve file: %w", err)
	}
	rel, err := filepath.Rel(root, artifact)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("file is outside the task directory")
	}
	info, err := os.Stat(artifact)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("file is not a non-empty regular file")
	}
	reader, err := zip.OpenReader(artifact)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer reader.Close()
	if len(reader.File) == 0 {
		return fmt.Errorf("zip has no entries")
	}
	return nil
}

func loadTaskInputs(taskDir string, task Task) ([]ItemInput, error) {
	uploadsDir := filepath.Join(taskDir, "uploads")
	entries, err := os.ReadDir(uploadsDir)
	if err != nil {
		return nil, fmt.Errorf("read uploads dir failed: %w", err)
	}

	type upload struct {
		path string
		name string
	}

	zips := make(map[string]upload)
	awrs := make(map[string]string)
	wdrs := make(map[string][]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, "zip-"):
			id, orig, ok := parseUploadName(name, "zip-")
			if !ok {
				continue
			}
			if err := validateTaskID(id); err != nil {
				continue
			}
			zips[id] = upload{path: filepath.Join(uploadsDir, name), name: orig}
		case strings.HasPrefix(name, "awr-"):
			id, _, ok := parseUploadName(name, "awr-")
			if !ok {
				continue
			}
			if err := validateTaskID(id); err != nil {
				continue
			}
			awrs[id] = filepath.Join(uploadsDir, name)
		case strings.HasPrefix(name, "wdr-"):
			id, _, ok := parseUploadName(name, "wdr-")
			if !ok {
				continue
			}
			if err := validateTaskID(id); err != nil {
				continue
			}
			wdrs[id] = append(wdrs[id], filepath.Join(uploadsDir, name))
		}
	}

	if len(task.Items) > 0 {
		out := make([]ItemInput, 0, len(task.Items))
		for _, item := range task.Items {
			zip, ok := zips[item.ID]
			if !ok {
				return nil, fmt.Errorf("missing zip for item %s", item.ID)
			}
			name := strings.TrimSpace(item.Name)
			if name == "" {
				name = zip.name
			}
			out = append(out, ItemInput{
				ID:       item.ID,
				Name:     name,
				ZipPath:  zip.path,
				AWRPath:  awrs[item.ID],
				WDRPaths: wdrs[item.ID],
			})
		}
		return out, nil
	}

	ids := make([]string, 0, len(zips))
	for id := range zips {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ai, err1 := strconv.Atoi(ids[i])
		aj, err2 := strconv.Atoi(ids[j])
		if err1 == nil && err2 == nil {
			return ai < aj
		}
		return ids[i] < ids[j]
	})
	out := make([]ItemInput, 0, len(ids))
	for _, id := range ids {
		zip := zips[id]
		out = append(out, ItemInput{
			ID:       id,
			Name:     zip.name,
			ZipPath:  zip.path,
			AWRPath:  awrs[id],
			WDRPaths: wdrs[id],
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no uploads found")
	}
	return out, nil
}

func parseUploadName(filename string, prefix string) (id string, origName string, ok bool) {
	rest := strings.TrimPrefix(filename, prefix)
	dash := strings.Index(rest, "-")
	if dash <= 0 || dash >= len(rest)-1 {
		return "", "", false
	}
	id = rest[:dash]
	origName = rest[dash+1:]
	return id, origName, true
}
