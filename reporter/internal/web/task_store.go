package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type TaskStore struct {
	dataDir   string
	tasksDir  string
	now       func() time.Time
	writeJSON func(path string, value any) error
	removeAll func(path string) error
}

func NewTaskStore(dataDir string) (*TaskStore, error) {
	if dataDir == "" {
		return nil, errors.New("dataDir is required")
	}
	tasksDir := filepath.Join(dataDir, "tasks")
	if err := os.MkdirAll(tasksDir, 0o755); err != nil {
		return nil, fmt.Errorf("create tasks dir failed: %w", err)
	}
	return &TaskStore{
		dataDir:   dataDir,
		tasksDir:  tasksDir,
		now:       time.Now,
		writeJSON: writeJSONFileAtomic,
		removeAll: os.RemoveAll,
	}, nil
}

func (s *TaskStore) Create(task Task) (Task, error) {
	if err := validateTaskID(task.ID); err != nil {
		return Task{}, err
	}
	task.CreatedAt = s.now()
	task.UpdatedAt = task.CreatedAt

	taskPath := s.taskPath(task.ID)
	if _, err := os.Stat(taskPath); err == nil {
		return Task{}, ErrTaskAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return Task{}, fmt.Errorf("stat task failed: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(taskPath), 0o755); err != nil {
		return Task{}, fmt.Errorf("create task dir failed: %w", err)
	}
	if err := s.writeJSON(taskPath, task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func (s *TaskStore) CreateStaging(id string) (string, error) {
	if err := validateTaskID(id); err != nil {
		return "", err
	}
	if err := os.MkdirAll(s.stagingDir(), 0o755); err != nil {
		return "", fmt.Errorf("create staging dir failed: %w", err)
	}
	path := filepath.Join(s.stagingDir(), id)
	if err := os.Mkdir(path, 0o755); err != nil {
		return "", fmt.Errorf("create task staging dir failed: %w", err)
	}
	return path, nil
}

func (s *TaskStore) WriteStagedTask(stagingDir string, task Task) error {
	if err := validateTaskID(task.ID); err != nil {
		return err
	}
	if filepath.Dir(stagingDir) != s.stagingDir() {
		return errors.New("staging directory is outside task staging root")
	}
	return s.writeJSON(filepath.Join(stagingDir, "task.json"), task)
}

// Publish atomically makes a complete staged task visible to task discovery.
func (s *TaskStore) Publish(stagingDir, id string) error {
	if err := validateTaskID(id); err != nil {
		return err
	}
	if filepath.Dir(stagingDir) != s.stagingDir() {
		return errors.New("staging directory is outside task staging root")
	}
	if _, err := os.Stat(s.taskDir(id)); err == nil {
		return ErrTaskAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat published task failed: %w", err)
	}
	if err := os.Rename(stagingDir, s.taskDir(id)); err != nil {
		return fmt.Errorf("publish task failed: %w", err)
	}
	return nil
}

func (s *TaskStore) Load(id string) (Task, error) {
	if err := validateTaskID(id); err != nil {
		return Task{}, err
	}
	taskPath := s.taskPath(id)
	content, err := os.ReadFile(taskPath)
	if errors.Is(err, os.ErrNotExist) {
		return Task{}, ErrTaskNotFound
	}
	if err != nil {
		return Task{}, fmt.Errorf("read task failed: %w", err)
	}
	var task Task
	if err := json.Unmarshal(content, &task); err != nil {
		return Task{}, fmt.Errorf("decode task failed: %w", err)
	}
	return task, nil
}

func (s *TaskStore) Update(task Task) (Task, error) {
	if err := validateTaskID(task.ID); err != nil {
		return Task{}, err
	}
	taskPath := s.taskPath(task.ID)
	if _, err := os.Stat(taskPath); errors.Is(err, os.ErrNotExist) {
		return Task{}, ErrTaskNotFound
	} else if err != nil {
		return Task{}, fmt.Errorf("stat task failed: %w", err)
	}
	// Preserve CreatedAt from disk if caller doesn't provide it.
	if task.CreatedAt.IsZero() {
		existing, err := s.Load(task.ID)
		if err != nil {
			return Task{}, err
		}
		task.CreatedAt = existing.CreatedAt
	}
	task.UpdatedAt = s.now()
	if err := s.writeJSON(taskPath, task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func (s *TaskStore) taskPath(id string) string {
	return filepath.Join(s.tasksDir, id, "task.json")
}

func (s *TaskStore) taskDir(id string) string {
	return filepath.Join(s.tasksDir, id)
}

func (s *TaskStore) stagingDir() string {
	return filepath.Join(s.tasksDir, ".staging")
}

// RemoveStaging removes incomplete submissions that were never published.
func (s *TaskStore) RemoveStaging() error {
	if err := s.removeAll(s.stagingDir()); err != nil {
		return fmt.Errorf("remove task staging failed: %w", err)
	}
	return nil
}

func (s *TaskStore) ListIDs() ([]string, error) {
	entries, err := os.ReadDir(s.tasksDir)
	if err != nil {
		return nil, fmt.Errorf("read tasks dir failed: %w", err)
	}
	var ids []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		if err := validateTaskID(id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *TaskStore) ListTasks() ([]Task, error) {
	ids, err := s.ListIDs()
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, 0, len(ids))
	for _, id := range ids {
		task, err := s.Load(id)
		if err != nil {
			return nil, err
		}
		if task.ID != id {
			return nil, fmt.Errorf("task metadata id mismatch: directory=%q metadata=%q", id, task.ID)
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (s *TaskStore) FindNextQueued() (Task, bool, error) {
	tasks, err := s.ListTasks()
	if err != nil {
		return Task{}, false, err
	}
	queued := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		if task.Status == TaskQueued {
			queued = append(queued, task)
		}
	}
	if len(queued) == 0 {
		return Task{}, false, nil
	}
	sort.Slice(queued, func(i, j int) bool {
		if queued[i].AcceptedOrder != queued[j].AcceptedOrder {
			return queued[i].AcceptedOrder < queued[j].AcceptedOrder
		}
		if !queued[i].AcceptedAt.Equal(queued[j].AcceptedAt) {
			return queued[i].AcceptedAt.Before(queued[j].AcceptedAt)
		}
		return queued[i].ID < queued[j].ID
	})
	return queued[0], true, nil
}

func writeJSONFileAtomic(path string, value any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create dir failed: %w", err)
	}
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode json failed: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".task.json.*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file failed: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp file failed: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file failed: %w", err)
	}
	if _, err := tmp.WriteString("\n"); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file failed: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file failed: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename temp file failed: %w", err)
	}
	return nil
}
