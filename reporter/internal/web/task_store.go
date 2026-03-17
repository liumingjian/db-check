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
	tasksDir string
	now      func() time.Time
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
		tasksDir: tasksDir,
		now:      time.Now,
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
	if err := writeJSONFileAtomic(taskPath, task); err != nil {
		return Task{}, err
	}
	return task, nil
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
	if err := writeJSONFileAtomic(taskPath, task); err != nil {
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
