package web

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func (h *apiHandler) resumeTasks() {
	ids, err := h.store.ListIDs()
	if err != nil {
		return
	}
	for _, id := range ids {
		task, err := h.store.Load(id)
		if err != nil {
			continue
		}
		if task.Status != TaskQueued && task.Status != TaskProcessing {
			continue
		}
		items, err := loadTaskInputs(h.store.taskDir(task.ID), task)
		if err != nil {
			task.Status = TaskFailed
			task.Error = fmt.Sprintf("resume failed: %v", err)
			task.CurrentFile = ""
			_, _ = h.store.Update(task)
			h.hub.emitError(task.ID, task.Error)
			continue
		}
		h.enqueue(queuedTask{TaskID: task.ID, Items: items})
		h.hub.emitLog(task.ID, "info", "服务重启，任务已恢复到队列")
	}
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
				ID:      item.ID,
				Name:    name,
				ZipPath: zip.path,
				AWRPath: awrs[item.ID],
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
			ID:      id,
			Name:    zip.name,
			ZipPath: zip.path,
			AWRPath: awrs[id],
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
