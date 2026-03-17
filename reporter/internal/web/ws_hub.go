package web

import (
	"encoding/json"
	"sync"
	"time"
)

type taskHub struct {
	mu      sync.Mutex
	maxLogs int
	tasks   map[string]*taskHubState
}

type taskHubState struct {
	nextSeq int64
	logs    [][]byte
	subs    map[*wsSubscriber]struct{}
}

type wsSubscriber struct {
	ch chan []byte
}

func newTaskHub(maxLogs int) *taskHub {
	return &taskHub{
		maxLogs: maxLogs,
		tasks:   make(map[string]*taskHubState),
	}
}

func (h *taskHub) snapshot(taskID string) (lastSeq int64, logs [][]byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.state(taskID)
	out := make([][]byte, 0, len(state.logs))
	for _, b := range state.logs {
		cp := make([]byte, len(b))
		copy(cp, b)
		out = append(out, cp)
	}
	return state.nextSeq, out
}

func (h *taskHub) subscribe(taskID string) (ch <-chan []byte, cancel func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.state(taskID)
	sub := &wsSubscriber{ch: make(chan []byte, 256)}
	state.subs[sub] = struct{}{}
	return sub.ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := state.subs[sub]; ok {
			delete(state.subs, sub)
			close(sub.ch)
		}
	}
}

func (h *taskHub) emitLog(taskID string, level string, message string) {
	payload := wsLogMessage{
		Type:      "log",
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Level:     level,
		Message:   message,
	}
	h.emit(taskID, true, &payload)
}

func (h *taskHub) emitProgress(taskID string, completed int, total int, currentFile string) {
	payload := wsProgressMessage{
		Type:        "progress",
		Completed:   completed,
		Total:       total,
		CurrentFile: currentFile,
	}
	h.emit(taskID, false, &payload)
}

func (h *taskHub) emitDone(taskID string, downloadURL string) {
	payload := wsDoneMessage{
		Type:        "done",
		DownloadURL: downloadURL,
	}
	h.emit(taskID, false, &payload)
}

func (h *taskHub) emitError(taskID string, message string) {
	payload := wsErrorMessage{
		Type:    "error",
		Message: message,
	}
	h.emit(taskID, false, &payload)
}

func (h *taskHub) emit(taskID string, storeLog bool, msg any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.state(taskID)
	state.nextSeq++
	seq := state.nextSeq
	// Inject seq into message by marshaling through a map for simplicity.
	b, _ := json.Marshal(withSeq(msg, seq))
	if storeLog {
		state.logs = append(state.logs, b)
		if h.maxLogs > 0 && len(state.logs) > h.maxLogs {
			state.logs = state.logs[len(state.logs)-h.maxLogs:]
		}
	}
	for sub := range state.subs {
		select {
		case sub.ch <- b:
		default:
			// Slow client: drop message to avoid blocking the worker.
		}
	}
}

func (h *taskHub) state(taskID string) *taskHubState {
	state, ok := h.tasks[taskID]
	if ok {
		return state
	}
	state = &taskHubState{
		nextSeq: 0,
		logs:    nil,
		subs:    make(map[*wsSubscriber]struct{}),
	}
	h.tasks[taskID] = state
	return state
}

func withSeq(msg any, seq int64) map[string]any {
	switch m := msg.(type) {
	case *wsLogMessage:
		return map[string]any{
			"type":      m.Type,
			"seq":       seq,
			"timestamp": m.Timestamp,
			"level":     m.Level,
			"message":   m.Message,
		}
	case *wsProgressMessage:
		return map[string]any{
			"type":         m.Type,
			"seq":          seq,
			"completed":    m.Completed,
			"total":        m.Total,
			"current_file": m.CurrentFile,
		}
	case *wsDoneMessage:
		return map[string]any{
			"type":         m.Type,
			"seq":          seq,
			"download_url": m.DownloadURL,
		}
	case *wsErrorMessage:
		return map[string]any{
			"type":    m.Type,
			"seq":     seq,
			"message": m.Message,
		}
	default:
		return map[string]any{
			"seq": seq,
		}
	}
}
