package web

import (
	"errors"
	"regexp"
	"time"
)

var (
	ErrInvalidTaskID     = errors.New("invalid task id")
	ErrTaskNotFound      = errors.New("task not found")
	ErrTaskAlreadyExists = errors.New("task already exists")
)

type TaskStatus string

const (
	TaskQueued     TaskStatus = "queued"
	TaskProcessing TaskStatus = "processing"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

type Task struct {
	ID        string     `json:"id"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`

	Total       int        `json:"total"`
	Completed   int        `json:"completed"`
	CurrentFile string     `json:"current_file,omitempty"`
	Error       string     `json:"error,omitempty"`
	Items       []TaskItem `json:"items,omitempty"`
}

type TaskItem struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	ReportDocx string `json:"report_docx,omitempty"`
}

var taskIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

func validateTaskID(id string) error {
	if !taskIDRe.MatchString(id) {
		return ErrInvalidTaskID
	}
	return nil
}
