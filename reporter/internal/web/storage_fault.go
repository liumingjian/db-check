package web

import "errors"

var ErrStorageUnavailable = errors.New("report task storage is unavailable")

const (
	storageFaultCode    = "storage_unavailable"
	storageFaultMessage = "report task storage is unavailable; repair storage and restart the service"
)

// StorageFault describes a lifecycle-wide storage pause in task snapshots.
type StorageFault struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (l *TaskLifecycle) storageFaultSnapshot() *StorageFault {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.storageFault == nil {
		return nil
	}
	copy := *l.storageFault
	return &copy
}

func (l *TaskLifecycle) hasStorageFault() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.storageFault != nil
}

func (l *TaskLifecycle) requireStorageHealthy() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.storageFault != nil {
		return ErrStorageUnavailable
	}
	return nil
}

func (l *TaskLifecycle) pauseForStorage(error) error {
	l.mu.Lock()
	newFault := l.pauseForStorageLocked()
	l.mu.Unlock()
	if newFault {
		l.signalDispatcher()
		l.hub.closeSubscribers()
	}
	return ErrStorageUnavailable
}

func (l *TaskLifecycle) pauseForStorageLocked() bool {
	if l.storageFault == nil {
		l.storageFault = &StorageFault{
			Code:    storageFaultCode,
			Message: storageFaultMessage,
		}
		return true
	}
	return false
}

func (l *TaskLifecycle) persistTask(task Task) (Task, error) {
	if err := l.requireStorageHealthy(); err != nil {
		return Task{}, err
	}
	updated, err := l.store.Update(task)
	if err != nil {
		return Task{}, l.pauseForStorage(err)
	}
	if l.hasStorageFault() {
		return Task{}, ErrStorageUnavailable
	}
	return updated, nil
}

func isTaskReadFailure(err error) bool {
	return err != nil && !errors.Is(err, ErrTaskNotFound) && !errors.Is(err, ErrInvalidTaskID)
}
