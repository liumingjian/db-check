//go:build windows

package web

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

type windowsWriterLock struct {
	file       *os.File
	overlapped windows.Overlapped
}

func acquireWriterLock(dataDir string) (writerLock, error) {
	file, err := os.OpenFile(filepath.Join(dataDir, ".dbcheck-web.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	lock := &windowsWriterLock{file: file}
	if err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&lock.overlapped,
	); err != nil {
		_ = file.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, fmt.Errorf("%w: %s", ErrDataDirInUse, dataDir)
		}
		return nil, fmt.Errorf("lock writer file: %w", err)
	}
	return lock, nil
}

func (l *windowsWriterLock) Close() error {
	unlockErr := windows.UnlockFileEx(windows.Handle(l.file.Fd()), 0, 1, 0, &l.overlapped)
	closeErr := l.file.Close()
	return errors.Join(unlockErr, closeErr)
}
