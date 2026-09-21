//go:build darwin || linux

package web

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type unixWriterLock struct {
	file *os.File
}

func acquireWriterLock(dataDir string) (writerLock, error) {
	file, err := os.OpenFile(filepath.Join(dataDir, ".dbcheck-web.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrDataDirInUse, dataDir)
		}
		return nil, fmt.Errorf("lock writer file: %w", err)
	}
	return &unixWriterLock{file: file}, nil
}

func (l *unixWriterLock) Close() error {
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	return errors.Join(unlockErr, closeErr)
}
