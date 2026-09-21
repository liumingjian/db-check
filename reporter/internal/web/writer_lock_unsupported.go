//go:build !darwin && !linux && !windows

package web

import (
	"fmt"
	"runtime"
)

func acquireWriterLock(string) (writerLock, error) {
	return nil, fmt.Errorf("report task writer lock is unsupported on %s", runtime.GOOS)
}
