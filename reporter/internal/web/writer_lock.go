package web

import "errors"

// ErrDataDirInUse reports that another lifecycle owns the configured data directory.
var ErrDataDirInUse = errors.New("report task data directory is already in use")

type writerLock interface {
	Close() error
}
