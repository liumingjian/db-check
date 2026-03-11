package launcher

import (
	"os"
	"path/filepath"
)

func stderr() *os.File {
	return os.Stderr
}

func pathBase(path string) string {
	return filepath.Base(path)
}
