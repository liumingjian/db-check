package web

import (
	"fmt"
	"os"
)

func ensureDir(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("创建 data-dir 失败: %w", err)
	}
	return nil
}
