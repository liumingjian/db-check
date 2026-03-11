package output

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type FileWriter struct{}

func (FileWriter) PrepareRunDir(outputDir string, runID string) (string, error) {
	runDir := filepath.Join(outputDir, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", fmt.Errorf("create run dir failed: %w", err)
	}
	return runDir, nil
}

func (FileWriter) WriteJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}
	tmpPath := path + ".tmp"
	bytes, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json failed: %w", err)
	}
	if err := os.WriteFile(tmpPath, bytes, 0o644); err != nil {
		return fmt.Errorf("write temp file failed: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp file failed: %w", err)
	}
	return nil
}

func (FileWriter) WriteText(path string, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write text failed: %w", err)
	}
	return nil
}
