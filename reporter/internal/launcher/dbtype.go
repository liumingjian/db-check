package launcher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type manifestDBType struct {
	DBType string `json:"db_type"`
}

type resultDBType struct {
	Meta struct {
		DBType string `json:"db_type"`
	} `json:"meta"`
}

// DetectRunDirDBType inspects runDir and returns the normalized db_type from
// manifest.json or result.json.
func DetectRunDirDBType(runDir string) (string, error) {
	if strings.TrimSpace(runDir) == "" {
		return "", nil
	}
	manifestType, err := detectManifestDBType(filepath.Join(runDir, "manifest.json"))
	if err == nil && manifestType != "" {
		return manifestType, nil
	}
	resultType, resultErr := detectResultDBType(filepath.Join(runDir, "result.json"))
	if resultErr == nil && resultType != "" {
		return resultType, nil
	}
	if err != nil {
		return "", err
	}
	return "", resultErr
}

func detectManifestDBType(path string) (string, error) {
	var payload manifestDBType
	if err := readJSON(path, &payload); err != nil {
		return "", err
	}
	return normalizeDBType(payload.DBType), nil
}

func detectResultDBType(path string) (string, error) {
	var payload resultDBType
	if err := readJSON(path, &payload); err != nil {
		return "", err
	}
	return normalizeDBType(payload.Meta.DBType), nil
}

func readJSON(path string, target any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	if err := json.Unmarshal(content, target); err != nil {
		return fmt.Errorf("解析 %s 失败: %w", path, err)
	}
	return nil
}

func normalizeDBType(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "mysql" || value == "oracle" || value == "gaussdb" {
		return value
	}
	return ""
}

func rulePathForDBType(defaultRulePath string, dbType string) string {
	if dbType == "" || dbType == "mysql" {
		return defaultRulePath
	}
	return strings.Replace(defaultRulePath, string(filepath.Separator)+"mysql"+string(filepath.Separator), string(filepath.Separator)+dbType+string(filepath.Separator), 1)
}
