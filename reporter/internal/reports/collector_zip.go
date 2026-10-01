package reports

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// CollectorZip is what the server reads from an uploaded collector ZIP. Its
// reading is authoritative; the browser's (web/src/lib/report-input/inspect.ts)
// only gives early feedback.
type CollectorZip struct {
	DBType string
	// CollectorVersion is the result JSON's meta.collector_version, trimmed;
	// nil when anything along the way is missing.
	CollectorVersion *string
}

// ReadCollectorZip follows the browser's rule: find the one manifest.json
// (at any depth, skipping macOS and git artifacts), follow
// manifest.artifacts.result to the result JSON, and read its meta block.
// db_type keeps the report launcher's normalisation (trim and lowercase,
// falling back to the result meta), as the real pipeline reads it. The error
// is a Chinese reason the console can show.
func ReadCollectorZip(zipPath string) (CollectorZip, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return CollectorZip{}, errors.New("不是有效的 ZIP 文件")
	}
	defer r.Close()

	entries := make(map[string]*zip.File, len(r.File))
	var manifests []string
	for _, f := range r.File {
		name := cleanZipPath(f.Name)
		entries[name] = f
		if !isSkipped(name) && path.Base(name) == "manifest.json" {
			manifests = append(manifests, name)
		}
	}
	switch len(manifests) {
	case 0:
		return CollectorZip{}, errors.New("ZIP 中没有 manifest.json")
	case 1:
	default:
		return CollectorZip{}, fmt.Errorf("ZIP 中有 %d 个 manifest.json", len(manifests))
	}

	manifest := readObject(entries[manifests[0]])
	if manifest == nil {
		return CollectorZip{}, errors.New("manifest.json 无法解析")
	}
	runDir := dirOf(manifests[0])
	var res map[string]any
	if name, ok := asObject(manifest["artifacts"])["result"].(string); ok && name != "" {
		res = readObject(entries[cleanZipPath(runDir+name)])
	}
	meta := asObject(res["meta"])

	dbType := normalizeDBType(manifest["db_type"])
	if dbType == "" {
		// The launcher falls back to result.json's meta when the manifest
		// has no usable db_type.
		fallback := meta
		if fallback == nil {
			fallback = asObject(readObject(entries[runDir+"result.json"])["meta"])
		}
		dbType = normalizeDBType(fallback["db_type"])
	}
	if dbType == "" {
		raw, present := manifest["db_type"]
		if !present {
			return CollectorZip{}, errors.New("manifest.json 缺少 db_type")
		}
		return CollectorZip{}, fmt.Errorf("不支持的数据库类型：%v", raw)
	}

	out := CollectorZip{DBType: dbType}
	if v, ok := meta["collector_version"].(string); ok && strings.TrimSpace(v) != "" {
		v = strings.TrimSpace(v)
		out.CollectorVersion = &v
	}
	return out, nil
}

// cleanZipPath mirrors the browser's cleanPath: backslashes become slashes
// and one leading "./" is dropped.
func cleanZipPath(name string) string {
	return strings.TrimPrefix(strings.ReplaceAll(name, `\`, "/"), "./")
}

// isSkipped matches the entries DetectRunDirByManifest skips.
func isSkipped(name string) bool {
	parts := strings.Split(name, "/")
	base := parts[len(parts)-1]
	for _, p := range parts {
		if p == "__MACOSX" || p == ".git" {
			return true
		}
	}
	return base == ".DS_Store" || strings.HasPrefix(base, "._")
}

func dirOf(name string) string {
	if cut := strings.LastIndex(name, "/"); cut >= 0 {
		return name[:cut+1]
	}
	return ""
}

// readObject decodes an entry as a JSON object; nil when it is absent, not
// JSON, or not an object.
func readObject(f *zip.File) map[string]any {
	if f == nil {
		return nil
	}
	rc, err := f.Open()
	if err != nil {
		return nil
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return nil
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return nil
	}
	return asObject(v)
}

func asObject(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func normalizeDBType(v any) string {
	s, _ := v.(string)
	switch s = strings.ToLower(strings.TrimSpace(s)); s {
	case "mysql", "oracle", "gaussdb":
		return s
	}
	return ""
}
