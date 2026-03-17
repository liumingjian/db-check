package web

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrZipSlip         = errors.New("zip slip detected")
	ErrZipSymlink      = errors.New("zip symlink entry is not allowed")
	ErrRunDirNotFound  = errors.New("run dir not found")
	ErrMultipleRunDirs = errors.New("multiple run dirs found")
)

// ExtractZipFile extracts zipPath into destDir.
// It rejects zip-slip (path traversal), absolute paths, and symlink entries.
func ExtractZipFile(zipPath string, destDir string) error {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("open zip failed: %w", err)
	}
	defer reader.Close()
	return extractZip(&reader.Reader, destDir)
}

func extractZip(reader *zip.Reader, destDir string) error {
	baseDir := filepath.Clean(destDir)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create dest dir failed: %w", err)
	}

	for _, file := range reader.File {
		name := normalizeZipName(file.Name)
		if name == "" || name == "." {
			continue
		}
		if filepath.IsAbs(name) {
			return fmt.Errorf("%w: absolute path %q", ErrZipSlip, file.Name)
		}

		targetPath := filepath.Join(baseDir, filepath.FromSlash(name))
		if !isWithinDir(baseDir, targetPath) {
			return fmt.Errorf("%w: %q", ErrZipSlip, file.Name)
		}

		// Reject symlinks (including on macOS zips).
		if file.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %q", ErrZipSymlink, file.Name)
		}

		if file.FileInfo().IsDir() || strings.HasSuffix(file.Name, "/") {
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				return fmt.Errorf("mkdir failed: %w", err)
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return fmt.Errorf("mkdir failed: %w", err)
		}

		in, err := file.Open()
		if err != nil {
			return fmt.Errorf("open zip entry failed: %w", err)
		}
		out, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			_ = in.Close()
			return fmt.Errorf("create file failed: %w", err)
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			_ = in.Close()
			return fmt.Errorf("extract file failed: %w", err)
		}
		if err := out.Close(); err != nil {
			_ = in.Close()
			return fmt.Errorf("close file failed: %w", err)
		}
		if err := in.Close(); err != nil {
			return fmt.Errorf("close zip entry failed: %w", err)
		}
	}
	return nil
}

func normalizeZipName(name string) string {
	// Defensively normalize backslashes (some tools create zip entries with them).
	value := strings.ReplaceAll(name, "\\", "/")
	value = filepath.Clean(value)
	// filepath.Clean can return "."; keep it as empty to skip.
	if value == "." {
		return ""
	}
	return value
}

func isWithinDir(baseDir string, path string) bool {
	rel, err := filepath.Rel(baseDir, path)
	if err != nil {
		return false
	}
	rel = filepath.Clean(rel)
	if rel == "." {
		return true
	}
	sep := string(filepath.Separator)
	return rel != ".." && !strings.HasPrefix(rel, ".."+sep)
}

// DetectRunDirByManifest finds the unique directory that contains manifest.json
// under root. It ignores macOS artifacts like __MACOSX and ._* files.
func DetectRunDirByManifest(root string) (string, error) {
	base := filepath.Clean(root)
	var matches []string
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if name == "__MACOSX" || name == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if name == ".DS_Store" || strings.HasPrefix(name, "._") {
			return nil
		}
		if name != "manifest.json" {
			return nil
		}
		matches = append(matches, filepath.Dir(path))
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk run dir failed: %w", err)
	}
	switch len(matches) {
	case 0:
		return "", ErrRunDirNotFound
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("%w: %d manifests found", ErrMultipleRunDirs, len(matches))
	}
}
