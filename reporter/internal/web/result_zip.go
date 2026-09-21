package web

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func buildResultZip(zipPath string, results []ItemResult, inputs []ItemInput) error {
	return buildResultZipWithDependencies(zipPath, results, inputs, resultZipDependencies{
		openFile: func(path string) (io.ReadCloser, error) {
			return os.Open(path)
		},
		validate: validateResultZip,
	})
}

type resultZipDependencies struct {
	openFile func(string) (io.ReadCloser, error)
	validate func(string) error
}

func buildResultZipWithDependencies(zipPath string, results []ItemResult, inputs []ItemInput, dependencies resultZipDependencies) error {
	nameByID := make(map[string]string, len(inputs))
	for _, in := range inputs {
		nameByID[in.ID] = in.Name
	}

	tmpPath := zipPath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(zipPath), 0o755); err != nil {
		return fmt.Errorf("create zip dir failed: %w", err)
	}
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create zip failed: %w", err)
	}
	defer func() { _ = os.Remove(tmpPath) }()

	zw := zip.NewWriter(tmp)
	success := 0
	for _, result := range results {
		if result.Status != ItemDone {
			continue
		}
		if strings.TrimSpace(result.ReportDocx) == "" {
			_ = zw.Close()
			_ = tmp.Close()
			return fmt.Errorf("completed report artifact is missing for item %q", result.ID)
		}
		folder := sanitizeZipFolder(nameByID[result.ID])
		if folder == "" {
			folder = result.ID
		}
		entryPath := strings.TrimLeft(filepath.ToSlash(filepath.Join(folder, "report.docx")), "/")
		if entryPath == "" {
			continue
		}

		if err := addFileToZip(zw, entryPath, result.ReportDocx, dependencies.openFile); err != nil {
			_ = zw.Close()
			_ = tmp.Close()
			return err
		}
		success++
	}

	if err := zw.Close(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("close zip failed: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close zip failed: %w", err)
	}
	if success == 0 {
		return noSuccessfulReportsError(results, nameByID)
	}
	if err := dependencies.validate(tmpPath); err != nil {
		return fmt.Errorf("validate result zip failed: %w", err)
	}
	if err := os.Rename(tmpPath, zipPath); err != nil {
		return fmt.Errorf("finalize zip failed: %w", err)
	}
	return nil
}

func noSuccessfulReportsError(results []ItemResult, nameByID map[string]string) error {
	failures := make([]string, 0, len(results))
	for _, result := range results {
		if result.Status != ItemFailed {
			continue
		}
		name := strings.TrimSpace(nameByID[result.ID])
		if name == "" {
			name = result.ID
		}
		detail := strings.TrimSpace(result.Error)
		if detail == "" {
			failures = append(failures, name)
			continue
		}
		failures = append(failures, fmt.Sprintf("%s: %s", name, detail))
	}
	if len(failures) == 0 {
		return errors.New("no successful reports to download")
	}
	return fmt.Errorf("no successful reports to download; failed items: %s", strings.Join(failures, "; "))
}

func sanitizeZipFolder(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	base = strings.ReplaceAll(base, "/", "_")
	base = strings.ReplaceAll(base, "\\", "_")
	if base == "." || base == string(filepath.Separator) {
		return ""
	}
	return base
}

func addFileToZip(zw *zip.Writer, entryPath string, srcPath string, openFile func(string) (io.ReadCloser, error)) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("stat report failed: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("report path is a dir: %s", srcPath)
	}
	src, err := openFile(srcPath)
	if err != nil {
		return fmt.Errorf("open report failed: %w", err)
	}
	defer src.Close()

	hdr, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("zip header failed: %w", err)
	}
	hdr.Name = entryPath
	hdr.Method = zip.Deflate

	dst, err := zw.CreateHeader(hdr)
	if err != nil {
		return fmt.Errorf("zip create entry failed: %w", err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("zip write entry failed: %w", err)
	}
	return nil
}

func validateResultZip(path string) error {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer reader.Close()
	if len(reader.File) == 0 {
		return errors.New("zip has no entries")
	}
	for _, file := range reader.File {
		entry, err := file.Open()
		if err != nil {
			return fmt.Errorf("open zip entry %q: %w", file.Name, err)
		}
		_, copyErr := io.Copy(io.Discard, entry)
		closeErr := entry.Close()
		if copyErr != nil {
			return fmt.Errorf("read zip entry %q: %w", file.Name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close zip entry %q: %w", file.Name, closeErr)
		}
	}
	return nil
}
