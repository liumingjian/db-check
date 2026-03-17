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
			continue
		}
		folder := sanitizeZipFolder(nameByID[result.ID])
		if folder == "" {
			folder = result.ID
		}
		entryPath := strings.TrimLeft(filepath.ToSlash(filepath.Join(folder, "report.docx")), "/")
		if entryPath == "" {
			continue
		}

		if err := addFileToZip(zw, entryPath, result.ReportDocx); err != nil {
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
		return errors.New("no successful reports to download")
	}
	if err := os.Rename(tmpPath, zipPath); err != nil {
		return fmt.Errorf("finalize zip failed: %w", err)
	}
	return nil
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

func addFileToZip(zw *zip.Writer, entryPath string, srcPath string) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("stat report failed: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("report path is a dir: %s", srcPath)
	}
	src, err := os.Open(srcPath)
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
