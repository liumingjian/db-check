package web

import (
	"archive/zip"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractZipFileRejectsZipSlip(t *testing.T) {
	root := t.TempDir()
	zipPath := filepath.Join(root, "bad.zip")
	writeZip(t, zipPath, map[string][]byte{
		"../evil.txt": []byte("nope"),
	})
	err := ExtractZipFile(zipPath, filepath.Join(root, "out"))
	if !errors.Is(err, ErrZipSlip) {
		t.Fatalf("expected ErrZipSlip, got %v", err)
	}
}

func TestExtractZipFileRejectsAbsolutePath(t *testing.T) {
	root := t.TempDir()
	zipPath := filepath.Join(root, "bad.zip")
	writeZip(t, zipPath, map[string][]byte{
		"/abs.txt": []byte("nope"),
	})
	err := ExtractZipFile(zipPath, filepath.Join(root, "out"))
	if !errors.Is(err, ErrZipSlip) {
		t.Fatalf("expected ErrZipSlip, got %v", err)
	}
}

func TestExtractZipFileAndDetectRunDirByManifest(t *testing.T) {
	root := t.TempDir()
	zipPath := filepath.Join(root, "ok.zip")
	writeZip(t, zipPath, map[string][]byte{
		"runs/demo/manifest.json": []byte(`{"db_type":"oracle"}`),
		"runs/demo/result.json":   []byte(`{"meta":{"db_type":"oracle"}}`),
	})
	outDir := filepath.Join(root, "out")
	if err := ExtractZipFile(zipPath, outDir); err != nil {
		t.Fatalf("ExtractZipFile failed: %v", err)
	}
	runDir, err := DetectRunDirByManifest(outDir)
	if err != nil {
		t.Fatalf("DetectRunDirByManifest failed: %v", err)
	}
	want := filepath.Join(outDir, "runs", "demo")
	if runDir != want {
		t.Fatalf("unexpected runDir: want %q got %q", want, runDir)
	}
}

func TestDetectRunDirByManifestRejectsMultiple(t *testing.T) {
	root := t.TempDir()
	mkdirFile(t, filepath.Join(root, "a", "manifest.json"), []byte(`{"db_type":"mysql"}`))
	mkdirFile(t, filepath.Join(root, "b", "manifest.json"), []byte(`{"db_type":"oracle"}`))

	_, err := DetectRunDirByManifest(root)
	if !errors.Is(err, ErrMultipleRunDirs) {
		t.Fatalf("expected ErrMultipleRunDirs, got %v", err)
	}
}

func writeZip(t *testing.T, zipPath string, files map[string][]byte) {
	t.Helper()

	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("create zip failed: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	for name, content := range files {
		zf, err := w.Create(name)
		if err != nil {
			_ = w.Close()
			t.Fatalf("create zip entry failed: %v", err)
		}
		if _, err := bytes.NewReader(content).WriteTo(zf); err != nil {
			_ = w.Close()
			t.Fatalf("write zip entry failed: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close zip failed: %v", err)
	}
}

func mkdirFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}
