package osprobeassets

import (
	"bytes"
	"compress/gzip"
	"embed"
	"fmt"
	"io"
)

//go:embed bin/linux-amd64/db-osprobe.gz bin/linux-arm64/db-osprobe.gz
var embeddedAssets embed.FS

func Lookup(goos string, goarch string) ([]byte, error) {
	path := fmt.Sprintf("bin/%s-%s/db-osprobe.gz", goos, goarch)
	compressed, err := embeddedAssets.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("embedded os probe asset missing for %s/%s: %w", goos, goarch, err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("open embedded os probe asset failed: %w", err)
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read embedded os probe asset failed: %w", err)
	}
	return payload, nil
}
