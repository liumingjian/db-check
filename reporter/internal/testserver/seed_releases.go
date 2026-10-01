package testserver

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbcheck/reporter/internal/releases"
	"dbcheck/reporter/internal/store"
)

func fixtureReleases(f fixture) ([]releases.Release, error) {
	var seed []releases.Release
	if err := f.section("releases", &seed); err != nil {
		return nil, err
	}
	for _, r := range seed {
		for _, p := range r.Packages {
			if want := releases.FileName(r.Version, p.Platform); p.FileName != want {
				return nil, fmt.Errorf("seed fixture: release %s package %s is %q, want %q", r.Version, p.Platform, p.FileName, want)
			}
		}
	}
	return seed, nil
}

func seedReleases(ctx context.Context, tx store.Querier, f fixture, now time.Time) error {
	seed, err := fixtureReleases(f)
	if err != nil {
		return err
	}
	for _, r := range seed {
		publishedAt, err := seedTime(r.PublishedAt, now)
		if err != nil {
			return err
		}
		r.PublishedAt = store.FormatTime(publishedAt)
		if err := releases.Insert(ctx, tx, r); err != nil {
			return err
		}
	}
	return nil
}

// restorePackageFiles replaces the releases area of the data directory with
// the fixture's package files, so it holds exactly what the seed lists.
// Each file is a real zip of the fixture's size and SHA256. They are
// generated once into seedPackagesDir and hard-linked on every reset.
func (s *Server) restorePackageFiles() error {
	seed, err := fixtureReleases(s.fixture)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(releases.Dir(s.dataDir)); err != nil {
		return err
	}
	cache := filepath.Join(s.dataDir, seedPackagesDir)
	for _, r := range seed {
		if err := os.MkdirAll(filepath.Join(releases.Dir(s.dataDir), r.Version), 0o755); err != nil {
			return err
		}
		for _, p := range r.Packages {
			cached := filepath.Join(cache, p.FileName)
			if _, err := os.Stat(cached); err != nil {
				if err := writeSeedPackage(cached, p.FileName, p.Size); err != nil {
					return err
				}
			}
			if err := os.Link(cached, releases.PackagePath(s.dataDir, r.Version, p.Platform)); err != nil {
				return err
			}
		}
	}
	return nil
}

// seedPackagesDir holds the generated seed package files, beside the
// releases area so they can be hard-linked into it.
const seedPackagesDir = "seed-packages"

// writeSeedPackage writes a zip of exactly size bytes whose one stored
// entry repeats a line naming the package, so its content, and so its
// SHA256, depends only on name and size. The fixture's checksums are these
// files' (TestSeedPackageFilesMatchTheFixture).
func writeSeedPackage(path, name string, size int64) error {
	overhead, err := seedZip(io.Discard, name, 0)
	if err != nil {
		return err
	}
	if size < overhead {
		return fmt.Errorf("seed package %s: size %d is below the zip overhead %d", name, size, overhead)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(f)
	written, err := seedZip(buf, name, size-overhead)
	if err == nil {
		err = buf.Flush()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil && written != size {
		err = fmt.Errorf("seed package %s: wrote %d bytes, want %d", name, written, size)
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// seedZip writes the seed zip with dataLen bytes of entry data and returns
// its total length.
func seedZip(w io.Writer, name string, dataLen int64) (int64, error) {
	counter := &countingWriter{w: w}
	zw := zip.NewWriter(counter)
	entry, err := zw.CreateHeader(&zip.FileHeader{
		Name:     "db-collector/SEED.txt",
		Method:   zip.Store,
		Modified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		return 0, err
	}
	line := []byte(fmt.Sprintf("contract test seed package %s\n", strings.TrimSuffix(name, ".zip")))
	filler := bytes.Repeat(line, 64<<10/len(line)+1)
	for left := dataLen; left > 0; {
		n := min(left, int64(len(filler)))
		if _, err := entry.Write(filler[:n]); err != nil {
			return 0, err
		}
		left -= n
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	return counter.n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
