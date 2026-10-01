package testserver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"dbcheck/reporter/internal/store"
)

// fixture is the shared seed fixture (tests/fixtures/console-seed.json),
// one raw section per top-level key, so each domain's seeder decodes only
// its own section.
type fixture map[string]json.RawMessage

func readFixture(path string) (fixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read seed fixture: %w", err)
	}
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse seed fixture %s: %w", path, err)
	}
	return f, nil
}

// section decodes one top-level key of the fixture.
func (f fixture) section(key string, into any) error {
	raw, ok := f[key]
	if !ok {
		return fmt.Errorf("seed fixture has no %q", key)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("seed fixture %q: %w", key, err)
	}
	return nil
}

// seeder loads one domain's section of the fixture inside the reset
// transaction, resolving time offsets against now.
type seeder func(ctx context.Context, tx store.Querier, f fixture, now time.Time) error

// seeders run in order on every reset. Users come first because every other
// domain refers to them. Each domain adds its seeder in its own
// seed_<domain>.go and appends it here.
var seeders = []seeder{
	seedUsers,
	seedReleases,
	seedDownloads,
}

var offsetPattern = regexp.MustCompile(`^now(?:-(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?)?$`)

// seedTime resolves a fixture time offset (`now`, `now-1d2h3m`, each part
// optional, in that order) against now. It mirrors seedTime in
// web/src/lib/api/seed-fixture.ts.
func seedTime(offset string, now time.Time) (time.Time, error) {
	m := offsetPattern.FindStringSubmatch(offset)
	if m == nil || offset == "now-" { // RE2 has no lookahead to refuse a bare "now-"
		return time.Time{}, fmt.Errorf("seed fixture: malformed time offset %q", offset)
	}
	units := []time.Duration{24 * time.Hour, time.Hour, time.Minute}
	var ago time.Duration
	for i, part := range m[1:] {
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return time.Time{}, err
		}
		ago += time.Duration(n) * units[i]
	}
	return now.Add(-ago), nil
}
