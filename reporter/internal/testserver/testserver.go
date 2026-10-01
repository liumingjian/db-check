// Package testserver is the contract test server: the production API routes
// and store, plus test-only endpoints that restore the shared seed fixture
// and pin the clock. The console's "real" contract entry drives it
// (web/src/lib/api/testing.ts). It is never built into production.
//
//	POST /test/reset {"now": "<ISO time>"}  wipe every table and task file, pin the clock, load the seed
//	POST /test/clock {"now": "<ISO time>"}  move the pinned clock; answers once report tasks it made due finished
//
// Reports run on a stub pipeline (stub_pipeline.go) instead of the collector
// toolchain.
package testserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/web"
)

// Server serves the production routes over its own store.
type Server struct {
	http.Handler
	dataDir string
	db      *store.DB
	fixture fixture
	clock   *pinnedClock
	reports *reportTasks
	// resetMu keeps a reset from interleaving with another one.
	resetMu sync.Mutex
}

// New opens a store in dataDir and wires the production routes to it. The
// store stays empty until the first reset.
func New(dataDir, fixturePath string) (*Server, error) {
	f, err := readFixture(fixturePath)
	if err != nil {
		return nil, err
	}
	db, err := store.Open(dataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{dataDir: dataDir, db: db, fixture: f, clock: &pinnedClock{}}
	s.reports = &reportTasks{stub: newStubPipeline(s.clock), tasksDir: filepath.Join(dataDir, "tasks")}
	cfg := web.Config{
		DataDir:        dataDir,
		AllowedOrigins: []string{"http://localhost:3000"},
		LogReplayLines: 1000,
		PythonBin:      "python3",
	}
	api, err := web.NewHandler(cfg, web.Platform{DB: db, Now: s.clock.Now, Log: log.Default(), Pipeline: s.reports.stub})
	if err != nil {
		db.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /test/reset", s.handleReset)
	mux.HandleFunc("POST /test/clock", s.handleClock)
	s.reports.routes(mux, api)
	mux.Handle("/", api)
	s.Handler = mux
	return s, nil
}

func (s *Server) Close() error { return s.db.Close() }

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	now, ok := readNow(w, r)
	if !ok {
		return
	}
	s.resetMu.Lock()
	defer s.resetMu.Unlock()
	s.clock.Set(now)
	if err := s.reset(r.Context(), now); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.reports.reset(r.Context(), s.db, now); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleClock moves the clock and answers once the report tasks it made due
// have finished.
func (s *Server) handleClock(w http.ResponseWriter, r *http.Request) {
	now, ok := readNow(w, r)
	if !ok {
		return
	}
	s.clock.Set(now)
	if err := s.reports.settle(r.Context(), s.db, now); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func readNow(w http.ResponseWriter, r *http.Request) (time.Time, bool) {
	var body struct {
		Now time.Time `json:"now"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Now.IsZero() {
		http.Error(w, `body must be {"now": "<ISO time>"}`, http.StatusBadRequest)
		return time.Time{}, false
	}
	return body.Now, true
}

// reset empties every table but the migration log and runs the seeders, in
// one transaction. Foreign keys are checked at commit, so tables can be
// emptied in any order. Then it restores the files the seed refers to.
func (s *Server) reset(ctx context.Context, now time.Time) error {
	err := s.db.Tx(ctx, func(tx store.Querier) error {
		if _, err := tx.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON"); err != nil {
			return err
		}
		tables, err := tableNames(ctx, tx)
		if err != nil {
			return err
		}
		for _, table := range tables {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %q", table)); err != nil {
				return err
			}
		}
		for _, seed := range seeders {
			if err := seed(ctx, tx, s.fixture, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return s.restorePackageFiles()
}

func tableNames(ctx context.Context, q store.Querier) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT name FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// pinnedClock is the platform clock: it stands still at the time a reset or
// a clock move set.
type pinnedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *pinnedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *pinnedClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}
