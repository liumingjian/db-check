package web

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// loadTaskInputs rebuilds a task's pipeline inputs from its uploads/
// directory, where saveUpload named each file <kind>-<item id>-<name>.
// With items it keeps their order; without (a legacy task that never
// started) it orders the ZIPs by item id.
func loadTaskInputs(taskDir string, items []TaskItem) ([]ItemInput, error) {
	uploads, err := scanUploads(filepath.Join(taskDir, "uploads"))
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		return uploads.inputsFor(items)
	}
	return uploads.inputsByID()
}

// savedZip is an uploaded ZIP as saved, with its original name.
type savedZip struct {
	path string
	name string
}

// savedUploads are a task's saved uploads by item id.
type savedUploads struct {
	zips map[string]savedZip
	awrs map[string]string
	wdrs map[string][]string // sorted
}

func scanUploads(uploadsDir string) (savedUploads, error) {
	entries, err := os.ReadDir(uploadsDir)
	if err != nil {
		return savedUploads{}, fmt.Errorf("read uploads dir failed: %w", err)
	}
	s := savedUploads{zips: map[string]savedZip{}, awrs: map[string]string{}, wdrs: map[string][]string{}}
	for _, entry := range entries {
		kind, rest, ok := strings.Cut(entry.Name(), "-")
		if entry.IsDir() || !ok {
			continue
		}
		id, orig, ok := parseUploadName(rest)
		if !ok || validateTaskID(id) != nil {
			continue
		}
		path := filepath.Join(uploadsDir, entry.Name())
		switch kind {
		case "zip":
			s.zips[id] = savedZip{path: path, name: orig}
		case "awr":
			s.awrs[id] = path
		case "wdr":
			s.wdrs[id] = append(s.wdrs[id], path)
		}
	}
	for id := range s.wdrs {
		sort.Strings(s.wdrs[id])
	}
	return s, nil
}

func (s savedUploads) input(id, name string) ItemInput {
	return ItemInput{ID: id, Name: name, ZipPath: s.zips[id].path, AWRPath: s.awrs[id], WDRPaths: s.wdrs[id]}
}

// inputsFor returns the items' inputs in their order; an item keeps its
// name, or takes its ZIP's.
func (s savedUploads) inputsFor(items []TaskItem) ([]ItemInput, error) {
	out := make([]ItemInput, 0, len(items))
	for _, item := range items {
		zip, ok := s.zips[item.ID]
		if !ok {
			return nil, fmt.Errorf("missing zip for item %s", item.ID)
		}
		name := strings.TrimSpace(item.Name)
		if name == "" {
			name = zip.name
		}
		out = append(out, s.input(item.ID, name))
	}
	return out, nil
}

// inputsByID returns one input per ZIP, ordered by numeric item id.
func (s savedUploads) inputsByID() ([]ItemInput, error) {
	if len(s.zips) == 0 {
		return nil, fmt.Errorf("no uploads found")
	}
	ids := make([]string, 0, len(s.zips))
	for id := range s.zips {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ai, err1 := strconv.Atoi(ids[i])
		aj, err2 := strconv.Atoi(ids[j])
		if err1 == nil && err2 == nil {
			return ai < aj
		}
		return ids[i] < ids[j]
	})
	out := make([]ItemInput, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.input(id, s.zips[id].name))
	}
	return out, nil
}

// parseUploadName splits "<item id>-<name>". For a WDR, saved under
// "<item id>-<k>", the id is still the item's.
func parseUploadName(rest string) (id string, origName string, ok bool) {
	dash := strings.Index(rest, "-")
	if dash <= 0 || dash >= len(rest)-1 {
		return "", "", false
	}
	return rest[:dash], rest[dash+1:], true
}
