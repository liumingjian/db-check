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
	uploadsDir := filepath.Join(taskDir, "uploads")
	entries, err := os.ReadDir(uploadsDir)
	if err != nil {
		return nil, fmt.Errorf("read uploads dir failed: %w", err)
	}

	type upload struct {
		path string
		name string
	}

	zips := make(map[string]upload)
	awrs := make(map[string]string)
	wdrs := make(map[string][]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		kind, rest, ok := strings.Cut(name, "-")
		if !ok {
			continue
		}
		id, orig, ok := parseUploadName(rest)
		if !ok || validateTaskID(id) != nil {
			continue
		}
		path := filepath.Join(uploadsDir, name)
		switch kind {
		case "zip":
			zips[id] = upload{path: path, name: orig}
		case "awr":
			awrs[id] = path
		case "wdr":
			wdrs[id] = append(wdrs[id], path)
		}
	}
	for id := range wdrs {
		sort.Strings(wdrs[id])
	}

	if len(items) > 0 {
		out := make([]ItemInput, 0, len(items))
		for _, item := range items {
			zip, ok := zips[item.ID]
			if !ok {
				return nil, fmt.Errorf("missing zip for item %s", item.ID)
			}
			name := strings.TrimSpace(item.Name)
			if name == "" {
				name = zip.name
			}
			out = append(out, ItemInput{ID: item.ID, Name: name, ZipPath: zip.path, AWRPath: awrs[item.ID], WDRPaths: wdrs[item.ID]})
		}
		return out, nil
	}

	ids := make([]string, 0, len(zips))
	for id := range zips {
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
		zip := zips[id]
		out = append(out, ItemInput{ID: id, Name: zip.name, ZipPath: zip.path, AWRPath: awrs[id], WDRPaths: wdrs[id]})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no uploads found")
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
