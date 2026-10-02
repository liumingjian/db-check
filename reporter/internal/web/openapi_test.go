package web

import (
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const openAPIPath = "../../../docs/openapi/dbcheck-web.yaml"

func TestOpenAPIDocumentMatchesTheRegisteredRoutes(t *testing.T) {
	var registered routeRecorder
	(&apiHandler{}).registerRoutes(&registered)
	documented := documentedRoutes(t)
	if len(registered) == 0 || len(documented) == 0 {
		t.Fatalf("registered %d routes, documented %d", len(registered), len(documented))
	}

	var undocumented, unserved []string
	for _, code := range registered {
		if !slices.ContainsFunc(documented, func(doc route) bool { return doc.servedBy(code) }) {
			undocumented = append(undocumented, code.pattern)
		}
	}
	for _, doc := range documented {
		if !slices.ContainsFunc(registered, doc.servedBy) {
			unserved = append(unserved, doc.pattern)
		}
	}
	if len(undocumented) > 0 {
		t.Errorf("routes missing from %s: %v", openAPIPath, undocumented)
	}
	if len(unserved) > 0 {
		t.Errorf("documented operations no route serves: %v", unserved)
	}
}

type route struct {
	method   string
	pattern  string
	segments []segment
}

// segment is one path segment: a literal, a wildcard ({name}), or, on the
// document side, one value of a path parameter with an enum.
type segment struct {
	literal string
	param   bool
	enum    bool
}

// servedBy reports whether the registered route code serves the documented
// route doc.
func (doc route) servedBy(code route) bool {
	if doc.method != code.method || len(doc.segments) != len(code.segments) {
		return false
	}
	for i, d := range doc.segments {
		if !d.servedBy(code.segments[i]) {
			return false
		}
	}
	return true
}

// servedBy matches a wildcard with a wildcard and a literal with the same
// literal. An enum value is served by its literal or by a wildcard.
func (d segment) servedBy(c segment) bool {
	switch {
	case d.param:
		return c.param
	case d.enum:
		return c.param || c.literal == d.literal
	default:
		return !c.param && c.literal == d.literal
	}
}

// routeRecorder lists the patterns registered on it.
type routeRecorder []route

func (r *routeRecorder) HandleFunc(pattern string, _ func(http.ResponseWriter, *http.Request)) {
	method, path, ok := strings.Cut(pattern, " ")
	if !ok {
		return // the catch-all fallback is not an API route
	}
	*r = append(*r, route{method: method, pattern: pattern, segments: splitPath(path, nil)})
}

// documentedRoutes reads every operation of the OpenAPI document. A path
// parameter with an enum gives one route per value.
func documentedRoutes(t *testing.T) []route {
	t.Helper()
	raw, err := os.ReadFile(openAPIPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths map[string]map[string]struct {
			Parameters []struct {
				Name, In string
				Schema   struct{ Enum []string }
			}
		}
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", openAPIPath, err)
	}
	var out []route
	for path, ops := range doc.Paths {
		for method, op := range ops {
			method = strings.ToUpper(method)
			values := []map[string]string{{}}
			for _, p := range op.Parameters {
				if p.In == "path" && len(p.Schema.Enum) > 0 {
					values = withEach(values, p.Name, p.Schema.Enum)
				}
			}
			for _, v := range values {
				pattern := method + " " + path
				for name, value := range v {
					pattern = strings.Replace(pattern, "{"+name+"}", value, 1)
				}
				out = append(out, route{method: method, pattern: pattern, segments: splitPath(path, v)})
			}
		}
	}
	return out
}

// withEach returns every combination of the given assignments with name set
// to one of values.
func withEach(assignments []map[string]string, name string, values []string) []map[string]string {
	var out []map[string]string
	for _, a := range assignments {
		for _, v := range values {
			next := map[string]string{name: v}
			for k, old := range a {
				next[k] = old
			}
			out = append(out, next)
		}
	}
	return out
}

func splitPath(path string, enumValues map[string]string) []segment {
	var out []segment
	for _, s := range strings.Split(strings.Trim(path, "/"), "/") {
		name, isParam := strings.CutPrefix(s, "{")
		name = strings.TrimSuffix(name, "}")
		value, isEnum := enumValues[name]
		switch {
		case isParam && isEnum:
			out = append(out, segment{literal: value, enum: true})
		case isParam:
			out = append(out, segment{param: true})
		default:
			out = append(out, segment{literal: s})
		}
	}
	return out
}
