package web

import (
	"fmt"
	"net/http"
	"testing"

	"dbcheck/reporter/internal/users"
)

func TestPublishingAFinalReleaseDemotesThePreviousLatest(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	token := f.token("admin")

	f.mustPublish(newTestRelease(t, "1.1.0"))
	f.mustPublish(newTestRelease(t, "1.2.0"))
	f.mustPublish(newTestRelease(t, "1.3.0-rc1"))

	want := map[string]string{"1.1.0": "deprecated", "1.2.0": "latest", "1.3.0-rc1": "pre-release"}
	if got := f.statuses(token); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
}

func TestPromotingDemotesThePreviousLatest(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	token := f.token("admin")
	f.mustPublish(newTestRelease(t, "1.2.0"))
	f.mustPublish(newTestRelease(t, "1.3.0-rc1"))

	rec := f.do(http.MethodPost, "/api/releases/1.3.0-rc1/promote", token, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("promote: %d %s", rec.Code, rec.Body)
	}
	want := map[string]string{"1.2.0": "deprecated", "1.3.0-rc1": "latest"}
	if got := f.statuses(token); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
}

func TestReleaseListFollowsTheRole(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	admin := f.token("admin")
	for _, v := range []string{"1.0.0", "1.1.0", "1.2.0", "1.3.0-rc1"} {
		r := newTestRelease(t, v)
		r.PublishedAt = fmt.Sprintf("2026-09-0%cT08:00:00.000Z", v[2]+1)
		f.mustPublish(r)
	}
	rec := f.do(http.MethodPost, "/api/releases/1.0.0/revoke", admin, map[string]string{"reason": " 锁表 "})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("revoke: %d %s", rec.Code, rec.Body)
	}

	var versions []string
	for _, r := range f.releases(admin) {
		versions = append(versions, r.Version+":"+r.Status)
		if r.Version == "1.0.0" && (r.RevokeReason == nil || *r.RevokeReason != "锁表") {
			t.Fatalf("revoke reason = %v", r.RevokeReason)
		}
	}
	if want := "[1.3.0-rc1:pre-release 1.2.0:latest 1.1.0:deprecated 1.0.0:revoked]"; fmt.Sprint(versions) != want {
		t.Fatalf("admin list = %v, want %s", versions, want)
	}
	versions = nil
	for _, r := range f.releases(f.token("user")) {
		versions = append(versions, r.Version)
	}
	if fmt.Sprint(versions) != "[1.2.0 1.1.0]" {
		t.Fatalf("engineer list = %v", versions)
	}
}

func TestReleaseStatusActionsFollowTheTable(t *testing.T) {
	f := newPlatformFixture(t)
	f.enablePublish()
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	admin := f.token("admin")
	f.mustPublish(newTestRelease(t, "1.2.0"))

	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/deprecate", f.token("user"), nil), apiError{http.StatusForbidden, "forbidden", "管理员"})
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/9.9.9/promote", admin, nil), apiError{http.StatusNotFound, "not_found", "9.9.9"})
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/restore", admin, nil), apiError{http.StatusBadRequest, "invalid", "1.2.0"})
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/revoke", admin, map[string]string{"reason": "  "}), apiError{http.StatusBadRequest, "invalid", "原因"})
	expectAPIError(t, f.do(http.MethodPost, "/api/releases/1.2.0/archive", admin, nil), apiError{http.StatusNotFound, "not_found", ""})

	steps := []struct{ action, want string }{
		{"deprecate", "deprecated"}, // no latest release is valid
		{"revoke", "revoked"},
		{"restore", "deprecated"},
		{"promote", "latest"},
	}
	for _, s := range steps {
		rec := f.do(http.MethodPost, "/api/releases/1.2.0/"+s.action, admin, map[string]string{"reason": "原因"})
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s: %d %s", s.action, rec.Code, rec.Body)
		}
		if got := f.statuses(admin)["1.2.0"]; got != s.want {
			t.Fatalf("after %s: %s, want %s", s.action, got, s.want)
		}
	}
	if r := f.releases(admin)[0]; r.RevokeReason != nil {
		t.Fatalf("restore kept the revoke reason %q", *r.RevokeReason)
	}
}
