package web

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"dbcheck/reporter/internal/users"
)

func registration(username, email string) map[string]string {
	return map[string]string{
		"username": username, "password": username + "-pass", "displayName": "周七",
		"email": email, "team": "华南交付二部", "note": "需要 GaussDB 巡检",
	}
}

func TestRegisterSignsInAPendingEngineer(t *testing.T) {
	f := newPlatformFixture(t)

	rec := f.do(http.MethodPost, "/api/auth/register", "", registration("zhouqi", "zhouqi@example.com"))
	if rec.Code != http.StatusOK {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	var session struct {
		Token string
		User  struct{ Username, Role, Status string }
	}
	decode(t, rec, &session)
	if session.User.Username != "zhouqi" || session.User.Role != "user" || session.User.Status != "pending" {
		t.Fatalf("session user = %+v", session.User)
	}
	rec = f.do(http.MethodGet, "/api/users/me", session.Token, nil)
	var profile struct{ Email, Team, Note, Status string }
	decode(t, rec, &profile)
	if rec.Code != http.StatusOK || profile.Email != "zhouqi@example.com" || profile.Status != "pending" {
		t.Fatalf("own profile: %d %s", rec.Code, rec.Body)
	}
}

func TestRegisterRefusesATakenUsernameOrEmail(t *testing.T) {
	f := newPlatformFixture(t)
	f.insert(users.NewUser{ID: "u-user", Username: "user", DisplayName: "user", Email: "user@example.com",
		Role: users.RoleEngineer, Status: users.StatusActive})

	expectAPIError(t, f.do(http.MethodPost, "/api/auth/register", "", registration("user", "other@example.com")),
		http.StatusBadRequest, "invalid", "用户名已被占用")
	expectAPIError(t, f.do(http.MethodPost, "/api/auth/register", "", registration("zhouqi", "User@Example.com")),
		http.StatusBadRequest, "invalid", "邮箱已被注册")
}

func TestDisablingEndsEverySessionForGood(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("user", users.RoleEngineer, users.StatusActive)
	admin, first, second := f.token("admin"), f.token("user"), f.token("user")

	if rec := f.do(http.MethodPost, "/api/users/u-user/disable", admin, map[string]string{"reason": "已转岗"}); rec.Code != http.StatusOK {
		t.Fatalf("disable: %d %s", rec.Code, rec.Body)
	}
	for _, token := range []string{first, second} {
		expectAPIError(t, f.do(http.MethodGet, "/api/users/me", token, nil), http.StatusUnauthorized, "unauthorized", "")
	}

	// The sessions are gone, not just refused while disabled.
	if rec := f.do(http.MethodPost, "/api/users/u-user/enable", admin, nil); rec.Code != http.StatusOK {
		t.Fatalf("enable: %d %s", rec.Code, rec.Body)
	}
	expectAPIError(t, f.do(http.MethodGet, "/api/auth/me", first, nil), http.StatusUnauthorized, "unauthorized", "登录")
	f.token("user")
}

func TestADemotedAdminsLiveSessionActsAsAnEngineer(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("second", users.RoleAdmin, users.StatusActive)
	admin, second := f.token("admin"), f.token("second")

	if rec := f.do(http.MethodGet, "/api/users", second, nil); rec.Code != http.StatusOK {
		t.Fatalf("list before demotion: %d %s", rec.Code, rec.Body)
	}
	if rec := f.do(http.MethodPost, "/api/users/u-second/demote", admin, nil); rec.Code != http.StatusOK {
		t.Fatalf("demote: %d %s", rec.Code, rec.Body)
	}
	expectAPIError(t, f.do(http.MethodGet, "/api/users", second, nil), http.StatusForbidden, "forbidden", "管理员")
}

func TestAdminGuardsKeepOneActiveAdmin(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	admin := f.token("admin")

	expectAPIError(t, f.do(http.MethodPost, "/api/users/u-admin/demote", admin, nil), http.StatusBadRequest, "invalid", "自己")
	expectAPIError(t, f.do(http.MethodPost, "/api/users/u-admin/disable", admin, map[string]string{"reason": "原因"}),
		http.StatusBadRequest, "invalid", "自己")
	expectAPIError(t, f.do(http.MethodPost, "/api/users/u-nobody/enable", admin, nil), http.StatusNotFound, "not_found", "用户不存在")
}

func TestAccountActionsRecordTheActingAdminAndTime(t *testing.T) {
	f := newPlatformFixture(t)
	f.addUser("admin", users.RoleAdmin, users.StatusActive)
	f.addUser("lisi", users.RoleEngineer, users.StatusPending)
	admin := f.token("admin")

	f.do(http.MethodPost, "/api/users/u-lisi/approve", admin, nil)
	f.clock.Advance(time.Hour)
	rec := f.do(http.MethodPost, "/api/users/u-lisi/promote", admin, nil)

	type action struct{ Action, By, At string }
	var profile struct {
		Role    string
		Actions []action
	}
	decode(t, rec, &profile)
	want := []action{
		{"approve", "admin", "2026-10-01T08:00:00.000Z"},
		{"promote", "admin", "2026-10-01T09:00:00.000Z"},
	}
	if rec.Code != http.StatusOK || profile.Role != "admin" || !slices.Equal(profile.Actions, want) {
		t.Fatalf("promote: %d %s", rec.Code, rec.Body)
	}
}

func TestRejectedApplicantResubmitsBackToPending(t *testing.T) {
	f := newPlatformFixture(t)
	f.insert(users.NewUser{ID: "u-zhaoliu", Username: "zhaoliu", DisplayName: "赵六", Email: "zhaoliu@partner.com",
		Role: users.RoleEngineer, Status: users.StatusRejected, Reason: "请补充说明"})
	token := f.token("zhaoliu")

	rec := f.do(http.MethodPost, "/api/auth/resubmit", token, map[string]string{"displayName": "赵六", "team": "华东交付一部", "note": "已确认"})
	var profile struct {
		Status, Team, Email string
		Reason              *string
	}
	decode(t, rec, &profile)
	if rec.Code != http.StatusOK || profile.Status != "pending" || profile.Team != "华东交付一部" ||
		profile.Email != "zhaoliu@partner.com" || profile.Reason != nil {
		t.Fatalf("resubmit: %d %s", rec.Code, rec.Body)
	}
	expectAPIError(t, f.do(http.MethodPost, "/api/auth/resubmit", token, map[string]string{"displayName": "赵六", "team": "x"}),
		http.StatusBadRequest, "invalid", "被拒绝")
}
