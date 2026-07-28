package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/gin-gonic/gin"
)

func TestNormalizeAutoOffsetReset(t *testing.T) {
	cases := map[string]struct {
		want string
		err  bool
	}{
		"":         {"earliest", false},
		"earliest": {"earliest", false},
		"none":     {"none", false},
		"latest":   {"", true}, // unsafe for replay
		"bogus":    {"", true},
	}
	for in, exp := range cases {
		got, err := normalizeAutoOffsetReset(in)
		if (err != nil) != exp.err {
			t.Errorf("normalizeAutoOffsetReset(%q) err=%v, wantErr=%v", in, err, exp.err)
		}
		if got != exp.want {
			t.Errorf("normalizeAutoOffsetReset(%q) = %q, want %q", in, got, exp.want)
		}
	}
}

func TestResolveFromMillis(t *testing.T) {
	if v, ok := (replayRequest{FromMillis: 123}).resolveFromMillis(); !ok || v != 123 {
		t.Errorf("explicit fromMillis: got %d,%v", v, ok)
	}
	got, ok := (replayRequest{HoursAgo: 2}).resolveFromMillis()
	if !ok {
		t.Fatal("hoursAgo should resolve")
	}
	want := time.Now().Add(-2 * time.Hour).UnixMilli()
	if diff := got - want; diff < -2000 || diff > 2000 {
		t.Errorf("hoursAgo=2 resolved to %d, want ~%d", got, want)
	}
	if _, ok := (replayRequest{}).resolveFromMillis(); ok {
		t.Error("empty request should not resolve a timestamp")
	}
}

// replayTestServer builds a server with the replay feature enabled and the
// GitOps-managed flag on, using the wiring fakeAccessor (which does NOT
// implement ConfigMapAccessor).
func replayTestServer(t *testing.T) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		DeploymentPrefix: "flink-sql-job-",
		StatusPollSec:    5,
		LogTailLines:     200,
		Auth:             config.AuthConfig{Username: "admin", Password: "secret", SessionSecret: "test-secret"},
		Cluster:          config.ClusterConfig{Name: "test", Namespace: "flink-operator"},
		Replay:           config.ReplayConfig{Enabled: true, GitOpsManaged: true, SQLVolumeName: "sql-scripts", SQLFileKey: "job.sql"},
	}
	svc := flink.NewService(fakeAccessor{}, cfg)
	a, err := auth.New(cfg.Auth)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	staticFS := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("<html>console</html>")}}
	return New(cfg, svc, nil, nil, nil, a, staticFS).Handler()
}

func TestReplayRouteGatedAndGitOpsGuard(t *testing.T) {
	h := replayTestServer(t)
	cookie := login(t, h)

	// The route is registered (enabled) — a GET reaches the handler and fails at
	// the accessor layer (fake lacks ConfigMap support) => 502, not 404.
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/job1/sql", nil)
	req.Header.Set("Cookie", cookie)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusNotFound {
		t.Fatalf("replay route not registered when enabled (got 404)")
	}

	// GitOps-managed guard: starting a replay without confirmDrift => 409.
	body := strings.NewReader(`{"hoursAgo":2}`)
	req = httptest.NewRequest(http.MethodPost, "/api/jobs/job1/replay", body)
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("gitops drift guard: got %d, want 409", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "gitopsDrift") {
		t.Errorf("expected gitopsDrift flag in response: %s", rec.Body.String())
	}
}

func TestReplayRouteAbsentWhenDisabled(t *testing.T) {
	h := testServer(t) // replay disabled
	cookie := login(t, h)
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/job1/sql", nil)
	req.Header.Set("Cookie", cookie)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Disabled => route not registered => API 404 (handled by api group, not SPA).
	if rec.Code != http.StatusNotFound {
		t.Fatalf("replay route should be absent when disabled: got %d", rec.Code)
	}
}
