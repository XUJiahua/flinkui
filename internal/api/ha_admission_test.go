package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
)

func TestHAActivationRoutes(t *testing.T) {
	cfg := &config.Config{
		DeploymentPrefix: "flink-",
		Cluster:          config.ClusterConfig{Namespace: "jobs"},
		Auth:             config.AuthConfig{Username: "admin", Password: "secret", SessionSecret: "test-secret"},
		HA: config.HAConfig{Groups: []config.LocalHAGroup{
			{Name: "payments-ha", Deployment: "flink-payments"},
			{Name: "reporting", Namespace: "other"},
		}},
		Replay: config.ReplayConfig{Enabled: true},
	}
	a, err := auth.New(cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	// No coordination store or HA service: admission must still apply.
	handler := New(cfg, flink.NewService(fakeAccessor{}, cfg), nil, nil, nil, a,
		fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("ok")}}).Handler()
	cookie := login(t, handler)

	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{http.MethodPost, "payments/resume", http.StatusConflict},
		{http.MethodPost, "flink-payments/restart", http.StatusConflict},
		{http.MethodPost, "payments/rollback", http.StatusConflict},
		{http.MethodPost, "payments/replay", http.StatusConflict},
		{http.MethodPost, "payments/replay/revert", http.StatusConflict},
		{http.MethodPatch, "payments/ui/jobs/id", http.StatusConflict},
		{http.MethodPost, "payments/suspend", http.StatusOK},
		{http.MethodPost, "payments/replay/preview", http.StatusBadRequest},
		{http.MethodPost, "reporting/resume", http.StatusOK},
		{http.MethodPost, "other/resume", http.StatusOK},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/api/jobs/"+tc.path, nil)
			req.Header.Set("Cookie", cookie)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("got %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.status == http.StatusConflict && !strings.Contains(rec.Body.String(), "ha_managed_job") {
				t.Fatalf("expected HA admission rejection: %s", rec.Body.String())
			}
		})
	}

	cfg.HA.AutoAll = true
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/unlisted/resume", nil)
	req.Header.Set("Cookie", cookie)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "ha_managed_job") {
		t.Fatalf("auto_all allowed an unlisted job: %d %s", rec.Code, rec.Body.String())
	}
}
