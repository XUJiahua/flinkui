package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/secretsync"
	"github.com/gin-gonic/gin"
)

// testServerWithSync builds the API handler with a secret-sync Syncer wired in.
func testServerWithSync(t *testing.T, ss *secretsync.Syncer) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{
		DeploymentPrefix: "flink-sql-job-",
		StatusPollSec:    5,
		LogTailLines:     200,
		Auth:             config.AuthConfig{Username: "admin", Password: "secret", SessionSecret: "test-secret"},
		Cluster:          config.ClusterConfig{Name: "test", Namespace: "flink-operator"},
	}
	svc := flink.NewService(fakeAccessor{}, cfg)
	a := auth.New(cfg.Auth)
	staticFS := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("<html>console</html>")}}
	return New(cfg, svc, nil, nil, ss, a, staticFS).Handler()
}

// fakeBao serves a minimal OpenBao KV v2 for the syncer.
func fakeBao(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"data": map[string]any{"access-key": "ak", "secret-key": "sk"}},
		})
	}))
}

func TestSecretSyncDisabled(t *testing.T) {
	h := testServer(t) // ss == nil
	cookie := login(t, h)

	req := httptest.NewRequest(http.MethodGet, "/api/secretsync", nil)
	req.Header.Set("Cookie", cookie)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/secretsync: got %d, want 200", rec.Code)
	}
	var resp struct {
		Enabled bool `json:"enabled"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Enabled {
		t.Fatalf("disabled: expected enabled=false")
	}

	// Manual trigger must 409 when disabled.
	req = httptest.NewRequest(http.MethodPost, "/api/secretsync/sync", nil)
	req.Header.Set("Cookie", cookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST sync (disabled): got %d, want 409", rec.Code)
	}
}

func TestSecretSyncEnabled(t *testing.T) {
	bao := fakeBao(t)
	defer bao.Close()

	cfg := config.SecretSyncConfig{
		Enabled: true, AutoRestart: false,
		OpenBao: config.OpenBaoConfig{Addr: bao.URL, KVMount: "kv", Token: "t"},
		Items:   []config.SecretSyncItem{{SecretName: "flink-s3-credentials", KVPath: "config/x/s3"}},
	}
	ss, err := secretsync.New(fakeAccessor{}, cfg)
	if err != nil || ss == nil {
		t.Fatalf("secretsync.New: %v (ss=%v)", err, ss)
	}
	h := testServerWithSync(t, ss)
	cookie := login(t, h)

	// Manual trigger runs a sync and returns status.
	req := httptest.NewRequest(http.MethodPost, "/api/secretsync/sync", nil)
	req.Header.Set("Cookie", cookie)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST sync: got %d, want 200", rec.Code)
	}

	// Status reflects the enabled loop + the synced item.
	req = httptest.NewRequest(http.MethodGet, "/api/secretsync", nil)
	req.Header.Set("Cookie", cookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/secretsync: got %d, want 200", rec.Code)
	}
	var st secretsync.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if !st.Enabled {
		t.Fatalf("expected enabled=true")
	}
	if len(st.Items) != 1 || !st.Items[0].OK || st.Items[0].SecretName != "flink-s3-credentials" {
		t.Fatalf("unexpected items: %+v", st.Items)
	}
	if st.LastSyncUnix == 0 {
		t.Fatalf("expected LastSyncUnix set after sync")
	}
}
