package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// fakeAccessor is an in-memory ClusterAccessor for wiring tests.
type fakeAccessor struct{}

func (fakeAccessor) Name() string      { return "test" }
func (fakeAccessor) Namespace() string { return "flink-operator" }

func (fakeAccessor) GetFlinkDeployment(_ context.Context, name string) (*unstructured.Unstructured, error) {
	return fakeDeployment(name), nil
}

func (fakeAccessor) ListFlinkDeployments(_ context.Context) (*unstructured.UnstructuredList, error) {
	return &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*fakeDeployment("flink-sql-job-demo")}}, nil
}

func (fakeAccessor) PatchFlinkDeployment(_ context.Context, _ string, _ []byte) error { return nil }
func (fakeAccessor) ListPods(_ context.Context, _ string) ([]cluster.PodInfo, error) {
	return []cluster.PodInfo{{Name: "jm-0", Component: "jobmanager", Phase: "Running", Ready: "1/1"}}, nil
}
func (fakeAccessor) CountPods(_ context.Context, _ string) (int, error) { return 0, nil }
func (fakeAccessor) PodLogs(_ context.Context, _, _ string, _ int64) (string, error) {
	return "log line", nil
}
func (fakeAccessor) PodLogsForPod(_ context.Context, _, _, _ string, _ int64) (string, error) {
	return "log line", nil
}
func (fakeAccessor) Exec(_ context.Context, _, _ string, _ []string) (*cluster.ExecResult, error) {
	return &cluster.ExecResult{}, nil
}
func (fakeAccessor) ListEvents(_ context.Context, _ string) ([]cluster.EventInfo, error) {
	return nil, nil
}

func fakeDeployment(name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkDeployment",
		"metadata":   map[string]any{"name": name, "namespace": "flink-operator"},
		"spec":       map[string]any{"job": map[string]any{"state": "running", "parallelism": int64(2), "upgradeMode": "last-state"}},
		"status": map[string]any{
			"jobStatus":      map[string]any{"state": "RUNNING", "jobId": "abc123"},
			"lifecycleState": "STABLE",
		},
	}}
	u.SetName(name)
	u.SetNamespace("flink-operator")
	u.SetCreationTimestamp(metav1.Now())
	return u
}

func testServer(t *testing.T) http.Handler {
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
	staticFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<html>console</html>")},
	}
	return New(cfg, svc, nil, nil, a, staticFS).Handler()
}

func login(t *testing.T, h http.Handler) string {
	t.Helper()
	body := strings.NewReader(`{"username":"admin","password":"secret"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/login", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login: got %d, want 200", rec.Code)
	}
	cookie := rec.Header().Get("Set-Cookie")
	if cookie == "" {
		t.Fatal("login: no Set-Cookie")
	}
	return cookie
}

func TestServesEmbeddedIndex(t *testing.T) {
	h := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /: got %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "console") {
		t.Fatalf("GET /: body did not contain index content: %q", rec.Body.String())
	}
}

func TestProtectedRouteRequiresAuth(t *testing.T) {
	h := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/jobs unauth: got %d, want 401", rec.Code)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	h := testServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"username":"admin","password":"nope"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad login: got %d, want 401", rec.Code)
	}
}

func TestListJobsAuthenticated(t *testing.T) {
	h := testServer(t)
	cookie := login(t, h)
	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	req.Header.Set("Cookie", cookie)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/jobs: got %d, want 200", rec.Code)
	}
	var resp struct {
		Jobs []flink.JobSummary `json:"jobs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("jobs: got %d, want 1", len(resp.Jobs))
	}
	j := resp.Jobs[0]
	if j.JobName != "demo" {
		t.Errorf("jobName: got %q, want demo", j.JobName)
	}
	if j.StatusText != "RUNNING/STABLE" || !j.Healthy {
		t.Errorf("status: got %q healthy=%v, want RUNNING/STABLE healthy=true", j.StatusText, j.Healthy)
	}
}

func TestApiNotFoundDoesNotFallThroughToIndex(t *testing.T) {
	h := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/does-not-exist", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown api route: got %d, want 404", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "console") {
		t.Fatal("api 404 should not serve index.html")
	}
}
