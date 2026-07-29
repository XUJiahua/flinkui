package secretsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func fakeBaoServer(t *testing.T, kv map[string]map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/kv/data/")
		data, ok := kv[path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data}})
	}))
}

// TestRestartTargetsResolution unit-tests target resolution for each mode.
func TestRestartTargetsResolution(t *testing.T) {
	acc := newFakeAccessor()
	acc.deployments = []unstructured.Unstructured{
		mkDeployment("job-a", "flink-job-env"),
		mkDeployment("job-b", "flink-job-env", "flink-s3-credentials"),
		mkDeployment("job-c", "flink-s3-credentials"),
	}
	cache := newDeploymentCache(context.Background(), acc)

	cases := []struct {
		name string
		item config.SecretSyncItem
		want []string
	}{
		{"references-shared", config.SecretSyncItem{SecretName: "flink-job-env"}, []string{"job-a", "job-b"}},
		{"references-narrow", config.SecretSyncItem{SecretName: "flink-s3-credentials"}, []string{"job-b", "job-c"}},
		{"references-none", config.SecretSyncItem{SecretName: "unused-secret"}, nil},
		{"all", config.SecretSyncItem{SecretName: "flink-job-env", Restart: config.RestartSpec{Mode: "all"}}, []string{"job-a", "job-b", "job-c"}},
		{"none", config.SecretSyncItem{SecretName: "flink-job-env", Restart: config.RestartSpec{Mode: "none"}}, nil},
		{"list", config.SecretSyncItem{SecretName: "flink-job-env", Restart: config.RestartSpec{Deployments: []string{"job-x", "job-a"}}}, []string{"job-a", "job-x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := restartTargets(tc.item, cache)
			if err != nil {
				t.Fatalf("restartTargets: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("targets = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReferencesModeIsDefaultAndRestartsOnlyConsumers proves the default mode
// restarts exactly the deployments that reference the changed secret.
func TestReferencesModeIsDefaultAndRestartsOnlyConsumers(t *testing.T) {
	kv := map[string]map[string]string{
		"p/job-env": {"KAFKA": "k1"},
	}
	srv := fakeBaoServer(t, kv)
	defer srv.Close()

	acc := newFakeAccessor()
	acc.deployments = []unstructured.Unstructured{
		mkDeployment("job-a", "flink-job-env"), // consumes it
		mkDeployment("job-b", "flink-job-env"), // consumes it
		mkDeployment("job-c", "other-secret"),  // does NOT
	}
	cfg := config.SecretSyncConfig{
		Enabled: true, AutoRestart: true,
		OpenBao: config.OpenBaoConfig{Addr: srv.URL, KVMount: "kv", Token: "t"},
		// No Restart block => references mode (default).
		Items: []config.SecretSyncItem{{SecretName: "flink-job-env", KVPath: "p/job-env"}},
	}
	syncer, err := New(acc, cfg)
	if err != nil || syncer == nil {
		t.Fatalf("New: %v", err)
	}
	syncer.syncOnce(context.Background())

	if acc.restarts["job-a"] != 1 || acc.restarts["job-b"] != 1 {
		t.Errorf("consumers should restart once: a=%d b=%d", acc.restarts["job-a"], acc.restarts["job-b"])
	}
	if acc.restarts["job-c"] != 0 {
		t.Errorf("non-consumer job-c must NOT restart, got %d", acc.restarts["job-c"])
	}
}
