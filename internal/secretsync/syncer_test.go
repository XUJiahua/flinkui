package secretsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/fko-demo/flinkui/internal/config"
)

// fakeAccessor implements the secretsync.Accessor interface in memory.
type fakeAccessor struct {
	mu          sync.Mutex
	secrets     map[string]map[string][]byte
	annotations map[string]string // deployment -> hash annotation
	restarts    map[string]int    // deployment -> restart (patch) count
}

func newFakeAccessor() *fakeAccessor {
	return &fakeAccessor{
		secrets:     map[string]map[string][]byte{},
		annotations: map[string]string{},
		restarts:    map[string]int{},
	}
}

func (f *fakeAccessor) Namespace() string { return "test-ns" }

func (f *fakeAccessor) GetFlinkDeployment(_ context.Context, name string) (*unstructured.Unstructured, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := &unstructured.Unstructured{Object: map[string]any{}}
	u.SetName(name)
	if h := f.annotations[name]; h != "" {
		u.SetAnnotations(map[string]string{hashAnnotation: h})
	}
	return u, nil
}

func (f *fakeAccessor) PatchFlinkDeployment(_ context.Context, name string, mergePatch []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var p map[string]any
	_ = json.Unmarshal(mergePatch, &p)
	if md, ok := p["metadata"].(map[string]any); ok {
		if ann, ok := md["annotations"].(map[string]any); ok {
			if h, ok := ann[hashAnnotation].(string); ok {
				f.annotations[name] = h
			}
		}
	}
	f.restarts[name]++
	return nil
}

func (f *fakeAccessor) GetSecret(_ context.Context, name string) (map[string][]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.secrets[name]
	return d, ok, nil
}

func (f *fakeAccessor) ApplySecret(_ context.Context, name string, data map[string][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.secrets[name] = data
	return nil
}

// TestAggregatedRestart proves: two secrets pointing at the same FlinkDeployment
// cause at most ONE restart per cycle; no change => no restart; a change to
// either secret => exactly one restart.
func TestAggregatedRestart(t *testing.T) {
	// Mutable KV backing the fake OpenBao server.
	kv := map[string]map[string]string{
		"config/cil/flink/vault-a/s3":      {"access-key": "ak1", "secret-key": "sk1"},
		"config/cil/flink/vault-a/job-env": {"KAFKA_BOOTSTRAP_SERVERS": "redpanda:9092", "REDIS_HOST": "redis"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// GET /v1/kv/data/<path>
		path := strings.TrimPrefix(r.URL.Path, "/v1/kv/data/")
		data, ok := kv[path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data}})
	}))
	defer srv.Close()

	dep := "flink-sql-job-demo-kafka-redis"
	cfg := config.SecretSyncConfig{
		Enabled:     true,
		AutoRestart: true,
		OpenBao:     config.OpenBaoConfig{Addr: srv.URL, KVMount: "kv", Token: "test-token"},
		Items: []config.SecretSyncItem{
			{SecretName: "flink-s3-credentials", KVPath: "config/cil/flink/vault-a/s3", RestartDeployment: dep},
			{SecretName: "flink-job-env", KVPath: "config/cil/flink/vault-a/job-env", RestartDeployment: dep},
		},
	}
	acc := newFakeAccessor()
	syncer, err := New(acc, cfg)
	if err != nil || syncer == nil {
		t.Fatalf("New: %v (syncer=%v)", err, syncer)
	}
	ctx := context.Background()

	// Cycle 1: baseline — content is "new" vs empty annotation => exactly 1 restart
	// (NOT 2, even though two secrets are involved).
	syncer.syncOnce(ctx)
	if got := acc.restarts[dep]; got != 1 {
		t.Fatalf("cycle1: expected 1 restart, got %d", got)
	}
	if len(acc.secrets) != 2 {
		t.Fatalf("cycle1: expected 2 secrets applied, got %d", len(acc.secrets))
	}

	// Cycle 2: nothing changed => no additional restart.
	syncer.syncOnce(ctx)
	if got := acc.restarts[dep]; got != 1 {
		t.Fatalf("cycle2 (no change): expected still 1 restart, got %d", got)
	}

	// Cycle 3: BOTH secrets change => still exactly ONE more restart (deduped).
	kv["config/cil/flink/vault-a/s3"]["access-key"] = "ak2"
	kv["config/cil/flink/vault-a/job-env"]["REDIS_HOST"] = "redis-2"
	syncer.syncOnce(ctx)
	if got := acc.restarts[dep]; got != 2 {
		t.Fatalf("cycle3 (both changed): expected 2 restarts total (one per cycle), got %d", got)
	}
	fmt.Println("OK: baseline=1, no-change=+0, both-changed=+1 (deduped per deployment)")
}

// fakeGuard vetoes restarts based on `allow`.
type fakeGuard struct{ allow bool }

func (g *fakeGuard) SafeToRestart(_ context.Context, _ string) (bool, string) {
	if g.allow {
		return true, ""
	}
	return false, "standby side"
}

// TestRestartGuard proves the HA interlock: a veto skips the restart (and records
// a reason) even when values changed; once allowed, the change triggers a restart.
func TestRestartGuard(t *testing.T) {
	kv := map[string]map[string]string{
		"p/s3": {"access-key": "ak1"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1/kv/data/")
		data, ok := kv[path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data}})
	}))
	defer srv.Close()

	dep := "job-a"
	cfg := config.SecretSyncConfig{
		Enabled: true, AutoRestart: true,
		OpenBao: config.OpenBaoConfig{Addr: srv.URL, KVMount: "kv", Token: "t"},
		Items:   []config.SecretSyncItem{{SecretName: "s3", KVPath: "p/s3", RestartDeployment: dep}},
	}
	acc := newFakeAccessor()
	syncer, _ := New(acc, cfg)

	// Guard vetoes: value is "new" but restart must be skipped, with a reason.
	syncer.SetRestartGuard(&fakeGuard{allow: false})
	syncer.syncOnce(context.Background())
	if acc.restarts[dep] != 0 {
		t.Fatalf("vetoed: expected 0 restarts, got %d", acc.restarts[dep])
	}
	if st := syncer.Status(); st.Skipped[dep] == "" {
		t.Fatalf("vetoed: expected a skip reason in status")
	}

	// Guard allows: same (still-changed) value now triggers exactly one restart.
	syncer.SetRestartGuard(&fakeGuard{allow: true})
	syncer.syncOnce(context.Background())
	if acc.restarts[dep] != 1 {
		t.Fatalf("allowed: expected 1 restart, got %d", acc.restarts[dep])
	}
}
