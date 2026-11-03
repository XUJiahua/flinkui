package failover

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/store"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const integrationSavepoint = "s3://flink/savepoints/orders/savepoint-1"

type handoffCluster struct {
	cluster.ClusterAccessor
	namespace, state, jobID, restorePath string
	nonce                                int64
}

func (a *handoffCluster) Namespace() string { return a.namespace }

func (a *handoffCluster) GetFlinkDeployment(_ context.Context, name string) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"job": map[string]any{
			"state": a.state, "initialSavepointPath": a.restorePath, "savepointRedeployNonce": a.nonce,
		}},
		"status": map[string]any{
			"lifecycleState": "STABLE", "jobStatus": map[string]any{"jobId": a.jobID, "state": a.state},
		},
	}}
	u.SetName(name)
	u.SetNamespace(a.namespace)
	if a.state == "running" {
		_ = unstructured.SetNestedField(u.Object, "RUNNING", "status", "jobStatus", "state")
	}
	return u, nil
}

func (a *handoffCluster) PatchFlinkDeployment(_ context.Context, _ string, patch []byte) error {
	var body struct {
		Spec struct {
			Job struct {
				State string `json:"state"`
				Path  string `json:"initialSavepointPath"`
				Nonce int64  `json:"savepointRedeployNonce"`
			} `json:"job"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(patch, &body); err != nil {
		return err
	}
	a.state, a.restorePath, a.nonce = body.Spec.Job.State, body.Spec.Job.Path, body.Spec.Job.Nonce
	if a.state == "running" {
		a.jobID = "restored-" + a.namespace
	}
	return nil
}

func (a *handoffCluster) CountPods(context.Context, string) (int, error) {
	if a.state == "suspended" {
		return 0, nil
	}
	return 2, nil
}

func (*handoffCluster) ListPods(context.Context, string) ([]cluster.PodInfo, error) { return nil, nil }
func (*handoffCluster) ListEvents(context.Context, string) ([]cluster.EventInfo, error) {
	return nil, nil
}

func (*handoffCluster) Exec(_ context.Context, _, _ string, cmd []string) (*cluster.ExecResult, error) {
	if len(cmd) > 3 && cmd[3] == "POST" {
		return &cluster.ExecResult{Stdout: `{"request-id":"sp-1"}`}, nil
	}
	return &cluster.ExecResult{Stdout: fmt.Sprintf(`{"status":{"id":"COMPLETED"},"operation":{"location":%q}}`, integrationSavepoint)}, nil
}

func TestTwoControllersPlannedHandoff(t *testing.T) {
	type object struct {
		body []byte
		etag string
	}
	objects := make(map[string]object)
	var mu sync.Mutex
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		current, exists := objects[r.URL.Path]
		switch r.Method {
		case http.MethodGet:
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", current.etag)
			_, _ = w.Write(current.body)
		case http.MethodPut:
			if match := r.Header.Get("If-Match"); (match != "" && match != current.etag) ||
				(r.Header.Get("If-None-Match") == "*" && exists) {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			next := object{body: body, etag: fmt.Sprintf("%q", sha256.Sum256(body))}
			objects[r.URL.Path] = next
			w.Header().Set("ETag", next.etag)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer endpoint.Close()

	ctx := context.Background()
	newSide := func(id, peer, state string) (*Service, *handoffCluster, config.LocalHAGroup) {
		t.Helper()
		cfg := &config.Config{Cluster: config.ClusterConfig{Namespace: id}, StopTimeoutSec: 1, SavepointTimeoutSec: 1}
		group := cfg.NormalizeGroup(config.LocalHAGroup{Name: "orders", ClusterID: id, PeerClusterID: peer})
		cfg.HA.Groups = []config.LocalHAGroup{group}
		coord, err := store.NewCoord(ctx, config.S3Config{
			Endpoint: endpoint.URL, Bucket: "flink", PathStyle: true, AccessKey: "test", SecretKey: "test",
		})
		if err != nil {
			t.Fatal(err)
		}
		acc := &handoffCluster{namespace: id, state: state, jobID: "original-" + id}
		s := NewService(cfg, coord, nil)
		s.accs[id] = acc
		return s, acc, group
	}
	a, aCluster, group := newSide("a", "b", "running")
	b, bCluster, _ := newSide("b", "a", "suspended")
	if err := a.Claim(group.Name); err != nil {
		t.Fatal(err)
	}
	if err := b.Claim(group.Name); err == nil {
		t.Fatal("standby claimed an existing group")
	}

	waitTask := func(s *Service, task *HATask, err error) *HATask {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			current, ok := s.GetTask(task.ID)
			if ok && current.Status != TaskRunning {
				return current
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("task %s did not finish", task.ID)
		return nil
	}

	task, err := b.Promote(group.Name, false, false)
	if result := waitTask(b, task, err); result.Status != TaskFailed || bCluster.state != "suspended" {
		t.Fatalf("premature promote proceeded: %+v", result)
	}
	task, err = a.Release(group.Name)
	if result := waitTask(a, task, err); result.Status != TaskSucceeded || result.RecoveryPoint.Path != integrationSavepoint {
		t.Fatalf("release did not publish the savepoint: %+v", result)
	}
	rec, ok, err := a.coord.ReadHandoff(ctx, group.HandoffKey)
	if err != nil || !ok || rec.Phase != store.PhaseReleased || rec.Epoch != 1 || aCluster.state != "suspended" {
		t.Fatalf("unexpected released state: %+v, %v", rec, err)
	}
	token, err := a.coord.ReadToken(ctx, group.FencingKey)
	if err != nil || token != group.NeutralToken {
		t.Fatalf("expected neutral token during handoff, got %q: %v", token, err)
	}

	task, err = b.Promote(group.Name, false, false)
	if result := waitTask(b, task, err); result.Status != TaskSucceeded || result.Epoch != 2 {
		t.Fatalf("promote failed: %+v", result)
	}
	rec, ok, err = b.coord.ReadHandoff(ctx, group.HandoffKey)
	if err != nil || !ok || rec.Phase != store.PhaseStable || rec.ActiveClusterID != "b" || rec.Epoch != 2 ||
		bCluster.state != "running" || bCluster.restorePath != integrationSavepoint || aCluster.state != "suspended" {
		t.Fatalf("unexpected final handoff: %+v, %v", rec, err)
	}
	token, err = b.coord.ReadToken(ctx, group.FencingKey)
	if err != nil || token != "b" {
		t.Fatalf("expected B token, got %q: %v", token, err)
	}
}
