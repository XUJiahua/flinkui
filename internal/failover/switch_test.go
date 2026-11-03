package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/store"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type stopObservation struct {
	cluster.ClusterAccessor
	pods int
	err  error
}

func (a stopObservation) CountPods(_ context.Context, selector string) (int, error) {
	if selector != "app=orders,component in (jobmanager,taskmanager)" {
		return 0, errors.New("selector must cover both Flink pod types")
	}
	return a.pods, a.err
}

func TestWaitLocalStoppedRequiresSuccessfulZeroCount(t *testing.T) {
	s := &Service{cfg: &config.Config{StopTimeoutSec: 1}}
	for _, tc := range []struct {
		name string
		acc  stopObservation
		want error
	}{
		{name: "zero pods"},
		{name: "remaining pod", acc: stopObservation{pods: 1}, want: context.DeadlineExceeded},
		{name: "API unavailable", acc: stopObservation{err: errors.New("unavailable")}, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			err := s.waitLocalStopped(ctx, tc.acc, "orders", func(int) {})
			if !errors.Is(err, tc.want) {
				t.Fatalf("waitLocalStopped returned %v, want %v", err, tc.want)
			}
		})
	}
}

func TestValidatePromotion(t *testing.T) {
	g := grp()
	for _, tc := range []struct {
		name, phase, owner string
		epoch              int64
		force, allowed     bool
	}{
		{"released", store.PhaseReleased, g.PeerClusterID, 1, false, true},
		{"stable peer", store.PhaseStable, g.PeerClusterID, 1, false, false},
		{"forced stable peer", store.PhaseStable, g.PeerClusterID, 1, true, true},
		{"forced stable self", store.PhaseStable, g.ClusterID, 1, true, false},
		{"forced concurrent release", store.PhaseReleasing, g.PeerClusterID, 1, true, false},
		{"forced concurrent promotion", store.PhasePromoting, g.PeerClusterID, 1, true, false},
		{"epoch exhausted", store.PhaseReleased, g.PeerClusterID, math.MaxInt64, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &store.HandoffRecord{Group: g.Name, Phase: tc.phase, ActiveClusterID: tc.owner, Epoch: tc.epoch}
			if err := validatePromotion(g, rec, tc.force); (err == nil) != tc.allowed {
				t.Fatalf("validatePromotion returned %v, allowed = %v", err, tc.allowed)
			}
		})
	}
	if err := validatePromotion(g, nil, true); err == nil {
		t.Fatal("forced promotion accepted a missing handoff")
	}
}

func TestPromotedJobReadyRequiresFreshRunningJob(t *testing.T) {
	for _, tc := range []struct {
		id, state, lifecycle string
		ready                bool
	}{
		{"old", "RUNNING", "STABLE", false},
		{"", "RUNNING", "STABLE", false},
		{"new", "RUNNING", "DEPLOYING", false},
		{"new", "FAILED", "STABLE", false},
		{"new", "RUNNING", "STABLE", true},
	} {
		dep := &unstructured.Unstructured{Object: map[string]any{
			"status": map[string]any{
				"lifecycleState": tc.lifecycle,
				"jobStatus":      map[string]any{"jobId": tc.id, "state": tc.state},
			},
		}}
		if got := promotedJobReady(dep, "old"); got != tc.ready {
			t.Errorf("readiness for %+v = %v", tc, got)
		}
	}
}

func (a stopObservation) GetFlinkDeployment(context.Context, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("status unavailable")
}

func (a stopObservation) PatchFlinkDeployment(context.Context, string, []byte) error {
	return nil
}

func TestReleaseDoesNotPublishHandoffWhenPodsRemain(t *testing.T) {
	g := grp()
	g.HandoffKey = "handoff"
	g.FencingKey = "token"
	g.Deployment = "orders"
	record := store.HandoffRecord{Group: g.Name, ActiveClusterID: g.ClusterID, Epoch: 5, Phase: store.PhaseStable}
	var mu sync.Mutex
	etag := `"stable"`
	var tokenWrites int
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/token") {
			tokenWrites++
			return
		}
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", etag)
			_ = json.NewEncoder(w).Encode(record)
		case http.MethodPut:
			if r.Header.Get("If-Match") != etag {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			etag = fmt.Sprintf("%q", record.Phase)
		}
	}))
	defer endpoint.Close()
	coord, err := store.NewCoord(context.Background(), config.S3Config{
		Endpoint: endpoint.URL, Bucket: "flink", PathStyle: true, AccessKey: "test", SecretKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	s := NewService(&config.Config{StopTimeoutSec: 1}, coord, nil)
	s.accs[g.Namespace] = stopObservation{pods: 1}
	task := s.tasks.create(g.Name, OpRelease, []string{StepSavepoint, StepSuspendLocal, StepWaitStopped, StepTokenNeutral, StepWriteHandoff})
	s.doRelease(task.ID, g)
	result, _ := s.GetTask(task.ID)
	if result.Status != TaskFailed || !strings.Contains(result.Error, StepWaitStopped) {
		t.Fatalf("unexpected task result: %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if tokenWrites != 0 || record.Phase != store.PhaseReleasing || record.Epoch != 5 {
		t.Fatalf("unsafe release: token writes=%d, handoff=%+v", tokenWrites, record)
	}
}
