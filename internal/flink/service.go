// Package flink implements FlinkDeployment lifecycle operations by translating
// the proven logic in scripts/job.sh into direct K8s API calls (design §4.2/§5).
package flink

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Service holds the cluster accessor and per-deployment locks. All mutating
// operations on a single FlinkDeployment are serialized (design §6 concurrency).
type Service struct {
	acc cluster.ClusterAccessor
	cfg *config.Config

	mu    sync.Mutex
	locks map[string]*sync.Mutex

	ops *operationStore
}

// NewService constructs a Service.
func NewService(acc cluster.ClusterAccessor, cfg *config.Config) *Service {
	return &Service{acc: acc, cfg: cfg, locks: map[string]*sync.Mutex{}, ops: newOperationStore()}
}

// lockFor returns (and lazily creates) the mutex guarding a deployment.
func (s *Service) lockFor(name string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[name]
	if !ok {
		m = &sync.Mutex{}
		s.locks[name] = m
	}
	return m
}

// List returns all FlinkDeployments as dashboard summaries. It reads from the
// informer cache when the accessor provides a synced one (design §3.3), falling
// back to a live API list otherwise. Results are sorted by deployment name so
// the payload has a deterministic default order (the informer lister returns
// items in nondeterministic map order); the UI can re-sort client-side.
func (s *Service) List(ctx context.Context) ([]JobSummary, error) {
	if cl, ok := s.acc.(cluster.CachedLister); ok {
		if items, synced := cl.CachedListFlinkDeployments(); synced {
			out := make([]JobSummary, 0, len(items))
			for _, u := range items {
				out = append(out, s.summaryFrom(u))
			}
			sortSummaries(out)
			return out, nil
		}
	}
	list, err := s.acc.ListFlinkDeployments(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]JobSummary, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, s.summaryFrom(&list.Items[i]))
	}
	sortSummaries(out)
	return out, nil
}

// sortSummaries orders summaries by deployment name for a stable default order.
func sortSummaries(out []JobSummary) {
	sort.Slice(out, func(i, j int) bool { return out[i].Deployment < out[j].Deployment })
}

// Get returns a single deployment's detail (status + pods + events).
func (s *Service) Get(ctx context.Context, name string) (*JobDetail, error) {
	dep := s.cfg.DeploymentName(name)
	u, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		if errors.IsNotFound(err) {
			return &JobDetail{JobSummary: notFoundSummary(s.acc.Namespace(), dep, s.cfg.JobName(dep))}, nil
		}
		return nil, err
	}
	sum := s.summaryFrom(u)
	pods, _ := s.acc.ListPods(ctx, "app="+dep)
	events, _ := s.acc.ListEvents(ctx, dep)
	return &JobDetail{JobSummary: sum, Pods: pods, Events: events}, nil
}

// StatusText returns just the combined status string for a deployment,
// mirroring job.sh get_status (used by the WebSocket pusher).
func (s *Service) StatusText(ctx context.Context, dep string) string {
	u, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		if errors.IsNotFound(err) {
			return StatusNotFound
		}
		return StatusUnreachable
	}
	return s.summaryFrom(u).StatusText
}

// summaryFrom builds a JobSummary from an unstructured FlinkDeployment,
// applying the same fallbacks as scripts/job.sh get_status().
func (s *Service) summaryFrom(u *unstructured.Unstructured) JobSummary {
	name := u.GetName()
	ns := u.GetNamespace()
	jobState, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "state")
	lifecycle, _, _ := unstructured.NestedString(u.Object, "status", "lifecycleState")
	jobID, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "jobId")
	specState, _, _ := unstructured.NestedString(u.Object, "spec", "job", "state")
	upgradeMode, _, _ := unstructured.NestedString(u.Object, "spec", "job", "upgradeMode")
	parallelism, _, _ := unstructured.NestedInt64(u.Object, "spec", "job", "parallelism")

	statusText := combinedStatus(jobState, lifecycle, specState)
	return JobSummary{
		Namespace:      ns,
		Deployment:     name,
		JobName:        s.cfg.JobName(name),
		JobState:       jobState,
		LifecycleState: lifecycle,
		JobID:          jobID,
		DesiredState:   specState,
		UpgradeMode:    upgradeMode,
		Parallelism:    parallelism,
		StatusText:     statusText,
		Healthy:        statusText == "RUNNING/STABLE",
		Reachable:      true,
	}
}

// combinedStatus mirrors job.sh get_status(): when .status is empty fall back
// to spec.job.state so we don't spuriously show UNKNOWN.
func combinedStatus(jobState, lifecycle, specState string) string {
	if jobState == "" && lifecycle == "" {
		switch specState {
		case "suspended":
			return "SUSPENDED/—"
		case "running":
			return "STARTING/—"
		default:
			return "UNKNOWN/UNKNOWN"
		}
	}
	js := jobState
	if js == "" {
		js = StatusUnknown
	}
	lc := lifecycle
	if lc == "" {
		lc = StatusUnknown
	}
	return js + "/" + lc
}

func notFoundSummary(ns, dep, job string) JobSummary {
	return JobSummary{
		Namespace: ns, Deployment: dep, JobName: job,
		StatusText: StatusNotFound, Reachable: false,
	}
}

// mergePatchState builds a merge patch that sets spec.job.state.
func statePatch(state string) []byte {
	b, _ := json.Marshal(map[string]any{
		"spec": map[string]any{"job": map[string]any{"state": state}},
	})
	return b
}

// Suspend sets spec.job.state=suspended (design §4.2).
func (s *Service) Suspend(ctx context.Context, name string) error {
	dep := s.cfg.DeploymentName(name)
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()
	return s.acc.PatchFlinkDeployment(ctx, dep, statePatch("suspended"))
}

// Resume sets spec.job.state=running (design §4.2).
func (s *Service) Resume(ctx context.Context, name string) error {
	dep := s.cfg.DeploymentName(name)
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()
	return s.acc.PatchFlinkDeployment(ctx, dep, statePatch("running"))
}

// Restart = suspend -> wait for JM pod to reach zero -> resume (last-state).
func (s *Service) Restart(ctx context.Context, name string) error {
	dep := s.cfg.DeploymentName(name)
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()

	if err := s.acc.PatchFlinkDeployment(ctx, dep, statePatch("suspended")); err != nil {
		return fmt.Errorf("suspend failed: %w", err)
	}
	s.waitStopped(ctx, dep)
	if err := s.acc.PatchFlinkDeployment(ctx, dep, statePatch("running")); err != nil {
		return fmt.Errorf("resume failed: %w", err)
	}
	return nil
}

// waitStopped polls until the JM pod count reaches zero or the timeout elapses.
func (s *Service) waitStopped(ctx context.Context, dep string) {
	s.waitStoppedProgress(ctx, dep, nil)
}

// waitStoppedProgress is waitStopped with an optional per-tick callback that
// reports the current JobManager pod count (for restart progress display).
func (s *Service) waitStoppedProgress(ctx context.Context, dep string, onTick func(pods int)) {
	selector := fmt.Sprintf("app=%s,component=jobmanager", dep)
	deadline := time.Now().Add(time.Duration(s.cfg.StopTimeoutSec) * time.Second)
	for time.Now().Before(deadline) {
		n, err := s.acc.CountPods(ctx, selector)
		if err == nil {
			if onTick != nil {
				onTick(n)
			}
			if n == 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// Rollback forces a redeploy from a given savepoint/checkpoint path (design §4.2).
func (s *Service) Rollback(ctx context.Context, name, path string) error {
	if path == "" {
		return fmt.Errorf("rollback requires a savepoint/checkpoint path")
	}
	dep := s.cfg.DeploymentName(name)
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()

	nonce := time.Now().Unix()
	patch, _ := json.Marshal(map[string]any{
		"spec": map[string]any{"job": map[string]any{
			"state":                  "running",
			"initialSavepointPath":   path,
			"savepointRedeployNonce": nonce,
		}},
	})
	return s.acc.PatchFlinkDeployment(ctx, dep, patch)
}

// Logs tails JobManager or TaskManager logs across matching pods (design §4.3).
// component selects the pod role ("jobmanager" or "taskmanager"); anything else
// defaults to jobmanager.
func (s *Service) Logs(ctx context.Context, name, component string, tail int64) (string, error) {
	dep := s.cfg.DeploymentName(name)
	if tail <= 0 {
		tail = s.cfg.LogTailLines
	}
	selector := fmt.Sprintf("app=%s,component=%s", dep, normalizeComponent(component))
	return s.acc.PodLogs(ctx, selector, "flink-main-container", tail)
}

// normalizeComponent restricts the pod-role selector to the two valid values,
// defaulting to jobmanager.
func normalizeComponent(component string) string {
	if component == "taskmanager" {
		return "taskmanager"
	}
	return "jobmanager"
}

// RecoveryDirs returns the deployment's configured savepoint and checkpoint
// directories (spec.flinkConfiguration state.savepoints.dir / state.checkpoints.dir).
// These are the authoritative S3 locations for the rollback recovery-point
// selector; empty strings are returned when the deployment does not set them.
func (s *Service) RecoveryDirs(ctx context.Context, name string) (savepointsDir, checkpointsDir string, err error) {
	dep := s.cfg.DeploymentName(name)
	u, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return "", "", err
	}
	savepointsDir, _, _ = unstructured.NestedString(u.Object, "spec", "flinkConfiguration", "state.savepoints.dir")
	checkpointsDir, _, _ = unstructured.NestedString(u.Object, "spec", "flinkConfiguration", "state.checkpoints.dir")
	return savepointsDir, checkpointsDir, nil
}
