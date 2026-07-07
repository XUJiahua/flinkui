package failover

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/google/uuid"
)

// switchStore is an in-memory registry of SwitchTasks (bounded retention).
type switchStore struct {
	mu       sync.RWMutex
	tasks    map[string]*SwitchTask
	finished []string
	maxKeep  int
}

func newSwitchStore() *switchStore {
	return &switchStore{tasks: map[string]*SwitchTask{}, maxKeep: 100}
}

func (s *switchStore) create(group, direction string, stepNames []string) *SwitchTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	steps := make([]StepState, len(stepNames))
	for i, n := range stepNames {
		steps[i] = StepState{Name: n, Status: StepPending}
	}
	t := &SwitchTask{
		ID:        uuid.NewString(),
		Group:     group,
		Direction: direction,
		Status:    SwitchRunning,
		Steps:     steps,
		StartedAt: time.Now(),
	}
	s.tasks[t.ID] = t
	return cloneTask(t)
}

func (s *switchStore) update(id string, fn func(*SwitchTask)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return
	}
	fn(t)
	if t.Status != SwitchRunning && t.FinishedAt == nil {
		now := time.Now()
		t.FinishedAt = &now
		s.finished = append(s.finished, id)
		for len(s.finished) > s.maxKeep {
			old := s.finished[0]
			s.finished = s.finished[1:]
			delete(s.tasks, old)
		}
	}
}

func (s *switchStore) get(id string) (*SwitchTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	return cloneTask(t), true
}

func cloneTask(t *SwitchTask) *SwitchTask {
	cp := *t
	cp.Steps = append([]StepState(nil), t.Steps...)
	if t.FinishedAt != nil {
		f := *t.FinishedAt
		cp.FinishedAt = &f
	}
	return &cp
}

// setStep updates a named step's status/message on a task.
func (s *switchStore) setStep(id, name, status, msg string) {
	s.update(id, func(t *SwitchTask) {
		for i := range t.Steps {
			if t.Steps[i].Name == name {
				t.Steps[i].Status = status
				if msg != "" {
					t.Steps[i].Message = msg
				}
				return
			}
		}
	})
}

// GetTask returns a switch task by ID.
func (s *Service) GetTask(id string) (*SwitchTask, bool) {
	return s.switches.get(id)
}

// Failover switches primary -> standby. Failback switches standby -> primary.
func (s *Service) Failover(name string) (*SwitchTask, error) { return s.startSwitch(name, DirectionFailover) }
func (s *Service) Failback(name string) (*SwitchTask, error) { return s.startSwitch(name, DirectionFailback) }

func (s *Service) startSwitch(name, direction string) (*SwitchTask, error) {
	g, ok := s.cfg.HAGroupByName(name)
	if !ok {
		return nil, fmt.Errorf("HA group %q not found", name)
	}
	from, to := g.Primary, g.Standby
	if direction == DirectionFailback {
		from, to = g.Standby, g.Primary
	}
	steps := []string{StepFenceNeutral, StepPickRecoveryPoint, StepStopSource, StepTokenToTarget, StepStartTarget, StepVerify}
	task := s.switches.create(g.Name, direction, steps)
	go s.doSwitch(task.ID, g, from, to)
	return task, nil
}

// doSwitch executes the five-step switch (mirrors failover.sh do_switch), with
// the neutral-token + wait-for-stop double safeguard against split-brain.
func (s *Service) doSwitch(taskID string, g config.HAGroupConfig, from, to config.SideConfig) {
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(s.cfg.StopTimeoutSec+s.cfg.SavepointTimeoutSec+120)*time.Second)
	defer cancel()

	fail := func(step string, err error) {
		s.switches.setStep(taskID, step, StepFailed, err.Error())
		s.switches.update(taskID, func(t *SwitchTask) {
			t.Status = SwitchFailed
			t.Error = fmt.Sprintf("%s: %v", step, err)
		})
	}

	fs, err := s.fencingStore(ctx, g)
	if err != nil {
		fail(StepFenceNeutral, fmt.Errorf("fencing store: %w", err))
		return
	}
	fromAcc, err := s.reg.AccessorFor(from.Cluster, from.Namespace)
	if err != nil {
		fail(StepStopSource, fmt.Errorf("source accessor: %w", err))
		return
	}
	toAcc, err := s.reg.AccessorFor(to.Cluster, to.Namespace)
	if err != nil {
		fail(StepStartTarget, fmt.Errorf("target accessor: %w", err))
		return
	}

	// 1. Neutral token — fence both sides for the whole transition.
	s.switches.setStep(taskID, StepFenceNeutral, StepRunning, "writing neutral token")
	if err := fs.WriteToken(ctx, g.FencingKey, g.NeutralToken); err != nil {
		fail(StepFenceNeutral, err)
		return
	}
	s.switches.setStep(taskID, StepFenceNeutral, StepDone, "both sides fenced")

	// 2. Pick recovery point: source healthy -> savepoint (zero loss); else latest checkpoint.
	s.switches.setStep(taskID, StepPickRecoveryPoint, StepRunning, "selecting recovery point")
	path, kind := s.pickRecoveryPoint(ctx, g, from)
	s.switches.update(taskID, func(t *SwitchTask) { t.RecoveryPoint = RecoveryPointRef{Path: path, Kind: kind} })
	s.switches.setStep(taskID, StepPickRecoveryPoint, StepDone, fmt.Sprintf("%s: %s", kind, path))

	// 3. Stop source and wait for its JobManager pod to disappear.
	s.switches.setStep(taskID, StepStopSource, StepRunning, "suspending source")
	if err := patchState(ctx, fromAcc, from.Deployment, statePatchJSON("suspended", "", 0)); err != nil {
		// A disaster failover may have an unreachable source; the neutral token
		// already fences it, so continue rather than abort.
		s.switches.setStep(taskID, StepStopSource, StepRunning, "source unreachable, relying on neutral token")
	}
	s.waitJMStopped(ctx, fromAcc, from.Deployment, func(pods int) {
		s.switches.setStep(taskID, StepStopSource, StepRunning,
			fmt.Sprintf("waiting for source JobManager to terminate (%d running)", pods))
	})
	s.switches.setStep(taskID, StepStopSource, StepDone, "source stopped")

	// 4. Point the token to the target (source now stopped; target exclusive).
	s.switches.setStep(taskID, StepTokenToTarget, StepRunning, "writing token -> "+to.ClusterID)
	if err := fs.WriteToken(ctx, g.FencingKey, to.ClusterID); err != nil {
		fail(StepTokenToTarget, err)
		return
	}
	s.switches.setStep(taskID, StepTokenToTarget, StepDone, "token -> "+to.ClusterID)

	// 5. Start the target from the recovery point (nonce forces redeploy).
	s.switches.setStep(taskID, StepStartTarget, StepRunning, "starting target")
	nonce := time.Now().Unix()
	if err := patchState(ctx, toAcc, to.Deployment, statePatchJSON("running", path, nonce)); err != nil {
		fail(StepStartTarget, err)
		return
	}
	s.switches.setStep(taskID, StepStartTarget, StepDone, "target start requested")

	// 6. Verify (best-effort): poll the target briefly for RUNNING/STABLE.
	s.switches.setStep(taskID, StepVerify, StepRunning, "waiting for target to become RUNNING/STABLE")
	stable := s.verifyTarget(ctx, toAcc, to.Deployment, 60*time.Second)
	if stable {
		s.switches.setStep(taskID, StepVerify, StepDone, "target RUNNING/STABLE")
	} else {
		s.switches.setStep(taskID, StepVerify, StepDone, "target starting (not yet STABLE; check the dashboard)")
	}

	s.switches.update(taskID, func(t *SwitchTask) { t.Status = SwitchSucceeded })
}

// pickRecoveryPoint returns (path, kind): source healthy -> best-effort savepoint;
// otherwise the group's latest checkpoint; else ("", "none").
func (s *Service) pickRecoveryPoint(ctx context.Context, g config.HAGroupConfig, from config.SideConfig) (string, string) {
	// Savepoint if the source is healthy.
	if acc, err := s.reg.AccessorFor(from.Cluster, from.Namespace); err == nil {
		fsvc := flink.NewService(acc, s.cfg)
		if d, err := fsvc.Get(ctx, from.Deployment); err == nil && d.Healthy {
			if res, err := fsvc.Savepoint(ctx, from.Deployment); err == nil && res.Location != "" {
				return res.Location, "savepoint"
			}
		}
	}
	// Fall back to latest checkpoint from S3 (newest first), else any savepoint.
	if st, err := s.recovStore(ctx, g); err == nil {
		spDir, cpDir, job := s.recoveryDirs(ctx, from)
		if points, err := st.ListRecoveryPoints(ctx, job, spDir, cpDir); err == nil {
			for _, p := range points {
				if p.Type == "checkpoint" {
					return p.Path, "checkpoint"
				}
			}
			for _, p := range points {
				if p.Type == "savepoint" {
					return p.Path, "savepoint"
				}
			}
		}
	}
	return "", "none"
}

// waitJMStopped polls until the deployment's JobManager pod count reaches zero.
func (s *Service) waitJMStopped(ctx context.Context, acc cluster.ClusterAccessor, dep string, onTick func(pods int)) {
	selector := fmt.Sprintf("app=%s,component=jobmanager", dep)
	deadline := time.Now().Add(time.Duration(s.cfg.StopTimeoutSec) * time.Second)
	for time.Now().Before(deadline) {
		n, err := acc.CountPods(ctx, selector)
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

// verifyTarget polls the target's status until RUNNING/STABLE or timeout.
func (s *Service) verifyTarget(ctx context.Context, acc cluster.ClusterAccessor, dep string, timeout time.Duration) bool {
	fsvc := flink.NewService(acc, s.cfg)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fsvc.StatusText(ctx, dep) == "RUNNING/STABLE" {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(5 * time.Second):
		}
	}
	return false
}

// patchState applies a spec.job merge patch to a deployment via the accessor.
func patchState(ctx context.Context, acc cluster.ClusterAccessor, dep string, patch []byte) error {
	return acc.PatchFlinkDeployment(ctx, dep, patch)
}

// statePatchJSON builds a spec.job merge patch. When savepointPath is set it
// adds initialSavepointPath + savepointRedeployNonce to force a redeploy.
func statePatchJSON(state, savepointPath string, nonce int64) []byte {
	job := map[string]any{"state": state}
	if savepointPath != "" {
		job["initialSavepointPath"] = savepointPath
		job["savepointRedeployNonce"] = nonce
	}
	b, _ := json.Marshal(map[string]any{"spec": map[string]any{"job": job}})
	return b
}
