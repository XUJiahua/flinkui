package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/google/uuid"
)

// taskStore is an in-memory registry of HA tasks (bounded retention).
type taskStore struct {
	mu       sync.RWMutex
	tasks    map[string]*HATask
	finished []string
	maxKeep  int
}

func newTaskStore() *taskStore { return &taskStore{tasks: map[string]*HATask{}, maxKeep: 100} }

func (s *taskStore) create(group, op string, stepNames []string) *HATask {
	s.mu.Lock()
	defer s.mu.Unlock()
	steps := make([]StepState, len(stepNames))
	for i, n := range stepNames {
		steps[i] = StepState{Name: n, Status: StepPending}
	}
	t := &HATask{ID: uuid.NewString(), Group: group, Op: op, Status: TaskRunning, Steps: steps, StartedAt: time.Now()}
	s.tasks[t.ID] = t
	return cloneTask(t)
}

func (s *taskStore) update(id string, fn func(*HATask)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return
	}
	fn(t)
	if t.Status != TaskRunning && t.FinishedAt == nil {
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

func (s *taskStore) get(id string) (*HATask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	return cloneTask(t), true
}

func (s *taskStore) setStep(id, name, status, msg string) {
	s.update(id, func(t *HATask) {
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

func cloneTask(t *HATask) *HATask {
	cp := *t
	cp.Steps = append([]StepState(nil), t.Steps...)
	if t.FinishedAt != nil {
		f := *t.FinishedAt
		cp.FinishedAt = &f
	}
	return &cp
}

// GetTask returns an HA task by ID.
func (s *Service) GetTask(id string) (*HATask, bool) { return s.tasks.get(id) }

// Claim idempotently marks THIS side active in the shared fencing token and
// handoff record WITHOUT restarting the local job. It is the cold-start
// bootstrap: after a fresh deploy the token is unset while the primary already
// runs; Claim establishes the baseline (token=self, handoff stable) so the
// fencing/observation is consistent. It does not force a redeploy (unlike
// Promote), so it is safe to run against an already-running active side.
func (s *Service) Claim(name string) error {
	g, ok := s.cfg.HAGroupByName(name)
	if !ok {
		return fmt.Errorf("HA group %q not found", name)
	}
	if s.coord == nil {
		return fmt.Errorf("S3 coordination not configured; cannot Claim")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.coord.WriteToken(ctx, g.FencingKey, g.ClusterID); err != nil {
		return err
	}
	// Preserve an existing epoch if higher; otherwise start at 1.
	epoch := int64(1)
	if rec, ok, _ := s.coord.ReadHandoff(ctx, g.HandoffKey); ok && rec != nil && rec.Epoch > epoch {
		epoch = rec.Epoch
	}
	return s.coord.WriteHandoff(ctx, g.HandoffKey, &store.HandoffRecord{
		Group:           g.Name,
		ActiveClusterID: g.ClusterID,
		Epoch:           epoch,
		Phase:           store.PhaseStable,
	})
}

func (s *Service) failTask(id, step string, err error) {
	s.tasks.setStep(id, step, StepFailed, err.Error())
	s.tasks.update(id, func(t *HATask) {
		t.Status = TaskFailed
		t.Error = fmt.Sprintf("%s: %v", step, err)
	})
}

// Release performs the source-side, LOCAL-only step-down: savepoint (if healthy)
// -> suspend -> wait local JM pod=0 -> neutral token -> write handoff(released).
func (s *Service) Release(name string) (*HATask, error) {
	g, ok := s.cfg.HAGroupByName(name)
	if !ok {
		return nil, fmt.Errorf("HA group %q not found", name)
	}
	if s.coord == nil {
		return nil, fmt.Errorf("S3 coordination not configured; cannot Release")
	}
	steps := []string{StepSavepoint, StepSuspendLocal, StepWaitStopped, StepTokenNeutral, StepWriteHandoff}
	task := s.tasks.create(g.Name, OpRelease, steps)
	go s.doRelease(task.ID, g)
	return task, nil
}

func (s *Service) doRelease(taskID string, g config.LocalHAGroup) {
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(s.cfg.StopTimeoutSec+s.cfg.SavepointTimeoutSec+60)*time.Second)
	defer cancel()

	acc, err := s.localAccessor(g.Namespace)
	if err != nil {
		s.failTask(taskID, StepSuspendLocal, fmt.Errorf("local accessor: %w", err))
		return
	}
	fsvc := flink.NewService(acc, s.cfg)

	// 1. Savepoint if the local job is healthy.
	var rp store.RecoveryPointRef
	s.tasks.setStep(taskID, StepSavepoint, StepRunning, "checking local health")
	if d, err := fsvc.Get(ctx, g.Deployment); err == nil && d.Healthy {
		if res, err := fsvc.Savepoint(ctx, g.Deployment); err == nil && res.Location != "" {
			rp = store.RecoveryPointRef{Path: res.Location, Kind: "savepoint"}
			s.tasks.setStep(taskID, StepSavepoint, StepDone, res.Location)
		} else {
			s.tasks.setStep(taskID, StepSavepoint, StepDone, "savepoint unavailable; peer will fall back to latest checkpoint")
		}
	} else {
		s.tasks.setStep(taskID, StepSavepoint, StepDone, "local not RUNNING/STABLE; skipped")
	}
	s.tasks.update(taskID, func(t *HATask) { t.RecoveryPoint = rp })

	// 2. Suspend local.
	s.tasks.setStep(taskID, StepSuspendLocal, StepRunning, "suspending local job")
	if err := acc.PatchFlinkDeployment(ctx, g.Deployment, statePatchJSON("suspended", "", 0)); err != nil {
		s.failTask(taskID, StepSuspendLocal, err)
		return
	}
	s.tasks.setStep(taskID, StepSuspendLocal, StepDone, "")

	// 3. Wait for the local JobManager pod to terminate.
	s.tasks.setStep(taskID, StepWaitStopped, StepRunning, "waiting for local JobManager to terminate")
	s.waitJMStopped(ctx, acc, g.Deployment, func(pods int) {
		s.tasks.setStep(taskID, StepWaitStopped, StepRunning, fmt.Sprintf("waiting for local JobManager (%d running)", pods))
	})
	s.tasks.setStep(taskID, StepWaitStopped, StepDone, "local stopped")

	// 4. Neutral token — fence both sides.
	s.tasks.setStep(taskID, StepTokenNeutral, StepRunning, "writing neutral token")
	if err := s.coord.WriteToken(ctx, g.FencingKey, g.NeutralToken); err != nil {
		s.failTask(taskID, StepTokenNeutral, err)
		return
	}
	s.tasks.setStep(taskID, StepTokenNeutral, StepDone, "both sides fenced")

	// 5. Write handoff (phase=released) so the peer can take over.
	s.tasks.setStep(taskID, StepWriteHandoff, StepRunning, "publishing handoff record")
	if err := s.writeReleasedHandoff(ctx, g, rp); err != nil {
		s.failTask(taskID, StepWriteHandoff, err)
		return
	}
	s.tasks.setStep(taskID, StepWriteHandoff, StepDone, "released; peer may now Promote")
	s.tasks.update(taskID, func(t *HATask) { t.Status = TaskSucceeded })
}

// writeReleasedHandoff sets phase=released via a compare-and-swap loop so a
// concurrent handoff update (e.g. the peer writing) cannot be silently clobbered
// nor clobber us; on CAS conflict it re-reads and retries a bounded number of
// times.
func (s *Service) writeReleasedHandoff(ctx context.Context, g config.LocalHAGroup, rp store.RecoveryPointRef) error {
	const attempts = 3
	for i := 0; i < attempts; i++ {
		rec, etag, _, err := s.coord.ReadHandoffWithETag(ctx, g.HandoffKey)
		if err != nil {
			return err
		}
		if rec == nil {
			rec = &store.HandoffRecord{Group: g.Name}
		}
		rec.Phase = store.PhaseReleased
		rec.ReleasedBy = g.ClusterID
		rec.RecoveryPoint = rp
		err = s.coord.WriteHandoffCAS(ctx, g.HandoffKey, rec, etag)
		if err == nil {
			return nil
		}
		if errors.Is(err, store.ErrHandoffConflict) {
			continue // re-read and retry
		}
		return err
	}
	return fmt.Errorf("handoff record kept changing during release (CAS retries exhausted)")
}

// Promote performs the target-side, LOCAL-only take-over: pick recovery point ->
// token->self (epoch+1) -> start local -> verify. force skips the "peer released"
// guard (disaster) and then requires ackDataLoss.
func (s *Service) Promote(name string, force, ackDataLoss bool) (*HATask, error) {
	g, ok := s.cfg.HAGroupByName(name)
	if !ok {
		return nil, fmt.Errorf("HA group %q not found", name)
	}
	if s.coord == nil {
		return nil, fmt.Errorf("S3 coordination not configured; cannot Promote")
	}
	if force && !ackDataLoss {
		return nil, fmt.Errorf("force Promote requires ackDataLoss=true")
	}
	steps := []string{StepReadHandoff, StepPickRecovery, StepTokenToSelf, StepStartLocal, StepVerifyLocal}
	task := s.tasks.create(g.Name, OpPromote, steps)
	go s.doPromote(task.ID, g, force)
	return task, nil
}

func (s *Service) doPromote(taskID string, g config.LocalHAGroup, force bool) {
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(s.cfg.StopTimeoutSec+90)*time.Second)
	defer cancel()

	acc, err := s.localAccessor(g.Namespace)
	if err != nil {
		s.failTask(taskID, StepStartLocal, fmt.Errorf("local accessor: %w", err))
		return
	}

	// 1. Read handoff and gate on it unless forcing.
	s.tasks.setStep(taskID, StepReadHandoff, StepRunning, "reading handoff record")
	rec, handoffETag, _, _ := s.coord.ReadHandoffWithETag(ctx, g.HandoffKey)
	if !force {
		released := rec != nil && rec.Phase == store.PhaseReleased
		if !released {
			s.failTask(taskID, StepReadHandoff,
				fmt.Errorf("peer has not released (handoff phase != released); use force to take over (risks split-brain), which requires acknowledging data loss"))
			return
		}
	}
	s.tasks.setStep(taskID, StepReadHandoff, StepDone, "")

	// 2. Pick recovery point: handoff's savepoint if present, else latest checkpoint.
	s.tasks.setStep(taskID, StepPickRecovery, StepRunning, "selecting recovery point")
	path, kind := s.pickRecoveryPoint(ctx, g, rec)
	s.tasks.update(taskID, func(t *HATask) { t.RecoveryPoint = store.RecoveryPointRef{Path: path, Kind: kind} })
	s.tasks.setStep(taskID, StepPickRecovery, StepDone, fmt.Sprintf("%s: %s", kind, path))

	// 3. Claim the handoff (epoch+1) via CAS, then point the token at self.
	//    The CAS is the atomic winner: if the peer promoted at the same instant
	//    the conditional write fails and we abort instead of overwriting.
	epoch := int64(1)
	if rec != nil {
		epoch = rec.Epoch + 1
	}
	s.tasks.setStep(taskID, StepTokenToSelf, StepRunning, "claiming handoff -> "+g.ClusterID)
	newRec := &store.HandoffRecord{
		Group: g.Name, ActiveClusterID: g.ClusterID, Epoch: epoch,
		Phase: store.PhaseStable, RecoveryPoint: store.RecoveryPointRef{Path: path, Kind: kind},
	}
	if err := s.coord.WriteHandoffCAS(ctx, g.HandoffKey, newRec, handoffETag); err != nil {
		if errors.Is(err, store.ErrHandoffConflict) {
			s.failTask(taskID, StepTokenToSelf,
				fmt.Errorf("the peer changed the handoff concurrently (likely promoted at the same time); aborted to avoid split-brain — re-check state and retry"))
			return
		}
		s.failTask(taskID, StepTokenToSelf, err)
		return
	}
	if err := s.coord.WriteToken(ctx, g.FencingKey, g.ClusterID); err != nil {
		s.failTask(taskID, StepTokenToSelf, err)
		return
	}
	s.tasks.update(taskID, func(t *HATask) { t.Epoch = epoch })
	s.tasks.setStep(taskID, StepTokenToSelf, StepDone, fmt.Sprintf("token=%s epoch=%d", g.ClusterID, epoch))

	// 4. Start local from the recovery point (nonce forces redeploy).
	s.tasks.setStep(taskID, StepStartLocal, StepRunning, "starting local job")
	nonce := flink.NextRedeployNonce()
	if err := acc.PatchFlinkDeployment(ctx, g.Deployment, statePatchJSON("running", path, nonce)); err != nil {
		s.failTask(taskID, StepStartLocal, err)
		return
	}
	s.tasks.setStep(taskID, StepStartLocal, StepDone, "start requested")

	// 5. Verify local becomes RUNNING/STABLE (best-effort).
	s.tasks.setStep(taskID, StepVerifyLocal, StepRunning, "waiting for local RUNNING/STABLE")
	if s.verifyLocal(ctx, acc, g.Deployment, 90*time.Second) {
		s.tasks.setStep(taskID, StepVerifyLocal, StepDone, "local RUNNING/STABLE")
	} else {
		s.tasks.setStep(taskID, StepVerifyLocal, StepDone, "local starting (not yet STABLE; check the dashboard)")
	}
	s.tasks.update(taskID, func(t *HATask) { t.Status = TaskSucceeded })
}

// pickRecoveryPoint prefers the handoff's recovery point; else the latest
// checkpoint from shared S3; else any savepoint; else none.
func (s *Service) pickRecoveryPoint(ctx context.Context, g config.LocalHAGroup, rec *store.HandoffRecord) (string, string) {
	if rec != nil && rec.RecoveryPoint.Path != "" {
		return rec.RecoveryPoint.Path, rec.RecoveryPoint.Kind
	}
	if s.recov != nil {
		sp, cp, job := s.recoveryDirs(ctx, g)
		if points, err := s.recov.ListRecoveryPoints(ctx, job, sp, cp); err == nil {
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

// recoveryDirs reads the local deployment's configured savepoint/checkpoint dirs.
func (s *Service) recoveryDirs(ctx context.Context, g config.LocalHAGroup) (sp, cp, job string) {
	job = s.cfg.JobName(g.Deployment)
	acc, err := s.localAccessor(g.Namespace)
	if err != nil {
		return "", "", job
	}
	sp, cp, err = flink.NewService(acc, s.cfg).RecoveryDirs(ctx, g.Deployment)
	if err != nil {
		return "", "", job
	}
	return sp, cp, job
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

// verifyLocal polls the local deployment until RUNNING/STABLE or timeout.
func (s *Service) verifyLocal(ctx context.Context, acc cluster.ClusterAccessor, dep string, timeout time.Duration) bool {
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

// statePatchJSON builds a spec.job merge patch; savepointPath adds
// initialSavepointPath + savepointRedeployNonce to force a redeploy.
func statePatchJSON(state, savepointPath string, nonce int64) []byte {
	job := map[string]any{"state": state}
	if savepointPath != "" {
		job["initialSavepointPath"] = savepointPath
		job["savepointRedeployNonce"] = nonce
	}
	b, _ := json.Marshal(map[string]any{"spec": map[string]any{"job": job}})
	return b
}
