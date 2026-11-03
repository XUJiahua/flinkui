package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	if s.coord == nil {
		return fmt.Errorf("S3 coordination not configured; cannot Claim")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	g, ok := s.resolveGroup(ctx, name)
	if !ok {
		return fmt.Errorf("HA group %q not found", name)
	}

	rec, etag, exists, err := s.coord.ReadHandoffWithETag(ctx, g.HandoffKey)
	if err != nil {
		return err
	}
	if exists {
		if rec.Group == g.Name && rec.ActiveClusterID == g.ClusterID && rec.Phase == store.PhaseStable {
			token, err := s.coord.ReadToken(ctx, g.FencingKey)
			if err != nil {
				return err
			}
			if token != g.ClusterID {
				return fmt.Errorf("Claim refused: stable handoff and token disagree")
			}
			return nil
		}
		return fmt.Errorf("Claim requires an uninitialized group")
	}
	token, err := s.coord.ReadToken(ctx, g.FencingKey)
	if err != nil {
		return err
	}
	if token != "" && token != g.ClusterID {
		return fmt.Errorf("Claim refused: token already points elsewhere")
	}
	rec = &store.HandoffRecord{
		Group: g.Name, ActiveClusterID: g.ClusterID, Epoch: 1, Phase: store.PhasePromoting,
	}
	if err := s.coord.WriteHandoffCAS(ctx, g.HandoffKey, rec, etag); err != nil {
		return err
	}
	etag, err = s.reservedHandoffETag(ctx, g, rec)
	if err != nil {
		return err
	}
	if err := s.coord.WriteToken(ctx, g.FencingKey, g.ClusterID); err != nil {
		return err
	}
	rec.Phase = store.PhaseStable
	return s.coord.WriteHandoffCAS(ctx, g.HandoffKey, rec, etag)
}

func (s *Service) failTask(id, step string, err error) {
	s.tasks.setStep(id, step, StepFailed, err.Error())
	s.tasks.update(id, func(t *HATask) {
		t.Status = TaskFailed
		t.Error = fmt.Sprintf("%s: %v", step, err)
	})
}

// Release performs the source-side, LOCAL-only step-down: savepoint (if healthy)
// -> suspend -> wait for local Flink pods to exit -> neutral token -> write handoff(released).
func (s *Service) Release(name string) (*HATask, error) {
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	g, ok := s.resolveGroup(rctx, name)
	rcancel()
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

	rec, etag, _, err := s.coord.ReadHandoffWithETag(ctx, g.HandoffKey)
	if err == nil && (rec == nil || rec.Group != g.Name || rec.ActiveClusterID != g.ClusterID || rec.Phase != store.PhaseStable) {
		err = fmt.Errorf("Release requires a stable handoff owned by this cluster")
	}
	if err == nil {
		rec.Phase = store.PhaseReleasing
		err = s.coord.WriteHandoffCAS(ctx, g.HandoffKey, rec, etag)
	}
	if err == nil {
		etag, err = s.reservedHandoffETag(ctx, g, rec)
	}
	if err != nil {
		s.failTask(taskID, StepSavepoint, err)
		return
	}

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

	// 3. Wait for local Flink pods to terminate.
	s.tasks.setStep(taskID, StepWaitStopped, StepRunning, "waiting for local Flink pods to terminate")
	if err := s.waitLocalStopped(ctx, acc, g.Deployment, func(pods int) {
		s.tasks.setStep(taskID, StepWaitStopped, StepRunning, fmt.Sprintf("waiting for local Flink pods (%d remaining)", pods))
	}); err != nil {
		s.failTask(taskID, StepWaitStopped, err)
		return
	}
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
	rec.Phase = store.PhaseReleased
	rec.ReleasedBy = g.ClusterID
	rec.RecoveryPoint = rp
	if err := s.coord.WriteHandoffCAS(ctx, g.HandoffKey, rec, etag); err != nil {
		s.failTask(taskID, StepWriteHandoff, err)
		return
	}
	s.tasks.setStep(taskID, StepWriteHandoff, StepDone, "released; peer may now Promote")
	s.tasks.update(taskID, func(t *HATask) { t.Status = TaskSucceeded })
}

// reservedHandoffETag confirms that the reservation is visible before changing
// the legacy token or issuing a Kubernetes operation.
func (s *Service) reservedHandoffETag(ctx context.Context, g config.LocalHAGroup, expected *store.HandoffRecord) (string, error) {
	rec, etag, _, err := s.coord.ReadHandoffWithETag(ctx, g.HandoffKey)
	if err != nil {
		return "", err
	}
	if rec == nil || rec.Group != expected.Group || rec.ActiveClusterID != expected.ActiveClusterID ||
		rec.Epoch != expected.Epoch || rec.Phase != expected.Phase || !rec.UpdatedAt.Equal(expected.UpdatedAt) {
		return "", store.ErrHandoffConflict
	}
	return etag, nil
}

// Promote performs the target-side, LOCAL-only take-over: pick recovery point ->
// token->self (epoch+1) -> start local -> verify. force skips the "peer released"
// guard (disaster) and then requires ackDataLoss.
func (s *Service) Promote(name string, force, ackDataLoss bool) (*HATask, error) {
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	g, ok := s.resolveGroup(rctx, name)
	rcancel()
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
	rec, handoffETag, _, err := s.coord.ReadHandoffWithETag(ctx, g.HandoffKey)
	if err == nil {
		err = validatePromotion(g, rec, force)
	}
	if err != nil {
		s.failTask(taskID, StepReadHandoff, err)
		return
	}
	s.tasks.setStep(taskID, StepReadHandoff, StepDone, "")

	// 2. Pick recovery point: handoff's savepoint if present, else latest checkpoint.
	s.tasks.setStep(taskID, StepPickRecovery, StepRunning, "selecting recovery point")
	path, kind := s.pickRecoveryPoint(ctx, g, rec)
	if path == "" {
		s.failTask(taskID, StepPickRecovery, fmt.Errorf("no recovery point available"))
		return
	}
	s.tasks.update(taskID, func(t *HATask) { t.RecoveryPoint = store.RecoveryPointRef{Path: path, Kind: kind} })
	s.tasks.setStep(taskID, StepPickRecovery, StepDone, fmt.Sprintf("%s: %s", kind, path))

	previous, err := acc.GetFlinkDeployment(ctx, g.Deployment)
	if err != nil {
		s.failTask(taskID, StepStartLocal, err)
		return
	}
	previousJobID, _, _ := unstructured.NestedString(previous.Object, "status", "jobStatus", "jobId")

	// 3. Claim the handoff (epoch+1) via CAS, then point the token at self.
	//    The CAS is the atomic winner: if the peer promoted at the same instant
	//    the conditional write fails and we abort instead of overwriting.
	epoch := rec.Epoch + 1
	s.tasks.setStep(taskID, StepTokenToSelf, StepRunning, "claiming handoff -> "+g.ClusterID)
	newRec := &store.HandoffRecord{
		Group: g.Name, ActiveClusterID: g.ClusterID, Epoch: epoch,
		Phase: store.PhasePromoting, RecoveryPoint: store.RecoveryPointRef{Path: path, Kind: kind},
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
	reservationETag, err := s.reservedHandoffETag(ctx, g, newRec)
	if err != nil {
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

	// 5. Publish stable only after observing local readiness.
	s.tasks.setStep(taskID, StepVerifyLocal, StepRunning, "waiting for local RUNNING/STABLE")
	if !s.verifyLocal(ctx, acc, g.Deployment, previousJobID, 90*time.Second) {
		s.failTask(taskID, StepVerifyLocal, fmt.Errorf("local readiness could not be confirmed"))
		return
	}
	newRec.Phase = store.PhaseStable
	if err := s.coord.WriteHandoffCAS(ctx, g.HandoffKey, newRec, reservationETag); err != nil {
		s.failTask(taskID, StepVerifyLocal, err)
		return
	}
	s.tasks.setStep(taskID, StepVerifyLocal, StepDone, "local RUNNING/STABLE")
	s.tasks.update(taskID, func(t *HATask) { t.Status = TaskSucceeded })
}

func validatePromotion(g config.LocalHAGroup, rec *store.HandoffRecord, force bool) error {
	if rec != nil && rec.Group == g.Name && rec.Epoch > 0 && rec.Epoch < math.MaxInt64 {
		if rec.Phase == store.PhaseReleased ||
			(force && rec.Phase == store.PhaseStable && rec.ActiveClusterID == g.PeerClusterID) {
			return nil
		}
	}
	return fmt.Errorf("Promote requires a released handoff or a stable peer handoff for an acknowledged forced promotion")
}

// pickRecoveryPoint prefers the handoff's recovery point; else the latest
// checkpoint from shared S3; else any savepoint; else none.
func (s *Service) pickRecoveryPoint(ctx context.Context, g config.LocalHAGroup, rec *store.HandoffRecord) (string, string) {
	if rec != nil && rec.Phase == store.PhaseReleased && rec.RecoveryPoint.Path != "" {
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

// waitLocalStopped requires a successful observation that both pod types are gone.
func (s *Service) waitLocalStopped(ctx context.Context, acc cluster.ClusterAccessor, dep string, onTick func(pods int)) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.StopTimeoutSec)*time.Second)
	defer cancel()
	selector := fmt.Sprintf("app=%s,component in (jobmanager,taskmanager)", dep)
	for ctx.Err() == nil {
		n, err := acc.CountPods(ctx, selector)
		if err == nil {
			onTick(n)
			if n == 0 && ctx.Err() == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("local pod termination could not be confirmed: %w", ctx.Err())
}

// verifyLocal requires a fresh job ID so stale pre-patch status cannot complete promotion.
func (s *Service) verifyLocal(ctx context.Context, acc cluster.ClusterAccessor, dep, previousJobID string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for ctx.Err() == nil {
		deployment, err := acc.GetFlinkDeployment(ctx, dep)
		if err == nil && promotedJobReady(deployment, previousJobID) && ctx.Err() == nil {
			return true
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
	return false
}

func promotedJobReady(deployment *unstructured.Unstructured, previousJobID string) bool {
	jobID, _, _ := unstructured.NestedString(deployment.Object, "status", "jobStatus", "jobId")
	state, _, _ := unstructured.NestedString(deployment.Object, "status", "jobStatus", "state")
	lifecycle, _, _ := unstructured.NestedString(deployment.Object, "status", "lifecycleState")
	return jobID != "" && jobID != previousJobID && state == "RUNNING" && lifecycle == "STABLE"
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
