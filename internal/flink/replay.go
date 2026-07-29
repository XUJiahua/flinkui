package flink

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// This file implements the "replay from timestamp" orchestration described in
// docs/replay-from-timestamp-design.md. It performs OUT-OF-BAND writes (the SQL
// ConfigMap + the FlinkDeployment spec) and is only wired when the feature is
// explicitly enabled (config.Replay.Enabled). All mutations run under the
// per-deployment lock, reusing the existing suspend/wait/resume restart.

const (
	annReplayActive   = "flinkui.io/replay-active"
	annReplayFrom     = "flinkui.io/replay-from-millis"
	annReplayStartAt  = "flinkui.io/replay-started-at"
	annReplayStartBy  = "flinkui.io/replay-started-by"
	annReplayPrevMode = "flinkui.io/replay-prev-upgrade-mode"
	annReplayTable    = "flinkui.io/replay-source-table"

	backupKeySuffix = ".flinkui-prereplay-backup"
	sqlChecksumKey  = "flinkui.sql-checksum"
)

// validUpgradeModes bounds SetUpgradeMode input.
var validUpgradeModes = map[string]bool{"stateless": true, "last-state": true, "savepoint": true}

// JobSQL is the SQL ConfigMap view returned to the UI.
type JobSQL struct {
	ConfigMap    string   `json:"configMap"`
	FileKey      string   `json:"fileKey"`
	SQL          string   `json:"sql"`
	KafkaSources []string `json:"kafkaSources"`
}

// PlanStep describes one step the console will perform, with the equivalent
// kubectl command for transparency / manual recovery. The console executes these
// via the Kubernetes API (client-go), not by shelling out to kubectl; the
// command is an equivalent for reference.
type PlanStep struct {
	Title   string `json:"title"`
	Detail  string `json:"detail,omitempty"`
	Kubectl string `json:"kubectl,omitempty"`
}

// ReplayPlan is the preview of a replay edit (no writes performed).
type ReplayPlan struct {
	Table       string     `json:"table"`
	FromMillis  int64      `json:"fromMillis"`
	UpgradeMode string     `json:"upgradeMode"` // will be set to "stateless"
	OriginalSQL string     `json:"originalSql"`
	NewSQL      string     `json:"newSql"`
	Steps       []PlanStep `json:"steps"`
}

// ReplaySession reports the current replay state read from CR annotations plus a
// log-based verdict.
type ReplaySession struct {
	Active          bool   `json:"active"`
	FromMillis      int64  `json:"fromMillis,omitempty"`
	StartedAt       string `json:"startedAt,omitempty"`
	StartedBy       string `json:"startedBy,omitempty"`
	SourceTable     string `json:"sourceTable,omitempty"`
	PrevUpgradeMode string `json:"prevUpgradeMode,omitempty"`
	UpgradeMode     string `json:"upgradeMode,omitempty"`
	Verdict         string `json:"verdict,omitempty"` // ok | still-last-state | offset-boundary | pending
	VerdictDetail   string `json:"verdictDetail,omitempty"`
	// ConfigMap is the resolved SQL ConfigMap name (for the kubectl equivalents).
	ConfigMap string `json:"configMap,omitempty"`
	// RevertSteps is the plan the console will run on revert, with equivalent
	// kubectl commands (the savepoint path is only known at run time, shown as
	// <savepoint-path>).
	RevertSteps []PlanStep `json:"revertSteps,omitempty"`
}

// cmAccessor returns the ConfigMap accessor if the underlying accessor supports
// it (KubeAccessor does).
func (s *Service) cmAccessor() (cluster.ConfigMapAccessor, bool) {
	ca, ok := s.acc.(cluster.ConfigMapAccessor)
	return ca, ok
}

// resolveSQLConfigMap reads the SQL ConfigMap name from the CR's SQL volume
// (naming-convention independent: the CR name prefix differs from the CM name).
func (s *Service) resolveSQLConfigMap(ctx context.Context, dep string) (string, error) {
	cr, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return "", err
	}
	volumes, _, _ := unstructured.NestedSlice(cr.Object, "spec", "podTemplate", "spec", "volumes")
	for _, v := range volumes {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := vm["name"].(string); name == s.cfg.Replay.SQLVolumeName {
			cmName, _, _ := unstructured.NestedString(vm, "configMap", "name")
			if cmName == "" {
				return "", fmt.Errorf("SQL volume %q has no configMap.name", s.cfg.Replay.SQLVolumeName)
			}
			return cmName, nil
		}
	}
	return "", fmt.Errorf("SQL volume %q not found on deployment %s", s.cfg.Replay.SQLVolumeName, dep)
}

// GetJobSQL returns the job's SQL and the detected Kafka source tables.
func (s *Service) GetJobSQL(ctx context.Context, name string) (*JobSQL, error) {
	ca, ok := s.cmAccessor()
	if !ok {
		return nil, fmt.Errorf("cluster accessor does not support ConfigMap read")
	}
	dep := s.cfg.DeploymentName(name)
	cmName, err := s.resolveSQLConfigMap(ctx, dep)
	if err != nil {
		return nil, err
	}
	data, found, err := ca.GetConfigMap(ctx, cmName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("SQL ConfigMap %q not found", cmName)
	}
	sql := data[s.cfg.Replay.SQLFileKey]
	var sources []string
	for _, b := range findWithBlocks(sql) {
		if strings.HasPrefix(strings.ToLower(b.connector), "kafka") {
			sources = append(sources, b.table)
		}
	}
	return &JobSQL{ConfigMap: cmName, FileKey: s.cfg.Replay.SQLFileKey, SQL: sql, KafkaSources: sources}, nil
}

// ReplayPreview computes the rewritten SQL without writing anything.
func (s *Service) ReplayPreview(ctx context.Context, name, sourceTable string, fromMillis int64, autoOffsetReset string) (*ReplayPlan, error) {
	cur, err := s.GetJobSQL(ctx, name)
	if err != nil {
		return nil, err
	}
	newSQL, table, err := rewriteForReplay(cur.SQL, sourceTable, fromMillis, autoOffsetReset)
	if err != nil {
		return nil, err
	}
	return &ReplayPlan{
		Table:       table,
		FromMillis:  fromMillis,
		UpgradeMode: "stateless",
		OriginalSQL: cur.SQL,
		NewSQL:      newSQL,
		Steps:       s.replaySteps(s.cfg.DeploymentName(name), cur.ConfigMap),
	}, nil
}

// replaySteps returns the kubectl-equivalent plan for a forward replay. The
// console executes these via the Kubernetes API; the commands are for reference.
func (s *Service) replaySteps(dep, cmName string) []PlanStep {
	ns := s.cfg.Cluster.Namespace
	key := s.cfg.Replay.SQLFileKey
	return []PlanStep{
		{
			Title:   "Back up current SQL",
			Detail:  "Copy the current job SQL to a backup key so revert can restore it verbatim.",
			Kubectl: fmt.Sprintf(`# console copies data[%q] -> data[%q] on configmap/%s`, key, key+backupKeySuffix, cmName),
		},
		{
			Title:   "Rewrite the Kafka source SQL",
			Detail:  "Set scan.startup.mode=timestamp (+ timestamp-millis, auto.offset.reset) in the target table.",
			Kubectl: fmt.Sprintf(`kubectl -n %s patch configmap %s --type merge -p '{"data":{%q:"<rewritten job.sql>"}}'`, ns, cmName, key),
		},
		{
			Title:   "Force stateless + mark spec dirty",
			Detail:  "Set upgradeMode=stateless and inject flinkui.sql-checksum so the operator reconciles.",
			Kubectl: fmt.Sprintf(`kubectl -n %s patch flinkdeployment %s --type merge -p '{"spec":{"job":{"upgradeMode":"stateless"},"flinkConfiguration":{%q:"<md5>"}}}'`, ns, dep, sqlChecksumKey),
		},
		{
			Title:   "Restart to redeploy stateless",
			Detail:  "Suspend, wait for the JobManager pod to terminate, then resume (stateless start from the timestamp).",
			Kubectl: fmt.Sprintf(`kubectl -n %s patch flinkdeployment %s --type merge -p '{"spec":{"job":{"state":"suspended"}}}'   # then, after JM=0: state=running`, ns, dep),
		},
	}
}

// revertSteps returns the kubectl-equivalent plan for a revert. The savepoint
// path is only known at run time and is shown as <savepoint-path>.
func (s *Service) revertSteps(dep, cmName, prevMode string) []PlanStep {
	ns := s.cfg.Cluster.Namespace
	key := s.cfg.Replay.SQLFileKey
	return []PlanStep{
		{
			Title:  "Preflight",
			Detail: "Require the job to be RUNNING/STABLE so a savepoint can be taken.",
		},
		{
			Title:   "Take a savepoint",
			Detail:  "Trigger a savepoint via the JobManager REST API and wait for it to complete.",
			Kubectl: `# console POSTs /jobs/<jobId>/savepoints via pod exec; result path => <savepoint-path>`,
		},
		{
			Title:   "Restore the original SQL",
			Detail:  "Write the backed-up job SQL back and refresh flinkui.sql-checksum.",
			Kubectl: fmt.Sprintf(`kubectl -n %s patch configmap %s --type merge -p '{"data":{%q:"<original job.sql>"}}'`, ns, cmName, key),
		},
		{
			Title:   "Redeploy from the savepoint",
			Detail:  fmt.Sprintf("Restore state from the savepoint (deterministic, no HA-metadata dependency) and return to upgradeMode=%s.", prevMode),
			Kubectl: fmt.Sprintf(`kubectl -n %s patch flinkdeployment %s --type merge -p '{"spec":{"job":{"state":"running","upgradeMode":%q,"initialSavepointPath":"<savepoint-path>","savepointRedeployNonce":<n>}}}'`, ns, dep, prevMode),
		},
		{
			Title:  "Clear the replay session",
			Detail: "Remove the SQL backup key and the replay annotations.",
		},
	}
}

// SetUpgradeMode patches spec.job.upgradeMode after validating the mode.
func (s *Service) SetUpgradeMode(ctx context.Context, name, mode string) error {
	if !validUpgradeModes[mode] {
		return fmt.Errorf("invalid upgradeMode %q (want stateless|last-state|savepoint)", mode)
	}
	dep := s.cfg.DeploymentName(name)
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()
	return s.acc.PatchFlinkDeployment(ctx, dep, upgradeModePatch(mode))
}

// ReplaySessionInfo returns the current replay state (annotations + log verdict).
func (s *Service) ReplaySessionInfo(ctx context.Context, name string) (*ReplaySession, error) {
	dep := s.cfg.DeploymentName(name)
	cr, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return nil, err
	}
	ann := cr.GetAnnotations()
	sess := &ReplaySession{Active: ann[annReplayActive] == "true"}
	if v := ann[annReplayFrom]; v != "" {
		sess.FromMillis, _ = strconv.ParseInt(v, 10, 64)
	}
	sess.StartedAt = ann[annReplayStartAt]
	sess.StartedBy = ann[annReplayStartBy]
	sess.SourceTable = ann[annReplayTable]
	sess.PrevUpgradeMode = ann[annReplayPrevMode]
	sess.UpgradeMode, _, _ = unstructured.NestedString(cr.Object, "spec", "job", "upgradeMode")
	if sess.Active {
		sess.Verdict, sess.VerdictDetail = s.replayVerdict(ctx, name)
		prev := sess.PrevUpgradeMode
		if prev == "" {
			prev = "last-state"
		}
		if cmName, err := s.resolveSQLConfigMap(ctx, dep); err == nil {
			sess.ConfigMap = cmName
			sess.RevertSteps = s.revertSteps(dep, cmName, prev)
		}
	}
	return sess, nil
}

// replayVerdict inspects recent logs to confirm the stateless timestamp start
// (design §7). Best-effort: returns "pending" when logs are inconclusive.
func (s *Service) replayVerdict(ctx context.Context, name string) (verdict, detail string) {
	jm, _ := s.Logs(ctx, name, "jobmanager", "", 200)
	if strings.Contains(jm, "KUBERNETES_OPERATOR_LAST_STATE") ||
		strings.Contains(jm, "Restoring") && strings.Contains(jm, "from Checkpoint") {
		return "still-last-state", "JobManager is still restoring from a checkpoint (last-state). " +
			"Restart may not have picked up stateless; residual HA metadata may need manual cleanup (see runbook §3)."
	}
	tm, _ := s.Logs(ctx, name, "taskmanager", "", 200)
	if strings.Contains(tm, "Seeking to offset") {
		return "ok", "TaskManager is seeking to timestamp-resolved offsets."
	}
	return "pending", "No decisive Seeking/Restoring log lines yet; check again shortly."
}

// StartReplay orchestrates the replay: backup -> edit SQL -> stateless -> restart.
// It is asynchronous and returns the tracking operation immediately.
func (s *Service) StartReplay(name, sourceTable string, fromMillis int64, autoOffsetReset, startedBy string) *Operation {
	dep := s.cfg.DeploymentName(name)
	op := s.ops.create(OpReplay, dep, s.cfg.JobName(dep))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(s.cfg.StopTimeoutSec+120)*time.Second)
		defer cancel()
		if err := s.runReplay(ctx, op.ID, name, dep, sourceTable, fromMillis, autoOffsetReset, startedBy); err != nil {
			s.failOp(op.ID, err)
		}
	}()
	return op
}

func (s *Service) runReplay(ctx context.Context, opID, name, dep, sourceTable string, fromMillis int64, autoOffsetReset, startedBy string) error {
	ca, ok := s.cmAccessor()
	if !ok {
		return fmt.Errorf("cluster accessor does not support ConfigMap write")
	}
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()

	s.setProgress(opID, "preflight")
	cr, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return err
	}
	if cr.GetAnnotations()[annReplayActive] == "true" {
		return fmt.Errorf("a replay is already active; revert it before starting a new one")
	}
	prevMode, _, _ := unstructured.NestedString(cr.Object, "spec", "job", "upgradeMode")
	if prevMode == "" {
		prevMode = "last-state"
	}
	cmName, err := s.resolveSQLConfigMap(ctx, dep)
	if err != nil {
		return err
	}
	data, found, err := ca.GetConfigMap(ctx, cmName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("SQL ConfigMap %q not found", cmName)
	}
	originalSQL := data[s.cfg.Replay.SQLFileKey]
	newSQL, table, err := rewriteForReplay(originalSQL, sourceTable, fromMillis, autoOffsetReset)
	if err != nil {
		return err
	}

	// Backup (only if not already backed up, so the true original is preserved).
	s.setProgress(opID, "backing up original SQL")
	backupKey := s.cfg.Replay.SQLFileKey + backupKeySuffix
	if _, hasBackup := data[backupKey]; !hasBackup {
		if err := ca.PatchConfigMapKey(ctx, cmName, backupKey, originalSQL); err != nil {
			return fmt.Errorf("backup SQL: %w", err)
		}
	}
	if err := s.acc.PatchFlinkDeployment(ctx, dep, annotationsPatch(map[string]any{
		annReplayActive:   "true",
		annReplayFrom:     strconv.FormatInt(fromMillis, 10),
		annReplayStartAt:  time.Now().UTC().Format(time.RFC3339),
		annReplayStartBy:  startedBy,
		annReplayTable:    table,
		annReplayPrevMode: prevMode,
	})); err != nil {
		return fmt.Errorf("annotate replay session: %w", err)
	}

	// Edit SQL + inject checksum so the operator reconciles on restart.
	s.setProgress(opID, "writing replay SQL")
	if err := ca.PatchConfigMapKey(ctx, cmName, s.cfg.Replay.SQLFileKey, newSQL); err != nil {
		return fmt.Errorf("write SQL: %w", err)
	}
	if err := s.acc.PatchFlinkDeployment(ctx, dep, sqlChecksumPatch(newSQL)); err != nil {
		return fmt.Errorf("inject sql-checksum: %w", err)
	}

	// Force stateless start.
	s.setProgress(opID, "setting upgradeMode=stateless")
	if err := s.acc.PatchFlinkDeployment(ctx, dep, upgradeModePatch("stateless")); err != nil {
		return fmt.Errorf("set stateless: %w", err)
	}

	// Restart so the operator redeploys stateless with the new SQL.
	if err := s.restartInLock(ctx, dep, opID); err != nil {
		return err
	}
	s.succeedOp(opID, table, fmt.Sprintf("replaying %q from %d (stateless); verify via logs (Seeking to offset)", table, fromMillis))
	return nil
}

// StartRevert restores the pre-replay SQL and upgradeMode, then restarts.
func (s *Service) StartRevert(name, startedBy string) *Operation {
	dep := s.cfg.DeploymentName(name)
	op := s.ops.create(OpReplayRevert, dep, s.cfg.JobName(dep))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(s.cfg.StopTimeoutSec+120)*time.Second)
		defer cancel()
		if err := s.runRevert(ctx, op.ID, name, dep); err != nil {
			s.failOp(op.ID, err)
		}
	}()
	return op
}

func (s *Service) runRevert(ctx context.Context, opID, name, dep string) error {
	ca, ok := s.cmAccessor()
	if !ok {
		return fmt.Errorf("cluster accessor does not support ConfigMap write")
	}
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()

	// Preflight: revert restores state from a fresh savepoint, so the job must be
	// healthy enough to take one. If it is not, we refuse rather than attempt a
	// last-state transition that can strand the operator ("JM deployment is
	// missing and HA metadata is not available"). The operator can instead
	// recover from the latest checkpoint via Rollback.
	s.setProgress(opID, "preflight")
	cr, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return err
	}
	if !s.summaryFrom(cr).Healthy {
		return fmt.Errorf("job is not RUNNING/STABLE; cannot take a savepoint to revert safely. " +
			"Wait for it to stabilize and retry, or recover from the latest checkpoint via Rollback " +
			"(the replay session is kept so you can retry).")
	}
	ann := cr.GetAnnotations()
	prevMode := ann[annReplayPrevMode]
	if prevMode == "" {
		prevMode = "last-state"
	}
	cmName, err := s.resolveSQLConfigMap(ctx, dep)
	if err != nil {
		return err
	}
	data, found, err := ca.GetConfigMap(ctx, cmName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("SQL ConfigMap %q not found", cmName)
	}
	backupKey := s.cfg.Replay.SQLFileKey + backupKeySuffix
	backupSQL, hasBackup := data[backupKey]
	if !hasBackup {
		return fmt.Errorf("no pre-replay SQL backup found (key %q); nothing to revert", backupKey)
	}

	// 1. Take a savepoint of the (stateless) replay job to get a deterministic
	//    recovery point. Nothing has been mutated yet, so a failure here is safe
	//    to retry and leaves the replay running.
	s.setProgress(opID, "taking savepoint")
	sp, err := s.savepointInLock(ctx, dep)
	if err != nil {
		return fmt.Errorf("savepoint before revert failed (replay left intact, retry later): %w", err)
	}
	// Surface the recovery point immediately: if a later step fails, the operator
	// has the exact path to redeploy from manually.
	s.ops.update(opID, func(o *Operation) { o.Result = sp.Location })
	s.setProgress(opID, "savepoint complete: "+sp.Location)

	// 2. Restore the original SQL + refresh the checksum.
	s.setProgress(opID, "restoring original SQL")
	if err := ca.PatchConfigMapKey(ctx, cmName, s.cfg.Replay.SQLFileKey, backupSQL); err != nil {
		return fmt.Errorf("restore SQL failed (savepoint=%s kept for manual recovery): %w", sp.Location, err)
	}
	if err := s.acc.PatchFlinkDeployment(ctx, dep, sqlChecksumPatch(backupSQL)); err != nil {
		return fmt.Errorf("update sql-checksum failed (savepoint=%s): %w", sp.Location, err)
	}

	// 3. Redeploy from the savepoint (deterministic; no HA-metadata dependency),
	//    returning to the previous upgradeMode. This mirrors Rollback and avoids
	//    the fragile stateless->last-state suspend/resume that could strand the op.
	s.setProgress(opID, fmt.Sprintf("redeploying from savepoint (upgradeMode=%s)", prevMode))
	if err := s.acc.PatchFlinkDeployment(ctx, dep, redeployFromSavepointPatch(prevMode, sp.Location)); err != nil {
		return fmt.Errorf("redeploy from savepoint %s failed: %w", sp.Location, err)
	}

	// 4. Only after a successful redeploy do we clear the backup + annotations.
	s.setProgress(opID, "clearing replay session")
	_ = ca.DeleteConfigMapKey(ctx, cmName, backupKey)
	_ = s.acc.PatchFlinkDeployment(ctx, dep, annotationsPatch(map[string]any{
		annReplayActive:   nil,
		annReplayFrom:     nil,
		annReplayStartAt:  nil,
		annReplayStartBy:  nil,
		annReplayTable:    nil,
		annReplayPrevMode: nil,
	}))
	s.succeedOp(opID, sp.Location, fmt.Sprintf("reverted to %s; redeployed from savepoint %s", prevMode, sp.Location))
	return nil
}

// restartInLock runs suspend -> wait for JM pod = 0 -> resume, reporting into the
// operation. The caller MUST already hold the per-deployment lock.
func (s *Service) restartInLock(ctx context.Context, dep, opID string) error {
	s.setProgress(opID, "suspending")
	if err := s.acc.PatchFlinkDeployment(ctx, dep, statePatch("suspended")); err != nil {
		return fmt.Errorf("suspend failed: %w", err)
	}
	s.waitStoppedProgress(ctx, dep, func(pods int) {
		if pods == 0 {
			s.setProgress(opID, "JobManager stopped; resuming")
		} else {
			s.setProgress(opID, fmt.Sprintf("waiting for JobManager pod to terminate (%d running)", pods))
		}
	})
	s.setProgress(opID, "resuming")
	if err := s.acc.PatchFlinkDeployment(ctx, dep, statePatch("running")); err != nil {
		return fmt.Errorf("resume failed: %w", err)
	}
	return nil
}

func (s *Service) setProgress(opID, msg string) {
	s.ops.update(opID, func(o *Operation) { o.Progress = msg })
}

// ---- patch builders ----

func upgradeModePatch(mode string) []byte {
	b, _ := json.Marshal(map[string]any{
		"spec": map[string]any{"job": map[string]any{"upgradeMode": mode}},
	})
	return b
}

// redeployFromSavepointPatch forces a redeploy that restores state from path,
// returning the job to the given upgradeMode. The changing savepointRedeployNonce
// is what triggers the operator to redeploy from initialSavepointPath.
func redeployFromSavepointPatch(mode, path string) []byte {
	b, _ := json.Marshal(map[string]any{
		"spec": map[string]any{"job": map[string]any{
			"state":                  "running",
			"upgradeMode":            mode,
			"initialSavepointPath":   path,
			"savepointRedeployNonce": NextRedeployNonce(),
		}},
	})
	return b
}

func sqlChecksumPatch(sql string) []byte {
	sum := md5.Sum([]byte(sql))
	b, _ := json.Marshal(map[string]any{
		"spec": map[string]any{"flinkConfiguration": map[string]any{
			sqlChecksumKey: hex.EncodeToString(sum[:]),
		}},
	})
	return b
}

// annotationsPatch builds a merge patch on metadata.annotations. A nil value
// deletes that annotation (JSON null in a merge patch).
func annotationsPatch(anns map[string]any) []byte {
	b, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": anns},
	})
	return b
}
