package flink

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// replayFake is an in-memory ClusterAccessor + ConfigMapAccessor that applies
// JSON merge patches to a CR so tests can assert the resulting spec/annotations.
type replayFake struct {
	cluster.ClusterAccessor
	cr  *unstructured.Unstructured
	cms map[string]map[string]string
}

func (f *replayFake) GetFlinkDeployment(_ context.Context, _ string) (*unstructured.Unstructured, error) {
	return f.cr, nil
}
func (f *replayFake) PatchFlinkDeployment(_ context.Context, _ string, patch []byte) error {
	var p map[string]any
	if err := json.Unmarshal(patch, &p); err != nil {
		return err
	}
	mergeInto(f.cr.Object, p)
	return nil
}
func (f *replayFake) CountPods(_ context.Context, _ string) (int, error) { return 0, nil }

func (f *replayFake) Exec(_ context.Context, _, _ string, cmd []string) (*cluster.ExecResult, error) {
	var url string
	for _, a := range cmd {
		if strings.Contains(a, "/savepoints") {
			url = a
		}
	}
	if strings.HasSuffix(url, "/savepoints") { // trigger
		return &cluster.ExecResult{Stdout: `{"request-id":"r1"}`}, nil
	}
	return &cluster.ExecResult{Stdout: `{"status":{"id":"COMPLETED"},"operation":{"location":"s3://bucket/savepoints/job1/savepoint-abc"}}`}, nil
}

func (f *replayFake) GetConfigMap(_ context.Context, name string) (map[string]string, bool, error) {
	d, ok := f.cms[name]
	if !ok {
		return nil, false, nil
	}
	// return a copy so callers can't mutate our store directly
	cp := map[string]string{}
	for k, v := range d {
		cp[k] = v
	}
	return cp, true, nil
}
func (f *replayFake) PatchConfigMapKey(_ context.Context, name, key, value string) error {
	if f.cms[name] == nil {
		f.cms[name] = map[string]string{}
	}
	f.cms[name][key] = value
	return nil
}
func (f *replayFake) DeleteConfigMapKey(_ context.Context, name, key string) error {
	delete(f.cms[name], key)
	return nil
}

// mergeInto applies an RFC 7386 JSON merge patch (nil deletes a key).
func mergeInto(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		if vm, ok := v.(map[string]any); ok {
			dm, ok := dst[k].(map[string]any)
			if !ok {
				dm = map[string]any{}
				dst[k] = dm
			}
			mergeInto(dm, vm)
			continue
		}
		dst[k] = v
	}
}

func newReplayFixture() (*Service, *replayFake) {
	cr := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "flink-sql-job-job1", "namespace": "flink-jobs"},
		"spec": map[string]any{
			"job": map[string]any{"upgradeMode": "last-state", "state": "running"},
			"podTemplate": map[string]any{"spec": map[string]any{"volumes": []any{
				map[string]any{"name": "sql-scripts", "configMap": map[string]any{"name": "flink-sql-job1"}},
			}}},
		},
		"status": map[string]any{
			"jobStatus":      map[string]any{"state": "RUNNING", "jobId": "jid-123"},
			"lifecycleState": "STABLE",
		},
	}}
	f := &replayFake{cr: cr, cms: map[string]map[string]string{
		"flink-sql-job1": {"job.sql": kafkaSourceSQL},
	}}
	cfg := &config.Config{
		DeploymentPrefix:    "flink-sql-job-",
		StopTimeoutSec:      2,
		SavepointTimeoutSec: 30,
		Replay:              config.ReplayConfig{Enabled: true, SQLVolumeName: "sql-scripts", SQLFileKey: "job.sql"},
	}
	return NewService(f, cfg), f
}

func waitOp(t *testing.T, s *Service, id string) *Operation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		op, ok := s.GetOperation(id)
		if ok && op.Status != OpRunning {
			return op
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("operation did not finish in time")
	return nil
}

func TestReplayThenRevertRoundTrip(t *testing.T) {
	svc, f := newReplayFixture()
	ctx := context.Background()

	// --- Replay ---
	op := waitOp(t, svc, svc.StartReplay("job1", "", 1730000000000, "earliest", "tester").ID)
	if op.Status != OpSucceeded {
		t.Fatalf("replay failed: %s / %s", op.Status, op.Error)
	}
	sql := f.cms["flink-sql-job1"]["job.sql"]
	if !contains(sql, "'scan.startup.mode' = 'timestamp'") ||
		!contains(sql, "'scan.startup.timestamp-millis' = '1730000000000'") {
		t.Errorf("SQL not rewritten for replay:\n%s", sql)
	}
	if _, ok := f.cms["flink-sql-job1"]["job.sql.flinkui-prereplay-backup"]; !ok {
		t.Error("original SQL was not backed up")
	}
	if mode := nestedStr(f.cr, "spec", "job", "upgradeMode"); mode != "stateless" {
		t.Errorf("upgradeMode = %q, want stateless", mode)
	}
	ann := f.cr.GetAnnotations()
	if ann[annReplayActive] != "true" || ann[annReplayPrevMode] != "last-state" {
		t.Errorf("replay annotations wrong: %v", ann)
	}
	if nestedStr(f.cr, "spec", "flinkConfiguration", sqlChecksumKey) == "" {
		t.Error("sql-checksum was not injected")
	}
	// A second replay must be rejected while one is active.
	op2 := waitOp(t, svc, svc.StartReplay("job1", "", 1, "earliest", "tester").ID)
	if op2.Status != OpFailed {
		t.Error("second concurrent replay should fail while one is active")
	}

	// --- Revert ---
	rev := waitOp(t, svc, svc.StartRevert("job1", "tester").ID)
	if rev.Status != OpSucceeded {
		t.Fatalf("revert failed: %s / %s", rev.Status, rev.Error)
	}
	if got := f.cms["flink-sql-job1"]["job.sql"]; got != kafkaSourceSQL {
		t.Errorf("SQL not restored to original:\n%s", got)
	}
	if _, ok := f.cms["flink-sql-job1"]["job.sql.flinkui-prereplay-backup"]; ok {
		t.Error("backup key should be cleared after revert")
	}
	if mode := nestedStr(f.cr, "spec", "job", "upgradeMode"); mode != "last-state" {
		t.Errorf("upgradeMode = %q, want last-state after revert", mode)
	}
	if sp := nestedStr(f.cr, "spec", "job", "initialSavepointPath"); sp == "" {
		t.Error("revert should redeploy from a savepoint (initialSavepointPath set)")
	}
	if rev.Result == "" {
		t.Error("revert op should surface the recovery savepoint path in Result")
	}
	if f.cr.GetAnnotations()[annReplayActive] == "true" {
		t.Error("replay-active annotation should be cleared after revert")
	}

	// Preview performs no writes.
	before := f.cms["flink-sql-job1"]["job.sql"]
	if _, err := svc.ReplayPreview(ctx, "job1", "", 999, "earliest"); err != nil {
		t.Fatalf("preview: %v", err)
	}
	if f.cms["flink-sql-job1"]["job.sql"] != before {
		t.Error("preview must not write SQL")
	}
}

func TestGetJobSQLResolvesConfigMapAndSources(t *testing.T) {
	svc, _ := newReplayFixture()
	res, err := svc.GetJobSQL(context.Background(), "job1")
	if err != nil {
		t.Fatalf("GetJobSQL: %v", err)
	}
	if res.ConfigMap != "flink-sql-job1" {
		t.Errorf("resolved cm = %q, want flink-sql-job1 (from CR volume, not name prefix)", res.ConfigMap)
	}
	if len(res.KafkaSources) != 1 || res.KafkaSources[0] != "evofinder_orders" {
		t.Errorf("kafka sources = %v, want [evofinder_orders]", res.KafkaSources)
	}
}

func nestedStr(u *unstructured.Unstructured, fields ...string) string {
	v, _, _ := unstructured.NestedString(u.Object, fields...)
	return v
}
func contains(s, sub string) bool { return len(s) >= len(sub) && (indexOf(s, sub) >= 0) }
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestRevertRefusesWhenJobUnhealthyAndKeepsSession(t *testing.T) {
	svc, f := newReplayFixture()

	// Start a replay so a session/backup exists.
	if op := waitOp(t, svc, svc.StartReplay("job1", "", 1730000000000, "earliest", "tester").ID); op.Status != OpSucceeded {
		t.Fatalf("replay failed: %s", op.Error)
	}
	// Make the job unhealthy (e.g. failing) so a savepoint cannot be taken.
	unstructured.SetNestedField(f.cr.Object, "FAILING", "status", "jobStatus", "state")
	unstructured.SetNestedField(f.cr.Object, "", "status", "lifecycleState")

	rev := waitOp(t, svc, svc.StartRevert("job1", "tester").ID)
	if rev.Status != OpFailed {
		t.Fatalf("revert should fail preflight when job is unhealthy, got %s", rev.Status)
	}
	// The replay session MUST be kept so the operator can retry / recover.
	if _, ok := f.cms["flink-sql-job1"]["job.sql.flinkui-prereplay-backup"]; !ok {
		t.Error("backup key must be kept when revert fails preflight")
	}
	if f.cr.GetAnnotations()[annReplayActive] != "true" {
		t.Error("replay-active annotation must be kept when revert fails preflight")
	}
}

func TestReplayStatelessPatchIsSingleCoherentDiff(t *testing.T) {
	var p map[string]any
	if err := json.Unmarshal(replayStatelessPatch("SELECT 1"), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	spec := p["spec"].(map[string]any)
	// One patch must carry BOTH the upgradeMode flip and the checksum, so the
	// operator reconciles once (not once for checksum, then again for the mode).
	if spec["job"].(map[string]any)["upgradeMode"] != "stateless" {
		t.Error("patch missing upgradeMode=stateless")
	}
	fc := spec["flinkConfiguration"].(map[string]any)
	if fc[sqlChecksumKey] != sqlChecksum("SELECT 1") {
		t.Error("patch missing/incorrect sql-checksum")
	}
}

func TestRevertRedeployPatchIsSingleWrite(t *testing.T) {
	var p map[string]any
	if err := json.Unmarshal(revertRedeployPatch("last-state", "s3://b/sp-1", "SELECT 2"), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	job := p["spec"].(map[string]any)["job"].(map[string]any)
	if job["initialSavepointPath"] != "s3://b/sp-1" || job["upgradeMode"] != "last-state" || job["state"] != "running" {
		t.Errorf("redeploy patch job fields wrong: %v", job)
	}
	fc := p["spec"].(map[string]any)["flinkConfiguration"].(map[string]any)
	if fc[sqlChecksumKey] != sqlChecksum("SELECT 2") {
		t.Error("redeploy patch should fold in the sql-checksum (single reconcile)")
	}
	// The SAME patch must clear the replay annotations, so revert is one CR write
	// (no second write to collide with the operator's redeploy reconcile).
	ann := p["metadata"].(map[string]any)["annotations"].(map[string]any)
	for _, k := range []string{annReplayActive, annReplayPrevMode, annReplayFrom} {
		if v, ok := ann[k]; !ok || v != nil {
			t.Errorf("annotation %s should be cleared (nil) in the same patch, got %v (present=%v)", k, v, ok)
		}
	}
}
