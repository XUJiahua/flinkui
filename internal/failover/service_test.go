package failover

import (
	"encoding/json"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
)

func grp() config.LocalHAGroup {
	return config.LocalHAGroup{
		Name: "g", ClusterID: "cluster-a", PeerClusterID: "cluster-b",
		NeutralToken: "__switching__",
	}
}

func TestClassifyToken(t *testing.T) {
	g := grp()
	cases := map[string]string{
		"":              PointsUnset,
		"__switching__": PointsNeutral,
		"cluster-a":     PointsSelf,
		"cluster-b":     PointsPeer,
		"weird":         PointsUnknown,
	}
	for tok, want := range cases {
		if got := classifyToken(tok, g); got != want {
			t.Errorf("classifyToken(%q) = %q, want %q", tok, got, want)
		}
	}
}

func detail(healthy bool) *flink.JobDetail {
	st := "SUSPENDED/—"
	if healthy {
		st = "RUNNING/STABLE"
	}
	return &flink.JobDetail{JobSummary: flink.JobSummary{StatusText: st, Healthy: healthy, Reachable: true}}
}

func TestDeriveRole(t *testing.T) {
	s := &Service{}
	g := grp()
	tests := []struct {
		name        string
		pointsTo    string
		localHealthy bool
		wantRole    string
		wantWarn    bool
	}{
		{"active ok", PointsSelf, true, RoleActive, false},
		{"active but local down", PointsSelf, false, RoleActive, true},
		{"standby ok", PointsPeer, false, RoleStandby, false},
		{"standby but local running (split-brain)", PointsPeer, true, RoleStandby, true},
		{"neutral switching", PointsNeutral, false, RoleNeutral, false},
		{"unset while running", PointsUnset, true, RoleUnknown, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &LocalView{Local: detail(tt.localHealthy), Fencing: FencingState{PointsTo: tt.pointsTo}}
			s.deriveRole(v, g)
			if v.Role != tt.wantRole {
				t.Errorf("Role = %q, want %q", v.Role, tt.wantRole)
			}
			if (v.Warning != "") != tt.wantWarn {
				t.Errorf("warning=%q, wantWarn=%v", v.Warning, tt.wantWarn)
			}
		})
	}
}

func TestStatePatchJSON(t *testing.T) {
	var m map[string]any
	json.Unmarshal(statePatchJSON("suspended", "", 0), &m)
	job := m["spec"].(map[string]any)["job"].(map[string]any)
	if job["state"] != "suspended" {
		t.Errorf("state=%v", job["state"])
	}
	if _, ok := job["initialSavepointPath"]; ok {
		t.Error("suspended must not carry initialSavepointPath")
	}
	json.Unmarshal(statePatchJSON("running", "s3://b/sp", 42), &m)
	job = m["spec"].(map[string]any)["job"].(map[string]any)
	if job["initialSavepointPath"] != "s3://b/sp" || job["savepointRedeployNonce"].(float64) != 42 {
		t.Errorf("running patch wrong: %v", job)
	}
}

func TestTaskStore(t *testing.T) {
	s := newTaskStore()
	task := s.create("g", OpRelease, []string{StepSavepoint, StepSuspendLocal})
	if task.Status != TaskRunning || len(task.Steps) != 2 {
		t.Fatalf("create wrong: %+v", task)
	}
	s.setStep(task.ID, StepSavepoint, StepDone, "ok")
	got, ok := s.get(task.ID)
	if !ok || got.Steps[0].Status != StepDone {
		t.Fatalf("setStep failed: %+v", got)
	}
	s.update(task.ID, func(t *HATask) { t.Status = TaskSucceeded })
	got, _ = s.get(task.ID)
	if got.FinishedAt == nil {
		t.Error("expected FinishedAt")
	}
}
