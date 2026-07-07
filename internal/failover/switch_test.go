package failover

import (
	"encoding/json"
	"testing"
)

func TestStatePatchJSON(t *testing.T) {
	// suspended: no savepoint fields.
	var m map[string]any
	if err := json.Unmarshal(statePatchJSON("suspended", "", 0), &m); err != nil {
		t.Fatal(err)
	}
	job := m["spec"].(map[string]any)["job"].(map[string]any)
	if job["state"] != "suspended" {
		t.Errorf("state = %v", job["state"])
	}
	if _, ok := job["initialSavepointPath"]; ok {
		t.Error("suspended patch should not carry initialSavepointPath")
	}

	// running with recovery point: adds path + nonce.
	if err := json.Unmarshal(statePatchJSON("running", "s3://b/sp", 123), &m); err != nil {
		t.Fatal(err)
	}
	job = m["spec"].(map[string]any)["job"].(map[string]any)
	if job["state"] != "running" || job["initialSavepointPath"] != "s3://b/sp" {
		t.Errorf("running patch wrong: %v", job)
	}
	if job["savepointRedeployNonce"].(float64) != 123 {
		t.Errorf("nonce = %v", job["savepointRedeployNonce"])
	}
}

func TestSwitchStore(t *testing.T) {
	s := newSwitchStore()
	steps := []string{StepFenceNeutral, StepStopSource}
	task := s.create("g", DirectionFailover, steps)
	if task.Status != SwitchRunning || len(task.Steps) != 2 {
		t.Fatalf("create wrong: %+v", task)
	}
	s.setStep(task.ID, StepFenceNeutral, StepDone, "ok")
	got, ok := s.get(task.ID)
	if !ok || got.Steps[0].Status != StepDone || got.Steps[0].Message != "ok" {
		t.Fatalf("setStep not applied: %+v", got)
	}
	// finishing sets FinishedAt.
	s.update(task.ID, func(t *SwitchTask) { t.Status = SwitchSucceeded })
	got, _ = s.get(task.ID)
	if got.FinishedAt == nil {
		t.Error("expected FinishedAt after success")
	}
	// unknown id.
	if _, ok := s.get("nope"); ok {
		t.Error("expected miss for unknown id")
	}
}
