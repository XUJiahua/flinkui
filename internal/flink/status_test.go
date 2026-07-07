package flink

import (
	"encoding/json"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestCombinedStatus(t *testing.T) {
	tests := []struct {
		name       string
		jobState   string
		lifecycle  string
		specState  string
		want       string
	}{
		{"running stable", "RUNNING", "STABLE", "running", "RUNNING/STABLE"},
		{"status empty, spec suspended", "", "", "suspended", "SUSPENDED/—"},
		{"status empty, spec running", "", "", "running", "STARTING/—"},
		{"status empty, spec unknown", "", "", "", "UNKNOWN/UNKNOWN"},
		{"partial jobState only", "RECONCILING", "", "running", "RECONCILING/UNKNOWN"},
		{"partial lifecycle only", "", "DEPLOYED", "running", "UNKNOWN/DEPLOYED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := combinedStatus(tt.jobState, tt.lifecycle, tt.specState); got != tt.want {
				t.Errorf("combinedStatus(%q,%q,%q) = %q, want %q",
					tt.jobState, tt.lifecycle, tt.specState, got, tt.want)
			}
		})
	}
}

func TestSummaryFromWithFallback(t *testing.T) {
	svc := NewService(nil, &config.Config{DeploymentPrefix: "flink-sql-job-"})

	// Full status present -> healthy.
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "flink-sql-job-codes", "namespace": "flink-jobs"},
		"spec":     map[string]any{"job": map[string]any{"state": "running", "parallelism": int64(2)}},
		"status": map[string]any{
			"jobStatus":      map[string]any{"state": "RUNNING", "jobId": "abc"},
			"lifecycleState": "STABLE",
		},
	}}
	s := svc.summaryFrom(u)
	if s.JobName != "codes" || s.StatusText != "RUNNING/STABLE" || !s.Healthy || !s.Reachable {
		t.Errorf("unexpected summary: %+v", s)
	}
	if s.JobID != "abc" || s.Parallelism != 2 {
		t.Errorf("jobId/parallelism wrong: %+v", s)
	}

	// No status, spec suspended -> SUSPENDED/—, not healthy.
	u2 := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "flink-sql-job-idle"},
		"spec":     map[string]any{"job": map[string]any{"state": "suspended"}},
	}}
	s2 := svc.summaryFrom(u2)
	if s2.StatusText != "SUSPENDED/—" || s2.Healthy {
		t.Errorf("suspended summary wrong: %+v", s2)
	}
}

func TestSavepointResponseParsing(t *testing.T) {
	var tr savepointTriggerResp
	if err := json.Unmarshal([]byte(`{"request-id":"req-123"}`), &tr); err != nil {
		t.Fatalf("trigger unmarshal: %v", err)
	}
	if tr.RequestID != "req-123" {
		t.Errorf("RequestID = %q, want req-123", tr.RequestID)
	}

	completed := `{"status":{"id":"COMPLETED"},"operation":{"location":"s3://b/savepoints/j/savepoint-x"}}`
	var st savepointStatusResp
	if err := json.Unmarshal([]byte(completed), &st); err != nil {
		t.Fatalf("status unmarshal: %v", err)
	}
	if st.Status.ID != "COMPLETED" || st.Operation.Location != "s3://b/savepoints/j/savepoint-x" {
		t.Errorf("parsed status wrong: %+v", st)
	}

	failed := `{"status":{"id":"COMPLETED"},"operation":{"failure-cause":{"class":"java.lang.Exception"}}}`
	var sf savepointStatusResp
	if err := json.Unmarshal([]byte(failed), &sf); err != nil {
		t.Fatalf("failed unmarshal: %v", err)
	}
	if sf.Operation.FailureCause.Class != "java.lang.Exception" {
		t.Errorf("failure class = %q", sf.Operation.FailureCause.Class)
	}
}
