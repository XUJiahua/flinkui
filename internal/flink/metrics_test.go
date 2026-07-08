package flink

import (
	"context"
	"strings"
	"testing"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// metricsAccessor is a fake ClusterAccessor that returns a FlinkDeployment with
// a jobId and canned JM REST JSON per exec'd curl URL.
type metricsAccessor struct {
	cluster.ClusterAccessor
	jobID    string
	overview string
	checkpts string
	cpErr    error
	lastURLs []string
}

func (a *metricsAccessor) GetFlinkDeployment(_ context.Context, name string) (*unstructured.Unstructured, error) {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": name, "namespace": "flink-jobs"},
		"status":   map[string]any{"jobStatus": map[string]any{"jobId": a.jobID}},
	}}, nil
}

func (a *metricsAccessor) Exec(_ context.Context, _, _ string, cmd []string) (*cluster.ExecResult, error) {
	url := cmd[len(cmd)-1]
	a.lastURLs = append(a.lastURLs, url)
	switch {
	case strings.HasSuffix(url, "/checkpoints"):
		if a.cpErr != nil {
			return nil, a.cpErr
		}
		return &cluster.ExecResult{Stdout: a.checkpts}, nil
	default:
		return &cluster.ExecResult{Stdout: a.overview}, nil
	}
}

func newMetricsSvc(a cluster.ClusterAccessor) *Service {
	return NewService(a, &config.Config{DeploymentPrefix: "flink-sql-job-"})
}

func TestMetricsAggregatesVerticesAndCheckpoints(t *testing.T) {
	acc := &metricsAccessor{
		jobID: "job-abc",
		overview: `{
			"jid":"job-abc","name":"orders","state":"RUNNING","duration":60000,
			"vertices":[
				{"parallelism":2,"metrics":{"read-bytes":10,"write-bytes":20,"read-records":5,"write-records":7}},
				{"parallelism":3,"metrics":{"read-bytes":100,"write-bytes":200,"read-records":50,"write-records":70}}
			]
		}`,
		checkpts: `{
			"counts":{"restored":1,"total":10,"in_progress":0,"completed":9,"failed":1},
			"latest":{"completed":{"end_to_end_duration":120,"state_size":1048576,"latest_ack_timestamp":1699999999999,"external_path":"s3://b/chk-9"}}
		}`,
	}
	m, err := newMetricsSvc(acc).Metrics(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}

	if m.JobID != "job-abc" || m.Name != "orders" || m.State != "RUNNING" || m.DurationMs != 60000 {
		t.Errorf("overview fields wrong: %+v", m)
	}
	if m.Vertices != 2 || m.Parallelism != 5 {
		t.Errorf("vertices/parallelism = %d/%d, want 2/5", m.Vertices, m.Parallelism)
	}
	if m.ReadRecords != 55 || m.WriteRecords != 77 || m.ReadBytes != 110 || m.WriteBytes != 220 {
		t.Errorf("aggregate I/O wrong: %+v", m)
	}
	if m.Checkpoints == nil {
		t.Fatal("checkpoints nil, want stats")
	}
	cp := m.Checkpoints
	if cp.Completed != 9 || cp.Failed != 1 || cp.Total != 10 || cp.Restored != 1 {
		t.Errorf("checkpoint counts wrong: %+v", cp)
	}
	if cp.LastSizeBytes != 1048576 || cp.LastDurationMs != 120 || cp.LastTimestampMs != 1699999999999 || cp.LastExternalPath != "s3://b/chk-9" {
		t.Errorf("last checkpoint wrong: %+v", cp)
	}
}

// A job with no jobId (not running) is a clear error, not a panic.
func TestMetricsNoJobID(t *testing.T) {
	acc := &metricsAccessor{jobID: ""}
	_, err := newMetricsSvc(acc).Metrics(context.Background(), "idle")
	if err == nil || !strings.Contains(err.Error(), "jobId") {
		t.Fatalf("want jobId error, got %v", err)
	}
}

// Checkpoints unavailable (e.g. checkpointing disabled / endpoint error) still
// yields valid overview metrics with Checkpoints=nil rather than failing.
func TestMetricsCheckpointsBestEffort(t *testing.T) {
	acc := &metricsAccessor{
		jobID:    "job-abc",
		overview: `{"jid":"job-abc","name":"orders","state":"RUNNING","duration":1,"vertices":[]}`,
		cpErr:    context.DeadlineExceeded,
	}
	m, err := newMetricsSvc(acc).Metrics(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Metrics should tolerate checkpoint failure: %v", err)
	}
	if m.Checkpoints != nil {
		t.Errorf("Checkpoints = %+v, want nil on fetch error", m.Checkpoints)
	}
}
