package flink

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// JobMetrics is a compact, UI-friendly snapshot of a running job's internal
// health, pulled from the JobManager REST API (design backlog P2-1). It reuses
// the same pod-exec-curl path as Savepoint so it works both in-cluster and
// out-of-cluster (the <dep>-rest Service is only reachable in-cluster).
//
// It intentionally exposes a small, reliable subset — job state/uptime,
// aggregate throughput counters, and checkpoint health — rather than the full
// (large, per-vertex, version-varying) metric surface. It answers "is this job
// actually healthy?" at a glance without standing up Prometheus.
type JobMetrics struct {
	JobID string `json:"jobId"`
	Name  string `json:"name"`
	State string `json:"state"`
	// DurationMs is how long the job has been running (Flink "duration").
	DurationMs int64 `json:"durationMs"`

	// Vertices is the operator count; Parallelism is their summed parallelism.
	Vertices    int   `json:"vertices"`
	Parallelism int64 `json:"parallelism"`

	// Aggregate I/O counters summed across all vertices (cumulative since start,
	// not rates — rates require sampling two snapshots client-side).
	ReadRecords  int64 `json:"readRecords"`
	WriteRecords int64 `json:"writeRecords"`
	ReadBytes    int64 `json:"readBytes"`
	WriteBytes   int64 `json:"writeBytes"`

	// Checkpoints summarizes checkpointing health (nil when unavailable).
	Checkpoints *CheckpointStats `json:"checkpoints,omitempty"`
}

// CheckpointStats summarizes a job's checkpointing from /jobs/{jid}/checkpoints.
type CheckpointStats struct {
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
	InProgress int64 `json:"inProgress"`
	Total      int64 `json:"total"`
	Restored   int64 `json:"restored"`

	// Latest completed checkpoint (zero values when none yet).
	LastSizeBytes    int64  `json:"lastSizeBytes"`
	LastDurationMs   int64  `json:"lastDurationMs"`
	LastTimestampMs  int64  `json:"lastTimestampMs"`
	LastExternalPath string `json:"lastExternalPath,omitempty"`
}

// jobOverviewResp maps the fields we consume from GET /jobs/{jid}.
type jobOverviewResp struct {
	JID      string `json:"jid"`
	Name     string `json:"name"`
	State    string `json:"state"`
	Duration int64  `json:"duration"`
	Vertices []struct {
		Parallelism int64 `json:"parallelism"`
		Metrics     struct {
			ReadBytes    int64 `json:"read-bytes"`
			WriteBytes   int64 `json:"write-bytes"`
			ReadRecords  int64 `json:"read-records"`
			WriteRecords int64 `json:"write-records"`
		} `json:"metrics"`
	} `json:"vertices"`
}

// checkpointsResp maps the fields we consume from GET /jobs/{jid}/checkpoints.
type checkpointsResp struct {
	Counts struct {
		Restored   int64 `json:"restored"`
		Total      int64 `json:"total"`
		InProgress int64 `json:"in_progress"`
		Completed  int64 `json:"completed"`
		Failed     int64 `json:"failed"`
	} `json:"counts"`
	Latest struct {
		Completed *struct {
			EndToEndDuration   int64  `json:"end_to_end_duration"`
			StateSize          int64  `json:"state_size"`
			LatestAckTimestamp int64  `json:"latest_ack_timestamp"`
			ExternalPath       string `json:"external_path"`
		} `json:"completed"`
	} `json:"latest"`
}

// Metrics fetches a compact job-internal metrics snapshot for a deployment via
// the JobManager REST API (pod exec curl localhost:8081, matching Savepoint).
// It is read-only and takes no per-deployment lock. An empty jobId (job not
// running) is a clear error the UI can render as "no metrics; job not running".
func (s *Service) Metrics(ctx context.Context, name string) (*JobMetrics, error) {
	dep := s.cfg.DeploymentName(name)
	u, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return nil, err
	}
	jid, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "jobId")
	if jid == "" {
		return nil, fmt.Errorf("cannot get jobId (job not running?)")
	}
	selector := fmt.Sprintf("app=%s,component=jobmanager", dep)

	// Job overview (state, duration, per-vertex I/O counters).
	var ov jobOverviewResp
	if err := s.curlJSON(ctx, selector, fmt.Sprintf("http://localhost:8081/jobs/%s", jid), &ov); err != nil {
		return nil, fmt.Errorf("fetch job overview: %w", err)
	}
	m := &JobMetrics{
		JobID:      jid,
		Name:       ov.Name,
		State:      ov.State,
		DurationMs: ov.Duration,
		Vertices:   len(ov.Vertices),
	}
	for _, v := range ov.Vertices {
		m.Parallelism += v.Parallelism
		m.ReadRecords += v.Metrics.ReadRecords
		m.WriteRecords += v.Metrics.WriteRecords
		m.ReadBytes += v.Metrics.ReadBytes
		m.WriteBytes += v.Metrics.WriteBytes
	}

	// Checkpoint stats are best-effort: a job without checkpointing still returns
	// valid metrics (Checkpoints stays nil).
	var cp checkpointsResp
	if err := s.curlJSON(ctx, selector, fmt.Sprintf("http://localhost:8081/jobs/%s/checkpoints", jid), &cp); err == nil {
		stats := &CheckpointStats{
			Completed:  cp.Counts.Completed,
			Failed:     cp.Counts.Failed,
			InProgress: cp.Counts.InProgress,
			Total:      cp.Counts.Total,
			Restored:   cp.Counts.Restored,
		}
		if c := cp.Latest.Completed; c != nil {
			stats.LastSizeBytes = c.StateSize
			stats.LastDurationMs = c.EndToEndDuration
			stats.LastTimestampMs = c.LatestAckTimestamp
			stats.LastExternalPath = c.ExternalPath
		}
		m.Checkpoints = stats
	}

	return m, nil
}

// curlJSON runs `curl -s <url>` inside the JobManager container and decodes the
// stdout as JSON into out.
func (s *Service) curlJSON(ctx context.Context, selector, url string, out any) error {
	res, err := s.acc.Exec(ctx, selector, "flink-main-container", []string{"curl", "-s", url})
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(res.Stdout), out); err != nil {
		return fmt.Errorf("decode %s: %w (body: %.200s)", url, err, res.Stdout)
	}
	return nil
}
