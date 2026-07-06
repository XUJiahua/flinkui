package flink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// savepointTriggerResp is the JM REST response for POST /jobs/{jid}/savepoints.
type savepointTriggerResp struct {
	RequestID string `json:"request-id"`
}

// savepointStatusResp is the JM REST response for the savepoint status poll.
type savepointStatusResp struct {
	Status struct {
		ID string `json:"id"` // IN_PROGRESS / COMPLETED
	} `json:"status"`
	Operation struct {
		Location     string `json:"location"`
		FailureCause struct {
			Class      string `json:"class"`
			StackTrace string `json:"stack-trace"`
		} `json:"failure-cause"`
	} `json:"operation"`
}

// Savepoint triggers a savepoint via the JobManager REST API using the pod exec
// subresource (matches scripts/job.sh trigger_savepoint), polling until the
// operation completes or the configured timeout elapses (design §4.2/§6).
func (s *Service) Savepoint(ctx context.Context, name string) (*SavepointResult, error) {
	dep := s.cfg.DeploymentName(name)
	l := s.lockFor(dep)
	l.Lock()
	defer l.Unlock()

	u, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return nil, err
	}
	jid, _, _ := unstructured.NestedString(u.Object, "status", "jobStatus", "jobId")
	if jid == "" {
		return nil, fmt.Errorf("cannot get jobId (job not running?)")
	}

	// Determine the savepoint target directory. Prefer the deployment's own
	// configured state.savepoints.dir (authoritative, includes any bucket/prefix
	// layout). Fall back to the configured S3 bucket, else omit target-directory
	// so the JobManager uses its configured default.
	targetDir, _, _ := unstructured.NestedString(u.Object, "spec", "flinkConfiguration", "state.savepoints.dir")
	if targetDir == "" && s.cfg.Cluster.S3.Bucket != "" {
		targetDir = fmt.Sprintf("s3://%s/savepoints/%s", s.cfg.Cluster.S3.Bucket, s.cfg.JobName(dep))
	}
	selector := fmt.Sprintf("app=%s,component=jobmanager", dep)
	var body string
	if targetDir != "" {
		body = fmt.Sprintf(`{"target-directory":%q,"cancel-job":false}`, targetDir)
	} else {
		body = `{"cancel-job":false}`
	}

	// Trigger.
	triggerCmd := []string{
		"curl", "-s", "-X", "POST",
		"-H", "Content-Type: application/json",
		fmt.Sprintf("http://localhost:8081/jobs/%s/savepoints", jid),
		"-d", body,
	}
	res, err := s.acc.Exec(ctx, selector, "flink-main-container", triggerCmd)
	if err != nil {
		return nil, fmt.Errorf("trigger savepoint: %w", err)
	}
	var tr savepointTriggerResp
	if err := json.Unmarshal([]byte(res.Stdout), &tr); err != nil || tr.RequestID == "" {
		return nil, fmt.Errorf("savepoint trigger request failed: %s", res.Stdout)
	}

	// Poll.
	timeout := time.Duration(s.cfg.SavepointTimeoutSec) * time.Second
	deadline := time.Now().Add(timeout)
	statusCmd := []string{
		"curl", "-s",
		fmt.Sprintf("http://localhost:8081/jobs/%s/savepoints/%s", jid, tr.RequestID),
	}
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
		sres, err := s.acc.Exec(ctx, selector, "flink-main-container", statusCmd)
		if err != nil {
			continue
		}
		var st savepointStatusResp
		if err := json.Unmarshal([]byte(sres.Stdout), &st); err != nil {
			continue
		}
		if st.Status.ID == "COMPLETED" {
			if st.Operation.Location != "" {
				return &SavepointResult{Location: st.Operation.Location}, nil
			}
			if st.Operation.FailureCause.Class != "" {
				return nil, fmt.Errorf("savepoint failed: %s", st.Operation.FailureCause.Class)
			}
			return nil, fmt.Errorf("savepoint COMPLETED but no location")
		}
	}
	return nil, fmt.Errorf("savepoint did not complete within %ds", s.cfg.SavepointTimeoutSec)
}
