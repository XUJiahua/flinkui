package flink

import "github.com/fko-demo/flinkui/internal/cluster"

// Special status sentinels mirroring scripts/job.sh get_status().
const (
	StatusUnreachable = "UNREACHABLE"
	StatusNotFound    = "NOTFOUND"
	StatusUnknown     = "UNKNOWN"
)

// JobSummary is the dashboard row for a single FlinkDeployment (design §4.1).
type JobSummary struct {
	Namespace      string `json:"namespace"`
	Deployment     string `json:"deployment"`     // metadata.name
	JobName        string `json:"jobName"`        // short name (prefix stripped)
	JobState       string `json:"jobState"`       // status.jobStatus.state
	LifecycleState string `json:"lifecycleState"` // status.lifecycleState
	JobID          string `json:"jobId"`          // status.jobStatus.jobId
	DesiredState   string `json:"desiredState"`   // spec.job.state
	UpgradeMode    string `json:"upgradeMode"`    // spec.job.upgradeMode
	Parallelism    int64  `json:"parallelism"`    // spec.job.parallelism
	// StatusText is the combined "jobState/lifecycleState" (design §3.3).
	StatusText string `json:"statusText"`
	// Healthy is true only for RUNNING/STABLE.
	Healthy bool `json:"healthy"`
	// Reachable is false when the object/cluster could not be read.
	Reachable bool `json:"reachable"`
}

// JobDetail extends JobSummary with pods and events (design §4.1 / §4.3).
type JobDetail struct {
	JobSummary
	Pods   []cluster.PodInfo   `json:"pods"`
	Events []cluster.EventInfo `json:"events"`
}

// SavepointResult is returned after a savepoint completes.
type SavepointResult struct {
	Location string `json:"location"`
}
