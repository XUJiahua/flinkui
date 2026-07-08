package flink

import "github.com/fko-demo/flinkui/internal/cluster"

// Special status sentinels mirroring scripts/job.sh get_status().
const (
	StatusUnreachable = "UNREACHABLE"
	StatusNotFound    = "NOTFOUND"
	StatusUnknown     = "UNKNOWN"
)

// Health is an explicit, coarse classification of a deployment derived from the
// operator's lifecycleState and Flink's jobStatus.state (design §13: map the FKO
// lifecycle states explicitly instead of substring-matching a concatenated
// string). It tells an operator at a glance whether to intervene.
const (
	HealthHealthy     = "healthy"     // RUNNING/STABLE
	HealthProgressing = "progressing" // transient: upgrading/deploying/reconciling/restarting
	HealthDegraded    = "degraded"    // FAILED/FAILING/ROLLED_BACK — needs attention
	HealthSuspended   = "suspended"   // intentionally stopped
	HealthStopped     = "stopped"     // FINISHED/CANCELED (terminal, non-failure)
	HealthUnreachable = "unreachable" // cluster/object could not be read
	HealthNotFound    = "notfound"    // deployment does not exist
	HealthUnknown     = "unknown"     // could not classify
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
	// Health is the explicit classification (healthy/progressing/degraded/
	// suspended/stopped/unreachable/notfound/unknown) — design §13.
	Health string `json:"health"`
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
