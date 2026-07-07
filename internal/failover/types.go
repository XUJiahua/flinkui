// Package failover implements the observation (and, in P1b, orchestration) of
// primary/standby HA groups, platformizing scripts/failover.sh (design failover P1).
package failover

import "github.com/fko-demo/flinkui/internal/flink"

// Fencing pointer values.
const (
	PointsPrimary = "primary"
	PointsStandby = "standby"
	PointsNeutral = "neutral"
	PointsUnset   = "unset"
	PointsUnknown = "unknown"
)

// Active side values.
const (
	ActivePrimary = "primary"
	ActiveStandby = "standby"
	ActiveNone    = "none"
	ActiveUnknown = "unknown"
)

// FencingState is the current S3 fencing token and where it points.
type FencingState struct {
	Token    string `json:"token"`
	PointsTo string `json:"pointsTo"`
	// Error is set when the token could not be read (S3 unconfigured/unreachable).
	Error string `json:"error,omitempty"`
}

// SideView is one side of an HA group with its resolved identity and status.
type SideView struct {
	Role       string           `json:"role"` // "primary" | "standby"
	Cluster    string           `json:"cluster"`
	Namespace  string           `json:"namespace"`
	Deployment string           `json:"deployment"`
	ClusterID  string           `json:"clusterId"`
	Detail     *flink.JobDetail `json:"detail"`
}

// GroupView is the observation of a whole HA group.
type GroupView struct {
	Name       string       `json:"name"`
	Primary    SideView     `json:"primary"`
	Standby    SideView     `json:"standby"`
	Fencing    FencingState `json:"fencing"`
	ActiveSide string       `json:"activeSide"` // primary|standby|none|unknown
	SplitBrain bool         `json:"splitBrain"`
	Warning    string       `json:"warning,omitempty"`
}
