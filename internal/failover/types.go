// Package failover implements the observation (and, in P1b, orchestration) of
// primary/standby HA groups, platformizing scripts/failover.sh (design failover P1).
package failover

import (
	"time"

	"github.com/fko-demo/flinkui/internal/flink"
)

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

// Switch task status + step values.
const (
	SwitchRunning   = "running"
	SwitchSucceeded = "succeeded"
	SwitchFailed    = "failed"

	StepPending = "pending"
	StepRunning = "running"
	StepDone    = "done"
	StepFailed  = "failed"

	DirectionFailover = "failover" // primary -> standby
	DirectionFailback = "failback" // standby -> primary
)

// Switch step names (mirror do_switch's five steps).
const (
	StepFenceNeutral      = "FENCE_NEUTRAL"
	StepPickRecoveryPoint = "PICK_RECOVERY_POINT"
	StepStopSource        = "STOP_SOURCE"
	StepTokenToTarget     = "TOKEN_TO_TARGET"
	StepStartTarget       = "START_TARGET"
	StepVerify            = "VERIFY"
)

// StepState is one step of a SwitchTask.
type StepState struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // pending|running|done|failed
	Message string `json:"message,omitempty"`
}

// RecoveryPointRef records the recovery point chosen for a switch.
type RecoveryPointRef struct {
	Path string `json:"path"`
	Kind string `json:"kind"` // savepoint|checkpoint|none
}

// SwitchTask tracks a failover/failback as a resumable-in-spirit state machine
// (design failover §6). Stored in-memory this phase.
type SwitchTask struct {
	ID            string           `json:"id"`
	Group         string           `json:"group"`
	Direction     string           `json:"direction"`
	Status        string           `json:"status"`
	Steps         []StepState      `json:"steps"`
	RecoveryPoint RecoveryPointRef `json:"recoveryPoint"`
	Error         string           `json:"error,omitempty"`
	StartedAt     time.Time        `json:"startedAt"`
	FinishedAt    *time.Time       `json:"finishedAt,omitempty"`
}
