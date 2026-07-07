// Package failover implements decentralized (peer-model) HA for the case where
// the peer cluster's k8s API is unreachable: each flinkui acts only on its LOCAL
// cluster and coordinates through shared S3 (fencing token + handoff record).
// A switch is two local half-operations — Release (step down) and Promote (take
// over). See docs/failover-decentralized-design.md.
package failover

import (
	"time"

	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
)

// Fencing pointer values (relative to THIS instance).
const (
	PointsSelf    = "self"
	PointsPeer    = "peer"
	PointsNeutral = "neutral"
	PointsUnset   = "unset"
	PointsUnknown = "unknown"
)

// Roles derived for the local side.
const (
	RoleActive  = "active"
	RoleStandby = "standby"
	RoleNeutral = "neutral" // a switch is in progress (neutral token)
	RoleUnknown = "unknown"
)

// FencingState is the shared token and where it points relative to this side.
type FencingState struct {
	Token    string `json:"token"`
	PointsTo string `json:"pointsTo"` // self|peer|neutral|unset|unknown
	Error    string `json:"error,omitempty"`
}

// LocalView is one HA group as seen from THIS instance: only the local side is
// observed; the peer side is explicitly "not observed" (cross-cluster).
type LocalView struct {
	Name          string               `json:"name"`
	ClusterID     string               `json:"clusterId"`     // this side
	PeerClusterID string               `json:"peerClusterId"` // other side (unobserved)
	Namespace     string               `json:"namespace"`
	Deployment    string               `json:"deployment"`
	Local         *flink.JobDetail     `json:"local"`
	Fencing       FencingState         `json:"fencing"`
	Handoff       *store.HandoffRecord `json:"handoff"`
	Role          string               `json:"role"` // active|standby|neutral|unknown
	Warning       string               `json:"warning,omitempty"`
}

// HA task op + status + step values.
const (
	OpRelease = "release"
	OpPromote = "promote"

	TaskRunning   = "running"
	TaskSucceeded = "succeeded"
	TaskFailed    = "failed"

	StepPending = "pending"
	StepRunning = "running"
	StepDone    = "done"
	StepFailed  = "failed"
)

// Release / Promote step names (mirror the design state machines).
const (
	StepSavepoint     = "SAVEPOINT"
	StepSuspendLocal  = "SUSPEND_LOCAL"
	StepWaitStopped   = "WAIT_LOCAL_STOPPED"
	StepTokenNeutral  = "TOKEN_NEUTRAL"
	StepWriteHandoff  = "WRITE_HANDOFF"
	StepReadHandoff   = "READ_HANDOFF"
	StepPickRecovery  = "PICK_RECOVERY_POINT"
	StepTokenToSelf   = "TOKEN_TO_SELF"
	StepStartLocal    = "START_LOCAL"
	StepVerifyLocal   = "VERIFY_LOCAL"
)

// StepState is one step of an HA task.
type StepState struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// HATask tracks a Release or Promote as an in-memory state machine.
type HATask struct {
	ID            string                 `json:"id"`
	Group         string                 `json:"group"`
	Op            string                 `json:"op"` // release|promote
	Status        string                 `json:"status"`
	Steps         []StepState            `json:"steps"`
	RecoveryPoint store.RecoveryPointRef `json:"recoveryPoint"`
	Epoch         int64                  `json:"epoch"`
	Error         string                 `json:"error,omitempty"`
	StartedAt     time.Time              `json:"startedAt"`
	FinishedAt    *time.Time             `json:"finishedAt,omitempty"`
}
