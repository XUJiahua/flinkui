package flink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// OperationType identifies a long-running lifecycle operation.
type OperationType string

const (
	OpSavepoint OperationType = "savepoint"
	OpRestart   OperationType = "restart"
)

// OperationStatus is the lifecycle state of an async operation.
type OperationStatus string

const (
	OpRunning   OperationStatus = "running"
	OpSucceeded OperationStatus = "succeeded"
	OpFailed    OperationStatus = "failed"
)

// Operation is a tracked async lifecycle operation (design §4.2: savepoint and
// restart are asynchronous with progress the UI can display).
type Operation struct {
	ID         string          `json:"id"`
	Type       OperationType   `json:"type"`
	Deployment string          `json:"deployment"`
	JobName    string          `json:"jobName"`
	Status     OperationStatus `json:"status"`
	Progress   string          `json:"progress"`         // human-readable current step
	Result     string          `json:"result,omitempty"` // e.g. savepoint location
	Error      string          `json:"error,omitempty"`
	StartedAt  time.Time       `json:"startedAt"`
	FinishedAt *time.Time      `json:"finishedAt,omitempty"`
}

// operationStore is an in-memory, concurrency-safe registry of operations with
// bounded retention of finished entries.
type operationStore struct {
	mu       sync.RWMutex
	ops      map[string]*Operation
	maxKeep  int
	finished []string // finished IDs in completion order (for pruning)
}

func newOperationStore() *operationStore {
	return &operationStore{ops: map[string]*Operation{}, maxKeep: 200}
}

// create registers a new running operation and returns a copy.
func (s *operationStore) create(t OperationType, deployment, jobName string) *Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := &Operation{
		ID:         uuid.NewString(),
		Type:       t,
		Deployment: deployment,
		JobName:    jobName,
		Status:     OpRunning,
		Progress:   "starting",
		StartedAt:  time.Now(),
	}
	s.ops[op.ID] = op
	return copyOp(op)
}

// update applies a mutation to a tracked operation under lock.
func (s *operationStore) update(id string, fn func(*Operation)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[id]
	if !ok {
		return
	}
	fn(op)
	if op.Status != OpRunning && op.FinishedAt == nil {
		now := time.Now()
		op.FinishedAt = &now
		s.finished = append(s.finished, id)
		s.pruneLocked()
	}
}

// pruneLocked evicts the oldest finished operations beyond maxKeep.
func (s *operationStore) pruneLocked() {
	for len(s.finished) > s.maxKeep {
		oldest := s.finished[0]
		s.finished = s.finished[1:]
		delete(s.ops, oldest)
	}
}

// get returns a copy of an operation by ID.
func (s *operationStore) get(id string) (*Operation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.ops[id]
	if !ok {
		return nil, false
	}
	return copyOp(op), true
}

func copyOp(op *Operation) *Operation {
	cp := *op
	if op.FinishedAt != nil {
		t := *op.FinishedAt
		cp.FinishedAt = &t
	}
	return &cp
}

// GetOperation returns a tracked operation by ID.
func (s *Service) GetOperation(id string) (*Operation, bool) {
	return s.ops.get(id)
}

func (s *Service) failOp(id string, err error) {
	s.ops.update(id, func(o *Operation) {
		o.Status = OpFailed
		o.Error = err.Error()
		o.Progress = "failed"
	})
}

func (s *Service) succeedOp(id, result, progress string) {
	s.ops.update(id, func(o *Operation) {
		o.Status = OpSucceeded
		o.Result = result
		o.Progress = progress
	})
}

// StartSavepoint triggers a savepoint asynchronously and returns the tracking
// operation immediately (design §4.2: savepoint is async with UI progress).
func (s *Service) StartSavepoint(name string) *Operation {
	dep := s.cfg.DeploymentName(name)
	op := s.ops.create(OpSavepoint, dep, s.cfg.JobName(dep))
	go func() {
		// Detached context: the HTTP request that started this has already returned.
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(s.cfg.SavepointTimeoutSec+30)*time.Second)
		defer cancel()
		s.ops.update(op.ID, func(o *Operation) {
			o.Progress = "triggering savepoint via JobManager REST"
		})
		res, err := s.Savepoint(ctx, name) // acquires the per-deployment lock itself
		if err != nil {
			s.failOp(op.ID, err)
			return
		}
		s.succeedOp(op.ID, res.Location, "savepoint completed")
	}()
	return op
}

// StartRestart runs suspend -> wait for JM pod = 0 -> resume asynchronously,
// reporting the wait progress into the operation (design §4.2).
func (s *Service) StartRestart(name string) *Operation {
	dep := s.cfg.DeploymentName(name)
	op := s.ops.create(OpRestart, dep, s.cfg.JobName(dep))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(s.cfg.StopTimeoutSec+60)*time.Second)
		defer cancel()

		l := s.lockFor(dep)
		l.Lock()
		defer l.Unlock()

		s.ops.update(op.ID, func(o *Operation) { o.Progress = "suspending" })
		if err := s.acc.PatchFlinkDeployment(ctx, dep, statePatch("suspended")); err != nil {
			s.failOp(op.ID, fmt.Errorf("suspend failed: %w", err))
			return
		}
		s.waitStoppedProgress(ctx, dep, func(pods int) {
			s.ops.update(op.ID, func(o *Operation) {
				if pods == 0 {
					o.Progress = "JobManager stopped; resuming"
				} else {
					o.Progress = fmt.Sprintf("waiting for JobManager pod to terminate (%d running)", pods)
				}
			})
		})
		s.ops.update(op.ID, func(o *Operation) { o.Progress = "resuming (last-state)" })
		if err := s.acc.PatchFlinkDeployment(ctx, dep, statePatch("running")); err != nil {
			s.failOp(op.ID, fmt.Errorf("resume failed: %w", err))
			return
		}
		s.succeedOp(op.ID, "", "restart requested; job resuming from last-state")
	}()
	return op
}
