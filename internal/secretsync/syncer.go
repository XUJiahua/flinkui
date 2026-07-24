package secretsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
)

// hashAnnotation records, on the FlinkDeployment, the value-hash last applied so
// the loop only restarts when the synced values actually change.
const hashAnnotation = "flinkui.fko-demo.io/openbao-value-hash"

// Accessor is the subset of cluster capabilities the syncer needs: read/write
// Secrets, and read/patch FlinkDeployments (to bump restartNonce). KubeAccessor
// satisfies this (ClusterAccessor + SecretAccessor).
type Accessor interface {
	Namespace() string
	GetFlinkDeployment(ctx context.Context, name string) (*unstructured.Unstructured, error)
	PatchFlinkDeployment(ctx context.Context, name string, mergePatch []byte) error
	cluster.SecretAccessor
}

// RestartGuard optionally vetoes a restart (e.g. HA: only the active side, never
// mid-switch). *failover.Service satisfies this structurally. When no guard is
// set, restarts always proceed.
type RestartGuard interface {
	SafeToRestart(ctx context.Context, deployment string) (bool, string)
}

// lastNonce backs restartNonce values; seeded from wall clock so it keeps
// increasing across process restarts.
var lastNonce int64 = time.Now().Unix()

func nextNonce() int64 {
	for {
		prev := atomic.LoadInt64(&lastNonce)
		next := time.Now().Unix()
		if next <= prev {
			next = prev + 1
		}
		if atomic.CompareAndSwapInt64(&lastNonce, prev, next) {
			return next
		}
	}
}

// ItemStatus is the last outcome of syncing one item (for the UI/API).
type ItemStatus struct {
	SecretName        string `json:"secretName"`
	KVPath            string `json:"kvPath"`
	RestartDeployment string `json:"restartDeployment,omitempty"`
	OK                bool   `json:"ok"`
	Keys              int    `json:"keys"`
	Error             string `json:"error,omitempty"`
}

// Status is a snapshot of the sync loop for the UI/API.
type Status struct {
	Enabled      bool              `json:"enabled"`
	AutoRestart  bool              `json:"autoRestart"`
	IntervalSec  int               `json:"intervalSec"`
	LastSyncUnix int64             `json:"lastSyncUnix"` // 0 = never synced yet
	Running      bool              `json:"running"`      // a sync is in progress
	Items        []ItemStatus      `json:"items"`
	Restarts     map[string]int    `json:"restarts"`          // deployment -> restarts triggered this process
	Skipped      map[string]string `json:"skipped,omitempty"` // deployment -> why last restart was skipped (guard)
}

// Syncer runs the periodic OpenBao -> Secret sync (+ optional restart) loop.
type Syncer struct {
	acc Accessor
	bao *baoClient
	cfg config.SecretSyncConfig

	runMu sync.Mutex // serializes syncOnce (ticker vs manual SyncNow)

	guard RestartGuard // optional restart veto (HA interlock)

	mu       sync.Mutex // guards the status snapshot below
	lastSync time.Time
	running  bool
	items    []ItemStatus
	restarts map[string]int
	skipped  map[string]string
}

// New builds a Syncer. Returns (nil, nil) when sync is disabled or has no items,
// so callers can simply skip starting it.
func New(acc Accessor, cfg config.SecretSyncConfig) (*Syncer, error) {
	if !cfg.Enabled || len(cfg.Items) == 0 {
		return nil, nil
	}
	bao, err := newBaoClient(cfg.OpenBao)
	if err != nil {
		return nil, err
	}
	return &Syncer{acc: acc, bao: bao, cfg: cfg, restarts: map[string]int{}, skipped: map[string]string{}}, nil
}

// SetRestartGuard installs an optional restart veto (e.g. the HA failover
// service, so only the active side is restarted and never mid-switch).
func (s *Syncer) SetRestartGuard(g RestartGuard) { s.guard = g }

// Run drives the loop until ctx is cancelled. It syncs once immediately (so the
// Secret exists before jobs start), then on every interval tick.
func (s *Syncer) Run(ctx context.Context) {
	interval := time.Duration(s.cfg.IntervalSec) * time.Second
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	log.Printf("secretsync: started (interval=%s, autoRestart=%v, items=%d)", interval, s.cfg.AutoRestart, len(s.cfg.Items))
	s.syncOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("secretsync: stopped")
			return
		case <-t.C:
			s.syncOnce(ctx)
		}
	}
}

// SyncNow triggers an immediate sync (used by the "Sync now" API). It shares the
// same serialization as the ticker, so a manual trigger cannot overlap a tick.
func (s *Syncer) SyncNow(ctx context.Context) { s.syncOnce(ctx) }

func (s *Syncer) syncOnce(ctx context.Context) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	s.setRunning(true)
	defer s.setRunning(false)

	statuses := make([]ItemStatus, 0, len(s.cfg.Items))
	token, err := s.bao.login()
	if err != nil {
		log.Printf("secretsync: openbao login failed: %v", err)
		for _, item := range s.cfg.Items {
			statuses = append(statuses, ItemStatus{SecretName: item.SecretName, KVPath: item.KVPath,
				RestartDeployment: item.RestartDeployment, Error: "openbao login: " + err.Error()})
		}
		s.recordSync(statuses)
		return
	}

	// Phase 1: sync every Secret. Accumulate — PER TARGET FlinkDeployment — the
	// merged key/value set of ALL its secrets, so a job consuming several secrets
	// (flink-s3-credentials + flink-job-env) is evaluated and restarted exactly
	// ONCE per cycle, only when its combined content changes. Keys are namespaced
	// by secret name to avoid cross-secret collisions in the merged map.
	merged := make(map[string]map[string]string)
	failed := make(map[string]bool)
	for _, item := range s.cfg.Items {
		st := ItemStatus{SecretName: item.SecretName, KVPath: item.KVPath, RestartDeployment: item.RestartDeployment}
		data, err := s.bao.readKV(token, item.KVPath)
		if err != nil {
			log.Printf("secretsync: read %s: %v", item.KVPath, err)
			st.Error = "read: " + err.Error()
			statuses = append(statuses, st)
			if item.RestartDeployment != "" {
				failed[item.RestartDeployment] = true
			}
			continue
		}
		byteData := make(map[string][]byte, len(data))
		for k, v := range data {
			byteData[k] = []byte(v)
		}
		if err := s.acc.ApplySecret(ctx, item.SecretName, byteData); err != nil {
			log.Printf("secretsync: apply secret %s: %v", item.SecretName, err)
			st.Error = "apply: " + err.Error()
			statuses = append(statuses, st)
			if item.RestartDeployment != "" {
				failed[item.RestartDeployment] = true
			}
			continue
		}
		st.OK = true
		st.Keys = len(data)
		statuses = append(statuses, st)
		if s.cfg.AutoRestart && item.RestartDeployment != "" {
			m := merged[item.RestartDeployment]
			if m == nil {
				m = make(map[string]string)
				merged[item.RestartDeployment] = m
			}
			for k, v := range data {
				m[item.SecretName+"/"+k] = v
			}
		}
	}

	// Phase 2: at most one restart per deployment, when its combined hash changed.
	for dep, m := range merged {
		if failed[dep] {
			log.Printf("secretsync: %s: skipping restart (some secret failed to sync this cycle)", dep)
			continue
		}
		if err := s.maybeRestart(ctx, dep, hashData(m)); err != nil {
			log.Printf("secretsync: restart %s: %v", dep, err)
		}
	}
	s.recordSync(statuses)
}

func (s *Syncer) setRunning(v bool) {
	s.mu.Lock()
	s.running = v
	s.mu.Unlock()
}

func (s *Syncer) recordSync(items []ItemStatus) {
	s.mu.Lock()
	s.lastSync = time.Now()
	s.items = items
	s.mu.Unlock()
}

// Status returns a snapshot of the loop for the UI/API.
func (s *Syncer) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	restarts := make(map[string]int, len(s.restarts))
	for k, v := range s.restarts {
		restarts[k] = v
	}
	var skipped map[string]string
	if len(s.skipped) > 0 {
		skipped = make(map[string]string, len(s.skipped))
		for k, v := range s.skipped {
			skipped[k] = v
		}
	}
	var last int64
	if !s.lastSync.IsZero() {
		last = s.lastSync.Unix()
	}
	items := make([]ItemStatus, len(s.items))
	copy(items, s.items)
	return Status{
		Enabled:      true,
		AutoRestart:  s.cfg.AutoRestart,
		IntervalSec:  s.cfg.IntervalSec,
		LastSyncUnix: last,
		Running:      s.running,
		Items:        items,
		Restarts:     restarts,
		Skipped:      skipped,
	}
}

// maybeRestart bumps the deployment's restartNonce (once) only if the recorded
// value-hash annotation differs from h.
func (s *Syncer) maybeRestart(ctx context.Context, dep, h string) error {
	u, err := s.acc.GetFlinkDeployment(ctx, dep)
	if err != nil {
		return err
	}
	if u.GetAnnotations()[hashAnnotation] == h {
		return nil // unchanged; no restart
	}
	// HA interlock: skip (do NOT update the hash annotation) when a guard vetoes,
	// so the restart is retried on a later cycle once it becomes safe. The Secret
	// itself was already updated in phase 1; a standby job picks it up on Resume.
	if s.guard != nil {
		if ok, reason := s.guard.SafeToRestart(ctx, dep); !ok {
			s.mu.Lock()
			s.skipped[dep] = reason
			s.mu.Unlock()
			log.Printf("secretsync: %s: restart skipped by guard: %s", dep, reason)
			return nil
		}
	}
	patch, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]string{hashAnnotation: h}},
		"spec":     map[string]any{"restartNonce": nextNonce()},
	})
	if err := s.acc.PatchFlinkDeployment(ctx, dep, patch); err != nil {
		return err
	}
	s.mu.Lock()
	s.restarts[dep]++
	delete(s.skipped, dep)
	s.mu.Unlock()
	log.Printf("secretsync: %s: secret content changed -> restart triggered (hash=%s)", dep, h)
	return nil
}

func hashData(data map[string]string) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(data[k]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
