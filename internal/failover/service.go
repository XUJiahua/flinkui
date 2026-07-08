package failover

import (
	"context"
	"fmt"
	"sync"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
)

// Service observes and switches decentralized HA groups. It only ever touches
// the LOCAL cluster; coordination is via the shared-S3 Coord store.
type Service struct {
	cfg   *config.Config
	coord *store.Coord // shared-S3 fencing token + handoff (may be nil if S3 unset)
	recov *store.Store // shared-S3 recovery-point lister (may be nil)

	mu    sync.Mutex
	accs  map[string]cluster.ClusterAccessor // local accessor per namespace
	tasks *taskStore
}

// NewService builds the decentralized failover service. coord/recov may be nil
// when S3 is not configured (observation still works for the local side/token=unset).
func NewService(cfg *config.Config, coord *store.Coord, recov *store.Store) *Service {
	return &Service{
		cfg:   cfg,
		coord: coord,
		recov: recov,
		accs:  map[string]cluster.ClusterAccessor{},
		tasks: newTaskStore(),
	}
}

// localAccessor lazily builds (and caches) a LOCAL-cluster accessor bound to a
// namespace, using the single-cluster kubeconfig (empty => in-cluster).
func (s *Service) localAccessor(ns string) (cluster.ClusterAccessor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.accs[ns]; ok {
		return a, nil
	}
	a, err := cluster.NewKubeAccessor(s.cfg.Cluster.Name, ns, s.cfg.Cluster.Kubeconfig, s.cfg.Cluster.Context)
	if err != nil {
		return nil, err
	}
	s.accs[ns] = a
	return a, nil
}

// Groups returns the declared HA group names.
func (s *Service) Groups() []string {
	gs := s.groupConfigs(context.Background())
	out := make([]string, 0, len(gs))
	for _, g := range gs {
		out = append(out, g.Name)
	}
	return out
}

// autoGroups lists local FlinkDeployments and synthesizes normalized HA groups
// for them (used when ha.auto_all is set). Name = JobName(deployment).
func (s *Service) autoGroups(ctx context.Context) []config.LocalHAGroup {
	acc, err := s.localAccessor(s.cfg.Cluster.Namespace)
	if err != nil {
		return nil
	}
	list, err := acc.ListFlinkDeployments(ctx)
	if err != nil {
		return nil
	}
	out := make([]config.LocalHAGroup, 0, len(list.Items))
	for i := range list.Items {
		dep := list.Items[i].GetName()
		out = append(out, s.cfg.NormalizeGroup(config.LocalHAGroup{
			Name: s.cfg.JobName(dep), Deployment: dep,
		}))
	}
	return out
}

// groupConfigs returns the effective HA groups: explicit declarations plus, when
// ha.auto_all is set, every local FlinkDeployment (explicit wins by name).
func (s *Service) groupConfigs(ctx context.Context) []config.LocalHAGroup {
	seen := map[string]bool{}
	out := make([]config.LocalHAGroup, 0, len(s.cfg.HA.Groups))
	for _, g := range s.cfg.HA.Groups {
		out = append(out, g)
		seen[g.Name] = true
	}
	if s.cfg.HA.AutoAll {
		for _, g := range s.autoGroups(ctx) {
			if !seen[g.Name] {
				out = append(out, g)
				seen[g.Name] = true
			}
		}
	}
	return out
}

// resolveGroup finds a group config by name: explicit first, else (auto_all) a
// local deployment whose JobName matches.
func (s *Service) resolveGroup(ctx context.Context, name string) (config.LocalHAGroup, bool) {
	if g, ok := s.cfg.HAGroupByName(name); ok {
		return g, true
	}
	if s.cfg.HA.AutoAll {
		for _, g := range s.autoGroups(ctx) {
			if g.Name == name {
				return g, true
			}
		}
	}
	return config.LocalHAGroup{}, false
}

// LocalView observes a single HA group from this instance's point of view.
func (s *Service) LocalView(ctx context.Context, name string) (*LocalView, error) {
	g, ok := s.resolveGroup(ctx, name)
	if !ok {
		return nil, fmt.Errorf("HA group %q not found", name)
	}

	v := &LocalView{
		Name:          g.Name,
		ClusterID:     g.ClusterID,
		PeerClusterID: g.PeerClusterID,
		Namespace:     g.Namespace,
		Deployment:    g.Deployment,
		Local:         s.localStatus(ctx, g),
		Fencing:       s.fencingState(ctx, g),
	}
	v.Handoff = s.readHandoff(ctx, g)
	s.deriveRole(v, g)
	return v, nil
}

// ListViews observes all effective HA groups (explicit + auto_all).
func (s *Service) ListViews(ctx context.Context) ([]*LocalView, error) {
	gs := s.groupConfigs(ctx)
	out := make([]*LocalView, 0, len(gs))
	for _, g := range gs {
		v := &LocalView{
			Name:          g.Name,
			ClusterID:     g.ClusterID,
			PeerClusterID: g.PeerClusterID,
			Namespace:     g.Namespace,
			Deployment:    g.Deployment,
			Local:         s.localStatus(ctx, g),
			Fencing:       s.fencingState(ctx, g),
		}
		v.Handoff = s.readHandoff(ctx, g)
		s.deriveRole(v, g)
		out = append(out, v)
	}
	return out, nil
}

// localStatus fetches the local deployment status (unreachable degrades gracefully).
func (s *Service) localStatus(ctx context.Context, g config.LocalHAGroup) *flink.JobDetail {
	acc, err := s.localAccessor(g.Namespace)
	if err != nil {
		return unreachable(g)
	}
	d, err := flink.NewService(acc, s.cfg).Get(ctx, g.Deployment)
	if err != nil {
		return unreachable(g)
	}
	return d
}

func unreachable(g config.LocalHAGroup) *flink.JobDetail {
	return &flink.JobDetail{JobSummary: flink.JobSummary{
		Namespace:  g.Namespace,
		Deployment: g.Deployment,
		StatusText: flink.StatusUnreachable,
		Health:     flink.HealthUnreachable,
		Reachable:  false,
	}}
}

// fencingState reads the shared token and classifies it relative to this side.
func (s *Service) fencingState(ctx context.Context, g config.LocalHAGroup) FencingState {
	if s.coord == nil {
		return FencingState{PointsTo: PointsUnknown, Error: "S3 coordination not configured"}
	}
	token, err := s.coord.ReadToken(ctx, g.FencingKey)
	if err != nil {
		return FencingState{PointsTo: PointsUnknown, Error: err.Error()}
	}
	return FencingState{Token: token, PointsTo: classifyToken(token, g)}
}

func classifyToken(token string, g config.LocalHAGroup) string {
	switch token {
	case "":
		return PointsUnset
	case g.NeutralToken:
		return PointsNeutral
	case g.ClusterID:
		return PointsSelf
	case g.PeerClusterID:
		return PointsPeer
	default:
		return PointsUnknown
	}
}

func (s *Service) readHandoff(ctx context.Context, g config.LocalHAGroup) *store.HandoffRecord {
	if s.coord == nil {
		return nil
	}
	rec, ok, err := s.coord.ReadHandoff(ctx, g.HandoffKey)
	if err != nil || !ok {
		return nil
	}
	return rec
}

// deriveRole sets Role and a local-inconsistency Warning from the token and the
// local job state (the peer side is not observed cross-cluster).
func (s *Service) deriveRole(v *LocalView, g config.LocalHAGroup) {
	localRunning := v.Local != nil && v.Local.Healthy
	switch v.Fencing.PointsTo {
	case PointsSelf:
		v.Role = RoleActive
		if !localRunning {
			v.Warning = "fencing token points here but the local job is not RUNNING/STABLE (consider Promote/Resume)"
		}
	case PointsPeer:
		v.Role = RoleStandby
		if localRunning {
			v.Warning = "fencing token points to the peer but the local job is RUNNING — it should be stopped (split-brain risk)"
		}
	case PointsNeutral:
		v.Role = RoleNeutral
		if localRunning {
			v.Warning = "a switch is in progress (neutral token) but the local job is still RUNNING"
		}
	default:
		v.Role = RoleUnknown
		if v.Fencing.PointsTo == PointsUnset && localRunning {
			v.Warning = "fencing token is unset while the local job runs"
		} else if v.Fencing.Error != "" {
			v.Warning = "fencing token unreadable: " + v.Fencing.Error
		}
	}
}
