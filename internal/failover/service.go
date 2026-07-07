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

// Service observes HA groups: it reads both sides' status through the cluster
// registry and the fencing token from S3, and derives the active side and any
// split-brain warning.
type Service struct {
	cfg *config.Config
	reg *cluster.Registry

	mu      sync.Mutex
	fencing map[string]*store.FencingStore // key: group name
}

// NewService builds the failover service.
func NewService(cfg *config.Config, reg *cluster.Registry) *Service {
	return &Service{cfg: cfg, reg: reg, fencing: map[string]*store.FencingStore{}}
}

// Groups returns the names of declared HA groups.
func (s *Service) Groups() []string {
	names := make([]string, 0, len(s.cfg.HAGroups))
	for _, g := range s.cfg.HAGroups {
		names = append(names, g.Name)
	}
	return names
}

// GroupView observes a single HA group by name.
func (s *Service) GroupView(ctx context.Context, name string) (*GroupView, error) {
	g, ok := s.cfg.HAGroupByName(name)
	if !ok {
		return nil, fmt.Errorf("HA group %q not found", name)
	}

	primary := s.sideView(ctx, "primary", g.Primary)
	standby := s.sideView(ctx, "standby", g.Standby)
	fencing := s.fencingState(ctx, g)

	view := &GroupView{
		Name:    g.Name,
		Primary: primary,
		Standby: standby,
		Fencing: fencing,
	}
	s.deriveActiveAndSplitBrain(view, g)
	return view, nil
}

// ListViews observes all declared HA groups.
func (s *Service) ListViews(ctx context.Context) ([]*GroupView, error) {
	out := make([]*GroupView, 0, len(s.cfg.HAGroups))
	for _, g := range s.cfg.HAGroups {
		v, err := s.GroupView(ctx, g.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// sideView resolves one side's identity and current status. Unreachable sides
// (missing kubeconfig, cluster down) are reported as not-reachable rather than
// failing the whole view.
func (s *Service) sideView(ctx context.Context, role string, side config.SideConfig) SideView {
	sv := SideView{
		Role:       role,
		Cluster:    side.Cluster,
		Namespace:  side.Namespace,
		Deployment: side.Deployment,
		ClusterID:  side.ClusterID,
	}
	acc, err := s.reg.AccessorFor(side.Cluster, side.Namespace)
	if err != nil {
		sv.Detail = unreachableDetail(side)
		return sv
	}
	fsvc := flink.NewService(acc, s.cfg)
	detail, err := fsvc.Get(ctx, side.Deployment)
	if err != nil {
		sv.Detail = unreachableDetail(side)
		return sv
	}
	sv.Detail = detail
	return sv
}

func unreachableDetail(side config.SideConfig) *flink.JobDetail {
	return &flink.JobDetail{JobSummary: flink.JobSummary{
		Namespace:  side.Namespace,
		Deployment: side.Deployment,
		StatusText: flink.StatusUnreachable,
		Reachable:  false,
	}}
}

// fencingState reads the S3 fencing token for the group and classifies it.
func (s *Service) fencingState(ctx context.Context, g config.HAGroupConfig) FencingState {
	fs, err := s.fencingStore(ctx, g)
	if err != nil {
		return FencingState{PointsTo: PointsUnknown, Error: err.Error()}
	}
	token, err := fs.ReadToken(ctx, g.FencingKey)
	if err != nil {
		return FencingState{PointsTo: PointsUnknown, Error: err.Error()}
	}
	return FencingState{Token: token, PointsTo: classifyToken(token, g)}
}

func classifyToken(token string, g config.HAGroupConfig) string {
	switch token {
	case "":
		return PointsUnset
	case g.NeutralToken:
		return PointsNeutral
	case g.Primary.ClusterID:
		return PointsPrimary
	case g.Standby.ClusterID:
		return PointsStandby
	default:
		return PointsUnknown
	}
}

// fencingStore lazily builds (and caches) the FencingStore for a group using
// its s3Cluster's S3 config.
func (s *Service) fencingStore(ctx context.Context, g config.HAGroupConfig) (*store.FencingStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fs, ok := s.fencing[g.Name]; ok {
		return fs, nil
	}
	cc, ok := s.cfg.ClusterByName(g.S3Cluster)
	if !ok {
		return nil, fmt.Errorf("s3Cluster %q not found for group %q", g.S3Cluster, g.Name)
	}
	fs, err := store.NewFencing(ctx, cc.S3)
	if err != nil {
		return nil, err
	}
	s.fencing[g.Name] = fs
	return fs, nil
}

// deriveActiveAndSplitBrain sets ActiveSide, SplitBrain and Warning from the
// two sides' health and the fencing pointer.
func (s *Service) deriveActiveAndSplitBrain(v *GroupView, g config.HAGroupConfig) {
	pHealthy := v.Primary.Detail != nil && v.Primary.Detail.Healthy
	sHealthy := v.Standby.Detail != nil && v.Standby.Detail.Healthy

	// Active side: trust the fencing pointer first, else infer from health.
	switch v.Fencing.PointsTo {
	case PointsPrimary:
		v.ActiveSide = ActivePrimary
	case PointsStandby:
		v.ActiveSide = ActiveStandby
	case PointsNeutral:
		v.ActiveSide = ActiveNone // switching in progress
	default:
		switch {
		case pHealthy && !sHealthy:
			v.ActiveSide = ActivePrimary
		case sHealthy && !pHealthy:
			v.ActiveSide = ActiveStandby
		default:
			v.ActiveSide = ActiveUnknown
		}
	}

	// Split-brain: both sides running at once (the fencing token exists to
	// prevent exactly this — design failover §2).
	if pHealthy && sHealthy {
		v.SplitBrain = true
		v.Warning = "both primary and standby are RUNNING/STABLE — split-brain risk"
		return
	}

	// Token/health inconsistencies (non-fatal warnings).
	switch v.Fencing.PointsTo {
	case PointsPrimary:
		if sHealthy && !pHealthy {
			v.Warning = "fencing token points to primary but standby is the running side"
		}
	case PointsStandby:
		if pHealthy && !sHealthy {
			v.Warning = "fencing token points to standby but primary is the running side"
		}
	case PointsUnset:
		if pHealthy || sHealthy {
			v.Warning = "fencing token is unset while a side is running"
		}
	case PointsUnknown:
		if v.Fencing.Error != "" {
			v.Warning = "fencing token unreadable: " + v.Fencing.Error
		} else if v.Fencing.Token != "" {
			v.Warning = "fencing token value does not match either side's clusterId"
		}
	}
}
