package cluster

import (
	"fmt"
	"sync"

	"github.com/fko-demo/flinkui/internal/config"
)

// Registry lazily builds and caches ClusterAccessors for the named cluster pool,
// keyed by (cluster, namespace). A "side" of an HA group is (cluster, namespace,
// deployment); since KubeAccessor is namespace-bound, one accessor is created per
// (cluster, namespace). This is additive to the single-cluster MVP path.
type Registry struct {
	cfg *config.Config

	mu        sync.Mutex
	accessors map[string]ClusterAccessor // key: "<cluster>/<namespace>"
}

// NewRegistry builds an empty registry backed by the config's cluster pool.
func NewRegistry(cfg *config.Config) *Registry {
	return &Registry{cfg: cfg, accessors: map[string]ClusterAccessor{}}
}

// AccessorFor returns (building and caching on first use) an accessor bound to
// the given named cluster and namespace.
func (r *Registry) AccessorFor(clusterName, namespace string) (ClusterAccessor, error) {
	key := clusterName + "/" + namespace
	r.mu.Lock()
	defer r.mu.Unlock()
	if a, ok := r.accessors[key]; ok {
		return a, nil
	}
	cc, ok := r.cfg.ClusterByName(clusterName)
	if !ok {
		return nil, fmt.Errorf("unknown cluster %q (not in clusters pool nor the single cluster)", clusterName)
	}
	acc, err := NewKubeAccessor(clusterName, namespace, cc.Kubeconfig, cc.Context)
	if err != nil {
		return nil, fmt.Errorf("build accessor for %s/%s: %w", clusterName, namespace, err)
	}
	r.accessors[key] = acc
	return acc, nil
}
