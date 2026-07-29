package secretsync

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/fko-demo/flinkui/internal/config"
)

// deploymentCache lazily lists FlinkDeployments once per sync cycle and caches
// the result (and any error), so the "all" / "references" modes across several
// items share a single List call.
type deploymentCache struct {
	ctx    context.Context
	acc    Accessor
	loaded bool
	items  []unstructured.Unstructured
	err    error
}

func newDeploymentCache(ctx context.Context, acc Accessor) *deploymentCache {
	return &deploymentCache{ctx: ctx, acc: acc}
}

func (c *deploymentCache) list() ([]unstructured.Unstructured, error) {
	if !c.loaded {
		c.loaded = true
		list, err := c.acc.ListFlinkDeployments(c.ctx)
		if err != nil {
			c.err = err
		} else if list != nil {
			c.items = list.Items
		}
	}
	return c.items, c.err
}

// restartTargets resolves the set of FlinkDeployment names a secret change
// should restart, per the item's RestartSpec. The result is sorted and
// de-duplicated for stable status output and merge keys.
func restartTargets(item config.SecretSyncItem, cache *deploymentCache) ([]string, error) {
	r := item.Restart
	switch r.ResolvedMode() {
	case "list":
		return dedupeSorted(r.Deployments), nil
	case config.RestartModeNone:
		return nil, nil
	case config.RestartModeAll:
		crs, err := cache.list()
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(crs))
		for i := range crs {
			names = append(names, crs[i].GetName())
		}
		return dedupeSorted(names), nil
	case config.RestartModeReferences:
		crs, err := cache.list()
		if err != nil {
			return nil, err
		}
		var names []string
		for i := range crs {
			if deploymentReferencesSecret(&crs[i], item.SecretName) {
				names = append(names, crs[i].GetName())
			}
		}
		return dedupeSorted(names), nil
	default:
		return nil, fmt.Errorf("unknown restart mode %q", r.Mode)
	}
}

// deploymentReferencesSecret reports whether a FlinkDeployment's podTemplate
// consumes the named Secret via envFrom, env valueFrom.secretKeyRef, or a
// secret-backed volume (init and main containers both checked).
func deploymentReferencesSecret(u *unstructured.Unstructured, secretName string) bool {
	spec, found, _ := unstructured.NestedMap(u.Object, "spec", "podTemplate", "spec")
	if !found {
		return false
	}
	for _, key := range []string{"containers", "initContainers"} {
		cs, _ := spec[key].([]interface{})
		for _, c := range cs {
			cm, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if containerReferencesSecret(cm, secretName) {
				return true
			}
		}
	}
	// Secret-backed volumes.
	vols, _ := spec["volumes"].([]interface{})
	for _, v := range vols {
		vm, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		if sec, ok := vm["secret"].(map[string]interface{}); ok {
			if name, _ := sec["secretName"].(string); name == secretName {
				return true
			}
		}
	}
	return false
}

func containerReferencesSecret(c map[string]interface{}, secretName string) bool {
	// envFrom[].secretRef.name
	if ef, ok := c["envFrom"].([]interface{}); ok {
		for _, e := range ef {
			em, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			if sr, ok := em["secretRef"].(map[string]interface{}); ok {
				if name, _ := sr["name"].(string); name == secretName {
					return true
				}
			}
		}
	}
	// env[].valueFrom.secretKeyRef.name
	if env, ok := c["env"].([]interface{}); ok {
		for _, e := range env {
			em, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			vf, ok := em["valueFrom"].(map[string]interface{})
			if !ok {
				continue
			}
			if skr, ok := vf["secretKeyRef"].(map[string]interface{}); ok {
				if name, _ := skr["name"].(string); name == secretName {
					return true
				}
			}
		}
	}
	return false
}

func dedupeSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
