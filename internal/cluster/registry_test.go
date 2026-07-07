package cluster

import (
	"strings"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
)

func TestRegistryUnknownCluster(t *testing.T) {
	r := NewRegistry(&config.Config{})
	_, err := r.AccessorFor("does-not-exist", "ns")
	if err == nil || !strings.Contains(err.Error(), "unknown cluster") {
		t.Fatalf("expected unknown cluster error, got %v", err)
	}
}
