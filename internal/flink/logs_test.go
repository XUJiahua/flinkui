package flink

import (
	"context"
	"testing"

	"github.com/fko-demo/flinkui/internal/cluster"
	"github.com/fko-demo/flinkui/internal/config"
)

// selectorCapturingAccessor records the label selector passed to PodLogs so we
// can assert the component→selector mapping. Only the methods used by Logs are
// meaningful; the rest satisfy the interface.
type selectorCapturingAccessor struct {
	cluster.ClusterAccessor
	lastSelector  string
	lastContainer string
	lastTail      int64
}

func (a *selectorCapturingAccessor) PodLogs(_ context.Context, selector, container string, tail int64) (string, error) {
	a.lastSelector = selector
	a.lastContainer = container
	a.lastTail = tail
	return "line", nil
}

func TestLogsComponentSelector(t *testing.T) {
	acc := &selectorCapturingAccessor{}
	svc := NewService(acc, &config.Config{DeploymentPrefix: "flink-sql-job-", LogTailLines: 200})

	tests := []struct {
		name         string
		component    string
		wantSelector string
	}{
		{"jobmanager", "jobmanager", "app=flink-sql-job-demo,component=jobmanager"},
		{"taskmanager", "taskmanager", "app=flink-sql-job-demo,component=taskmanager"},
		{"empty defaults to jobmanager", "", "app=flink-sql-job-demo,component=jobmanager"},
		{"unknown defaults to jobmanager", "sidecar", "app=flink-sql-job-demo,component=jobmanager"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := svc.Logs(context.Background(), "demo", tt.component, 0); err != nil {
				t.Fatalf("Logs: %v", err)
			}
			if acc.lastSelector != tt.wantSelector {
				t.Errorf("selector = %q, want %q", acc.lastSelector, tt.wantSelector)
			}
			if acc.lastContainer != "flink-main-container" {
				t.Errorf("container = %q, want flink-main-container", acc.lastContainer)
			}
			if acc.lastTail != 200 {
				t.Errorf("tail = %d, want 200 (default from LogTailLines)", acc.lastTail)
			}
		})
	}
}

func TestNormalizeComponent(t *testing.T) {
	cases := map[string]string{
		"jobmanager":  "jobmanager",
		"taskmanager": "taskmanager",
		"":            "jobmanager",
		"bogus":       "jobmanager",
	}
	for in, want := range cases {
		if got := normalizeComponent(in); got != want {
			t.Errorf("normalizeComponent(%q) = %q, want %q", in, got, want)
		}
	}
}
