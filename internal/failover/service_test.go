package failover

import (
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
)

func testGroup() config.HAGroupConfig {
	return config.HAGroupConfig{
		Name:         "g",
		NeutralToken: "__switching__",
		Primary:      config.SideConfig{ClusterID: "cluster-a"},
		Standby:      config.SideConfig{ClusterID: "cluster-b"},
	}
}

func TestClassifyToken(t *testing.T) {
	g := testGroup()
	cases := map[string]string{
		"":              PointsUnset,
		"__switching__": PointsNeutral,
		"cluster-a":     PointsPrimary,
		"cluster-b":     PointsStandby,
		"something":     PointsUnknown,
	}
	for tok, want := range cases {
		if got := classifyToken(tok, g); got != want {
			t.Errorf("classifyToken(%q) = %q, want %q", tok, got, want)
		}
	}
}

func detail(healthy bool) *flink.JobDetail {
	st := "SUSPENDED/—"
	if healthy {
		st = "RUNNING/STABLE"
	}
	return &flink.JobDetail{JobSummary: flink.JobSummary{StatusText: st, Healthy: healthy, Reachable: true}}
}

func TestDeriveActiveAndSplitBrain(t *testing.T) {
	s := &Service{}
	g := testGroup()

	tests := []struct {
		name        string
		pHealthy    bool
		sHealthy    bool
		pointsTo    string
		wantActive  string
		wantSplit   bool
	}{
		{"normal primary active", true, false, PointsPrimary, ActivePrimary, false},
		{"after failover standby active", false, true, PointsStandby, ActiveStandby, false},
		{"switching neutral", false, false, PointsNeutral, ActiveNone, false},
		{"split brain both running", true, true, PointsPrimary, ActivePrimary, true},
		{"infer primary when token unknown", true, false, PointsUnknown, ActivePrimary, false},
		{"infer standby when token unset", false, true, PointsUnset, ActiveStandby, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &GroupView{
				Primary: SideView{Detail: detail(tt.pHealthy)},
				Standby: SideView{Detail: detail(tt.sHealthy)},
				Fencing: FencingState{PointsTo: tt.pointsTo},
			}
			s.deriveActiveAndSplitBrain(v, g)
			if v.ActiveSide != tt.wantActive {
				t.Errorf("ActiveSide = %q, want %q", v.ActiveSide, tt.wantActive)
			}
			if v.SplitBrain != tt.wantSplit {
				t.Errorf("SplitBrain = %v, want %v", v.SplitBrain, tt.wantSplit)
			}
			if tt.wantSplit && v.Warning == "" {
				t.Error("expected a split-brain warning")
			}
		})
	}
}
