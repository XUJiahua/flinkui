package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeploymentNameAndJobName(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		job        string
		wantDep    string
		wantJob    string // JobName(wantDep)
	}{
		{"default prefix, short job", "flink-sql-job-", "codes", "flink-sql-job-codes", "codes"},
		{"already prefixed", "flink-sql-job-", "flink-sql-job-codes", "flink-sql-job-codes", "codes"},
		{"empty prefix is identity", "", "codes", "codes", "codes"},
		{"custom prefix", "job-", "abc", "job-abc", "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{DeploymentPrefix: tt.prefix}
			if got := c.DeploymentName(tt.job); got != tt.wantDep {
				t.Errorf("DeploymentName(%q) = %q, want %q", tt.job, got, tt.wantDep)
			}
			if got := c.JobName(tt.wantDep); got != tt.wantJob {
				t.Errorf("JobName(%q) = %q, want %q", tt.wantDep, got, tt.wantJob)
			}
		})
	}
}

func TestLoadDefaultsAndEnvBinding(t *testing.T) {
	// Defaults when no env / file.
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("default Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.DeploymentPrefix != "flink-sql-job-" {
		t.Errorf("default DeploymentPrefix = %q", cfg.DeploymentPrefix)
	}
	if !cfg.Cluster.S3.PathStyle {
		t.Errorf("default S3.PathStyle = false, want true")
	}

	// Nested env binding must take effect (regression: viper AutomaticEnv alone
	// does not resolve nested keys without BindEnv).
	t.Setenv("FKO_CLUSTER_KUBECONFIG", "/tmp/kc.yaml")
	t.Setenv("FKO_CLUSTER_NAMESPACE", "flink-jobs")
	t.Setenv("FKO_AUTH_PASSWORD", "s3cret")
	t.Setenv("FKO_CLUSTER_S3_INSECURE", "true")
	cfg, err = Load("")
	if err != nil {
		t.Fatalf("Load with env: %v", err)
	}
	if cfg.Cluster.Kubeconfig != "/tmp/kc.yaml" {
		t.Errorf("Kubeconfig = %q, want /tmp/kc.yaml", cfg.Cluster.Kubeconfig)
	}
	if cfg.Cluster.Namespace != "flink-jobs" {
		t.Errorf("Namespace = %q, want flink-jobs", cfg.Cluster.Namespace)
	}
	if cfg.Auth.Password != "s3cret" {
		t.Errorf("Auth.Password = %q, want s3cret", cfg.Auth.Password)
	}
	if !cfg.Cluster.S3.Insecure {
		t.Errorf("S3.Insecure = false, want true")
	}
}

func TestLoadHAGroups(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "config.yaml")
	yaml := `
cluster:
  name: cluster-a
  namespace: flink-jobs
ha:
  groups:
    - name: orders
      namespace: flink-jobs
      deployment: flink-sql-job-orders
      cluster_id: cluster-a
      peer_cluster_id: cluster-b
    - name: custom
      namespace: ns
      deployment: d
      cluster_id: a
      peer_cluster_id: b
      fencing_key: fencing/custom
      neutral_token: "__x__"
      handoff_key: h/custom
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.HA.Groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(cfg.HA.Groups))
	}
	g, ok := cfg.HAGroupByName("orders")
	if !ok {
		t.Fatal("orders not found")
	}
	// defaults applied
	if g.FencingKey != "fencing/orders/active-cluster" || g.NeutralToken != DefaultNeutralToken {
		t.Errorf("defaults not applied: %+v", g)
	}
	if g.HandoffKey != "fencing/handoff/orders" {
		t.Errorf("handoff default = %q", g.HandoffKey)
	}
	if g.ClusterID != "cluster-a" || g.PeerClusterID != "cluster-b" || g.Deployment != "flink-sql-job-orders" {
		t.Errorf("orders fields wrong: %+v", g)
	}
	// explicit overrides kept
	g2, _ := cfg.HAGroupByName("custom")
	if g2.FencingKey != "fencing/custom" || g2.NeutralToken != "__x__" || g2.HandoffKey != "h/custom" {
		t.Errorf("overrides lost: %+v", g2)
	}
}

func TestHAGroupDefaultsFromInstanceLevel(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "c.yaml")
	// Only `name` per group; identities come from ha.self/default_peer.
	yaml := `
cluster: { name: cluster-a, namespace: flink-jobs }
deployment_prefix: "flink-sql-job-"
ha:
  self_cluster_id: cluster-a
  default_peer_cluster_id: cluster-b
  groups:
    - name: codes
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	g, _ := cfg.HAGroupByName("codes")
	if g.Namespace != "flink-jobs" {
		t.Errorf("namespace default = %q", g.Namespace)
	}
	if g.Deployment != "flink-sql-job-codes" {
		t.Errorf("deployment default = %q", g.Deployment)
	}
	if g.ClusterID != "cluster-a" || g.PeerClusterID != "cluster-b" {
		t.Errorf("identity defaults wrong: %+v", g)
	}
	if g.HandoffKey != "fencing/handoff/codes" {
		t.Errorf("handoff default = %q", g.HandoffKey)
	}
}

func TestHAGroupMissingIdentityErrors(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "c.yaml")
	yaml := `
cluster: { name: cluster-a, namespace: flink-jobs }
ha:
  groups:
    - name: codes   # no cluster_id / peer, and no instance defaults => error
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(file); err == nil {
		t.Fatal("expected error for missing clusterId/peerClusterId")
	}
}

func TestHAAutoAllRequiresIdentities(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "c.yaml")
	yaml := `
cluster: { name: cluster-a, namespace: flink-jobs }
ha:
  auto_all: true    # without self/default_peer => error
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(file); err == nil {
		t.Fatal("expected error for auto_all without identities")
	}
}
