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

func TestLoadClustersAndHAGroups(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "config.yaml")
	yaml := `
addr: ":9090"
clusters:
  cluster-a:
    kubeconfig: /etc/kube/a.yaml
    s3: { endpoint: "https://minio:9000", bucket: flink, access_key: ak, secret_key: sk, insecure: true }
  cluster-b:
    kubeconfig: /etc/kube/b.yaml
ha_groups:
  - name: codes
    s3_cluster: cluster-a
    primary: { cluster: cluster-a, namespace: flink-jobs, deployment: flink-sql-job-codes, cluster_id: cluster-a }
    standby: { cluster: cluster-b, namespace: flink-jobs, deployment: flink-sql-job-codes, cluster_id: cluster-b }
  - name: withfencing
    fencing_key: custom/key
    neutral_token: "__x__"
    primary: { cluster: cluster-a, namespace: ns1, deployment: d, cluster_id: a }
    standby: { cluster: cluster-a, namespace: ns2, deployment: d, cluster_id: b }
`
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr != ":9090" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if len(cfg.Clusters) != 2 {
		t.Fatalf("clusters = %d, want 2", len(cfg.Clusters))
	}
	ca, ok := cfg.ClusterByName("cluster-a")
	if !ok || ca.Kubeconfig != "/etc/kube/a.yaml" || ca.S3.Bucket != "flink" || !ca.S3.Insecure {
		t.Errorf("cluster-a resolved wrong: %+v ok=%v", ca, ok)
	}
	if len(cfg.HAGroups) != 2 {
		t.Fatalf("haGroups = %d, want 2", len(cfg.HAGroups))
	}
	g, ok := cfg.HAGroupByName("codes")
	if !ok {
		t.Fatal("HAGroupByName(codes) not found")
	}
	// Defaults applied.
	if g.FencingKey != DefaultFencingKey || g.NeutralToken != DefaultNeutralToken {
		t.Errorf("defaults not applied: key=%q token=%q", g.FencingKey, g.NeutralToken)
	}
	if g.Primary.ClusterID != "cluster-a" || g.Standby.Cluster != "cluster-b" {
		t.Errorf("group sides wrong: %+v", g)
	}
	// Explicit fencing overrides kept.
	g2, _ := cfg.HAGroupByName("withfencing")
	if g2.FencingKey != "custom/key" || g2.NeutralToken != "__x__" {
		t.Errorf("explicit fencing overridden: %+v", g2)
	}
	// Single-cluster two-namespace form: same cluster both sides.
	if g2.Primary.Cluster != g2.Standby.Cluster {
		t.Errorf("expected same cluster both sides")
	}
}
