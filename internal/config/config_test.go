package config

import "testing"

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
