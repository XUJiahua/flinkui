// Package config loads platform configuration from environment variables and
// an optional config file (via viper). It models a "cluster list" even though
// the MVP only wires a single cluster, leaving room for multi-cluster later.
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// ClusterConfig describes how to reach a single target Kubernetes cluster.
type ClusterConfig struct {
	// Name is a stable identifier for the cluster (used in API paths / UI).
	Name string `mapstructure:"name"`
	// Namespace is the namespace that holds the FlinkDeployment resources.
	Namespace string `mapstructure:"namespace"`
	// Kubeconfig is the path to a kubeconfig file. Empty => in-cluster config.
	Kubeconfig string `mapstructure:"kubeconfig"`
	// Context optionally selects a context inside the kubeconfig file.
	Context string `mapstructure:"context"`

	// S3 credentials/endpoint for listing savepoints & checkpoints.
	S3 S3Config `mapstructure:"s3"`
}

// S3Config holds S3/MinIO connection info for recovery-point listing.
type S3Config struct {
	Endpoint  string `mapstructure:"endpoint"`
	Bucket    string `mapstructure:"bucket"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Region    string `mapstructure:"region"`
	PathStyle bool   `mapstructure:"path_style"`
	// Insecure skips TLS certificate verification, for internal MinIO endpoints
	// that use a self-signed certificate. (TLS on/off is determined by the
	// endpoint URL scheme; there is no separate use_ssl toggle.)
	Insecure bool `mapstructure:"insecure"`
}

// AuthConfig holds basic-auth credentials guarding the whole platform.
type AuthConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	// SessionSecret signs session cookies.
	SessionSecret string `mapstructure:"session_secret"`
}

// Config is the top-level platform configuration.
type Config struct {
	// Addr is the HTTP listen address, e.g. ":8080".
	Addr string `mapstructure:"addr"`
	// DeploymentPrefix is the FlinkDeployment naming convention prefix.
	// job.sh uses "flink-sql-job-<job>"; setting the prefix to
	// "flink-sql-job-" lets the UI show short job names.
	DeploymentPrefix string `mapstructure:"deployment_prefix"`
	// SavepointTimeoutSec bounds savepoint polling.
	SavepointTimeoutSec int `mapstructure:"savepoint_timeout_sec"`
	// StopTimeoutSec bounds "wait for JM pod to reach zero" during restart.
	StopTimeoutSec int `mapstructure:"stop_timeout_sec"`
	// LogTailLines is the default number of log lines to tail.
	LogTailLines int64 `mapstructure:"log_tail_lines"`
	// StatusPollSec controls the WebSocket status push interval.
	StatusPollSec int `mapstructure:"status_poll_sec"`

	Auth    AuthConfig    `mapstructure:"auth"`
	Cluster ClusterConfig `mapstructure:"cluster"`
}

// Load reads configuration from env vars (prefix FKO_) and an optional file.
// Env example: FKO_CLUSTER_NAMESPACE, FKO_CLUSTER_S3_BUCKET, FKO_AUTH_PASSWORD.
func Load(configFile string) (*Config, error) {
	v := viper.New()

	// Defaults.
	v.SetDefault("addr", ":8080")
	v.SetDefault("deployment_prefix", "flink-sql-job-")
	v.SetDefault("savepoint_timeout_sec", 120)
	v.SetDefault("stop_timeout_sec", 120)
	v.SetDefault("log_tail_lines", 200)
	v.SetDefault("status_poll_sec", 5)
	v.SetDefault("cluster.name", "default")
	v.SetDefault("cluster.namespace", "flink-operator")
	v.SetDefault("cluster.s3.region", "us-east-1")
	v.SetDefault("cluster.s3.path_style", true)
	v.SetDefault("auth.username", "admin")
	v.SetDefault("auth.session_secret", "change-me-please")

	// Env binding: FKO_ prefix, nested keys via underscore.
	v.SetEnvPrefix("FKO")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicitly bind every key. AutomaticEnv only resolves keys that viper
	// already knows about (via defaults or a config file); nested keys such as
	// cluster.kubeconfig have no default, so bind them here to make e.g.
	// FKO_CLUSTER_KUBECONFIG and FKO_AUTH_PASSWORD take effect.
	for _, key := range []string{
		"addr", "deployment_prefix", "savepoint_timeout_sec", "stop_timeout_sec",
		"log_tail_lines", "status_poll_sec",
		"cluster.name", "cluster.namespace", "cluster.kubeconfig", "cluster.context",
		"cluster.s3.endpoint", "cluster.s3.bucket", "cluster.s3.access_key",
		"cluster.s3.secret_key", "cluster.s3.region", "cluster.s3.path_style",
		"cluster.s3.insecure",
		"auth.username", "auth.password", "auth.session_secret",
	} {
		_ = v.BindEnv(key)
	}

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config file %q: %w", configFile, err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return &cfg, nil
}

// DeploymentName maps a short job name to its FlinkDeployment resource name.
// If the job already carries the prefix (or prefix is empty) it is returned as-is.
func (c *Config) DeploymentName(job string) string {
	if c.DeploymentPrefix == "" || strings.HasPrefix(job, c.DeploymentPrefix) {
		return job
	}
	return c.DeploymentPrefix + job
}

// JobName reverses DeploymentName, stripping the prefix for display.
func (c *Config) JobName(deployment string) string {
	return strings.TrimPrefix(deployment, c.DeploymentPrefix)
}
