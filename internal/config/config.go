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

// SideConfig identifies one side (primary or standby) of an HA group: which
// named cluster, namespace, deployment, and the fencing clusterId written to
// the S3 token (design failover §3/§4).
type SideConfig struct {
	Cluster    string `mapstructure:"cluster"`
	Namespace  string `mapstructure:"namespace"`
	Deployment string `mapstructure:"deployment"`
	ClusterID  string `mapstructure:"cluster_id"`
}

// HAGroupConfig statically declares a primary/standby pair of the same logical
// job across two sides sharing one S3 (checkpoints/savepoints/fencing).
type HAGroupConfig struct {
	Name string `mapstructure:"name"`
	// S3Cluster names the cluster whose S3 config is used to read/write the
	// fencing token and list recovery points (both sides share the same S3).
	S3Cluster string `mapstructure:"s3_cluster"`
	// FencingKey is the S3 object key of the active-cluster token.
	FencingKey string `mapstructure:"fencing_key"`
	// NeutralToken fences both sides during a switch transition.
	NeutralToken string     `mapstructure:"neutral_token"`
	Primary      SideConfig `mapstructure:"primary"`
	Standby      SideConfig `mapstructure:"standby"`
}

// Default fencing settings mirroring scripts/failover.sh.
const (
	DefaultFencingKey   = "fencing/active-cluster"
	DefaultNeutralToken = "__switching__"
)

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

	// Clusters is a named pool of clusters for multi-cluster / failover use.
	// Each entry carries its own kubeconfig (empty => in-cluster) and S3 config.
	Clusters map[string]ClusterConfig `mapstructure:"clusters"`
	// HAGroups statically declares primary/standby pairs (design failover P1).
	HAGroups []HAGroupConfig `mapstructure:"ha_groups"`
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
	// Apply fencing defaults per HA group (mirrors scripts/failover.sh).
	for i := range cfg.HAGroups {
		if cfg.HAGroups[i].FencingKey == "" {
			cfg.HAGroups[i].FencingKey = DefaultFencingKey
		}
		if cfg.HAGroups[i].NeutralToken == "" {
			cfg.HAGroups[i].NeutralToken = DefaultNeutralToken
		}
	}
	return &cfg, nil
}

// ClusterByName returns a named cluster from the pool. It also resolves the
// implicit single-cluster config under its Name (or "default"/"local").
func (c *Config) ClusterByName(name string) (ClusterConfig, bool) {
	if cc, ok := c.Clusters[name]; ok {
		return cc, true
	}
	// Fall back to the implicit single cluster.
	if name == c.Cluster.Name || name == "default" || name == "local" || name == "" {
		return c.Cluster, true
	}
	return ClusterConfig{}, false
}

// HAGroupByName returns a declared HA group by name.
func (c *Config) HAGroupByName(name string) (HAGroupConfig, bool) {
	for _, g := range c.HAGroups {
		if g.Name == name {
			return g, true
		}
	}
	return HAGroupConfig{}, false
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
