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
	// CookieSecure sets the Secure attribute on the session cookie so it is only
	// sent over HTTPS. Enable this whenever the console is served over TLS
	// (e.g. behind an ingress terminating TLS). Default false to keep plain-HTTP
	// local/dev usable.
	CookieSecure bool `mapstructure:"cookie_secure"`
}

// LocalHAGroup declares one decentralized HA group from THIS instance's point of
// view. Only `name` is required in the common case: namespace defaults to the
// console's cluster namespace, deployment to DeploymentName(name), and
// clusterId/peerClusterId to the instance-level ha.self_cluster_id /
// ha.default_peer_cluster_id. Explicit fields override those defaults.
type LocalHAGroup struct {
	Name string `mapstructure:"name"`
	// Local side (this cluster) — optional; defaulted.
	Namespace  string `mapstructure:"namespace"`
	Deployment string `mapstructure:"deployment"`
	ClusterID  string `mapstructure:"cluster_id"`
	// Peer side (other cluster) — optional; defaulted.
	PeerClusterID string `mapstructure:"peer_cluster_id"`
	// Shared-S3 coordination keys — optional; defaulted.
	FencingKey   string `mapstructure:"fencing_key"`
	NeutralToken string `mapstructure:"neutral_token"`
	HandoffKey   string `mapstructure:"handoff_key"`
}

// HAConfig groups the decentralized HA declarations.
type HAConfig struct {
	// SelfClusterID is this instance's fencing identity (token value when active).
	SelfClusterID string `mapstructure:"self_cluster_id"`
	// DefaultPeerClusterID is the peer clusterId applied to groups that don't set one.
	DefaultPeerClusterID string `mapstructure:"default_peer_cluster_id"`
	// AutoAll, when true, treats every FlinkDeployment in the cluster namespace as
	// an HA group (resolved at runtime), using the instance-level defaults. Groups
	// listed explicitly still apply (and override per name).
	AutoAll bool           `mapstructure:"auto_all"`
	Groups  []LocalHAGroup `mapstructure:"groups"`
}

// Default fencing/handoff settings mirroring scripts/failover.sh. The fencing
// key is per-group (fencing/<group>/active-cluster) so groups fail over
// independently; the neutral token fences a group during its own switch.
const (
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

	// AllowedOrigins is a comma-separated allowlist of extra browser origins
	// (scheme://host[:port]) permitted to open the status WebSocket, in addition
	// to same-origin requests which are always allowed. Set this when the SPA is
	// served from a different origin than the API (e.g. a separate dev host).
	AllowedOrigins string `mapstructure:"allowed_origins"`

	Auth    AuthConfig    `mapstructure:"auth"`
	Cluster ClusterConfig `mapstructure:"cluster"`

	// HA declares decentralized primary/standby groups this instance participates
	// in (design failover-decentralized). Empty => failover UI disabled.
	HA HAConfig `mapstructure:"ha"`
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
		"log_tail_lines", "status_poll_sec", "allowed_origins",
		"cluster.name", "cluster.namespace", "cluster.kubeconfig", "cluster.context",
		"cluster.s3.endpoint", "cluster.s3.bucket", "cluster.s3.access_key",
		"cluster.s3.secret_key", "cluster.s3.region", "cluster.s3.path_style",
		"cluster.s3.insecure",
		"auth.username", "auth.password", "auth.session_secret", "auth.cookie_secure",
		"ha.self_cluster_id", "ha.default_peer_cluster_id", "ha.auto_all",
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
	// Normalize + validate explicitly-declared HA groups.
	for i := range cfg.HA.Groups {
		cfg.HA.Groups[i] = cfg.NormalizeGroup(cfg.HA.Groups[i])
		g := cfg.HA.Groups[i]
		if g.ClusterID == "" || g.PeerClusterID == "" {
			return nil, fmt.Errorf("ha.groups[%q]: clusterId and peerClusterId are required "+
				"(set them on the group or via ha.self_cluster_id / ha.default_peer_cluster_id)", g.Name)
		}
	}
	if cfg.HA.AutoAll && (cfg.HA.SelfClusterID == "" || cfg.HA.DefaultPeerClusterID == "") {
		return nil, fmt.Errorf("ha.auto_all requires ha.self_cluster_id and ha.default_peer_cluster_id")
	}
	return &cfg, nil
}

// NormalizeGroup fills a group's optional fields from instance-level defaults and
// naming conventions: namespace <- cluster.namespace, deployment <-
// DeploymentName(name), clusterId <- ha.self_cluster_id, peerClusterId <-
// ha.default_peer_cluster_id, and fencing/neutral/handoff keys to defaults.
func (c *Config) NormalizeGroup(g LocalHAGroup) LocalHAGroup {
	if g.Namespace == "" {
		g.Namespace = c.Cluster.Namespace
	}
	if g.Deployment == "" {
		g.Deployment = c.DeploymentName(g.Name)
	}
	if g.ClusterID == "" {
		g.ClusterID = c.HA.SelfClusterID
	}
	if g.PeerClusterID == "" {
		g.PeerClusterID = c.HA.DefaultPeerClusterID
	}
	if g.FencingKey == "" {
		// Per-group by default so Releasing one group does not flip a shared
		// token for all groups. MUST match the key the job's fencing
		// initContainer reads (see docs/failover-decentralized-design.md §3).
		g.FencingKey = "fencing/" + g.Name + "/active-cluster"
	}
	if g.NeutralToken == "" {
		g.NeutralToken = DefaultNeutralToken
	}
	if g.HandoffKey == "" {
		// Co-located with the token under fencing/<group>/ for consistency and
		// easy per-job cleanup (fencing/<group>/active-cluster + /handoff).
		g.HandoffKey = "fencing/" + g.Name + "/handoff"
	}
	return g
}

// AllowedOriginList parses the comma-separated AllowedOrigins into a trimmed,
// lower-cased slice (empty entries dropped).
func (c *Config) AllowedOriginList() []string {
	var out []string
	for _, o := range strings.Split(c.AllowedOrigins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, strings.ToLower(o))
		}
	}
	return out
}

// HAGroupByName returns a declared local HA group by name.
func (c *Config) HAGroupByName(name string) (LocalHAGroup, bool) {
	for _, g := range c.HA.Groups {
		if g.Name == name {
			return g, true
		}
	}
	return LocalHAGroup{}, false
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
