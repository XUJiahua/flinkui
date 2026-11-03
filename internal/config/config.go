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
	Endpoint string `mapstructure:"endpoint"`
	Bucket   string `mapstructure:"bucket"`
	// Prefix is an optional base key prefix inside the bucket, so several
	// flinkui instances can share one bucket and stay isolated by path
	// (e.g. prefix "tenant-a" => keys under tenant-a/...). As a convenience the
	// Bucket value itself may carry a path suffix ("flink/tenant-a"), which is
	// merged with Prefix. It only affects flinkui-owned keys (fencing/handoff
	// and the fallback savepoint/checkpoint locations); absolute s3:// dirs
	// configured on the deployment are used verbatim.
	Prefix    string `mapstructure:"prefix"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Region    string `mapstructure:"region"`
	PathStyle bool   `mapstructure:"path_style"`
	// Insecure skips TLS certificate verification, for internal MinIO endpoints
	// that use a self-signed certificate. (TLS on/off is determined by the
	// endpoint URL scheme; there is no separate use_ssl toggle.)
	Insecure bool `mapstructure:"insecure"`
}

// BucketPrefix normalizes Bucket (which may itself carry a "/path" suffix) and
// Prefix into a plain bucket name plus a cleaned base key prefix (no leading or
// trailing slash; empty when none). Examples:
//
//	{Bucket: "flink"}                          => ("flink", "")
//	{Bucket: "flink/tenant-a"}                 => ("flink", "tenant-a")
//	{Bucket: "flink", Prefix: "tenant-a/sit"}  => ("flink", "tenant-a/sit")
//	{Bucket: "flink/a", Prefix: "b"}           => ("flink", "a/b")
func (s S3Config) BucketPrefix() (bucket, prefix string) {
	b := strings.Trim(s.Bucket, "/")
	parts := strings.SplitN(b, "/", 2)
	bucket = parts[0]
	var segs []string
	if len(parts) == 2 {
		if p := strings.Trim(parts[1], "/"); p != "" {
			segs = append(segs, p)
		}
	}
	if p := strings.Trim(s.Prefix, "/"); p != "" {
		segs = append(segs, p)
	}
	return bucket, strings.Join(segs, "/")
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
	// LDAP optionally enables directory authentication as an alternative to the
	// single hard-coded Username/Password. When enabled, a login is accepted if
	// it matches the local credential OR authenticates against LDAP, so the
	// local account stays usable as a break-glass admin.
	LDAP LDAPConfig `mapstructure:"ldap"`
}

// LDAPConfig configures optional LDAP/Active Directory authentication. Two bind
// modes are supported:
//
//   - Direct bind: set UserDNTemplate (e.g. "uid=%s,ou=people,dc=example,dc=com").
//     The %s is replaced with the (DN-escaped) login name and bound directly.
//   - Search + bind: leave UserDNTemplate empty and set BaseDN + UserFilter
//     (e.g. "(uid=%s)" or "(sAMAccountName=%s)"). The console first binds with
//     BindDN/BindPassword (anonymous if BindDN is empty), searches for the user,
//     then re-binds as the found DN to verify the password.
type LDAPConfig struct {
	// Enabled turns LDAP authentication on.
	Enabled bool `mapstructure:"enabled"`
	// URL is the server URL, e.g. "ldap://ldap.example.com:389" or
	// "ldaps://ldap.example.com:636".
	URL string `mapstructure:"url"`
	// StartTLS upgrades a plain ldap:// connection to TLS after connecting.
	// Ignored for ldaps:// (already TLS).
	StartTLS bool `mapstructure:"start_tls"`
	// InsecureSkipVerify disables TLS certificate verification (ldaps/StartTLS),
	// for internal servers with a self-signed certificate.
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
	// UserDNTemplate enables direct-bind mode. "%s" is replaced with the
	// DN-escaped login name.
	UserDNTemplate string `mapstructure:"user_dn_template"`
	// BindDN / BindPassword are the service-account credentials used to search
	// for the user in search+bind mode (empty BindDN => anonymous search).
	BindDN       string `mapstructure:"bind_dn"`
	BindPassword string `mapstructure:"bind_password"`
	// BaseDN is the search base for search+bind mode.
	BaseDN string `mapstructure:"base_dn"`
	// UserFilter is the search filter for search+bind mode; "%s" is replaced with
	// the filter-escaped login name (e.g. "(uid=%s)").
	UserFilter string `mapstructure:"user_filter"`
	// TimeoutSec bounds the dial/search; defaults to 10s when zero.
	TimeoutSec int `mapstructure:"timeout_sec"`
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

// OpenBaoConfig describes how to reach OpenBao/Vault for the secret-sync loop
// (mirrors the org contract used by the self-developed apps: KV v2 + K8s auth).
type OpenBaoConfig struct {
	// Addr is the OpenBao/Vault address, e.g. http://vault.vault.svc:8200.
	Addr string `mapstructure:"addr"`
	// Namespace is the Vault Enterprise namespace (empty for OpenBao OSS).
	Namespace string `mapstructure:"namespace"`
	// KVMount is the KV v2 mount name (e.g. "kv"); the loop reads <mount>/data/<path>.
	KVMount string `mapstructure:"kv_mount"`
	// AuthMount is the Kubernetes auth mount (e.g. "kubernetes").
	AuthMount string `mapstructure:"auth_mount"`
	// Role is the Kubernetes auth role bound to this pod's ServiceAccount.
	Role string `mapstructure:"role"`
	// Token is a static token for local/debug use; empty => K8s auth via SA token.
	Token string `mapstructure:"token"`
	// CACert is a path to the OpenBao CA file (https self-signed); empty for http.
	CACert string `mapstructure:"cacert"`
}

// SecretSyncItem maps one Vault KV path to one Kubernetes Secret and declares
// which FlinkDeployment(s) to restart when the synced values change.
type SecretSyncItem struct {
	// SecretName is the target K8s Secret (e.g. flink-s3-credentials).
	SecretName string `mapstructure:"secret_name"`
	// KVPath is the KV v2 path under the mount (e.g. config/cil/flink/vault-a/s3).
	KVPath string `mapstructure:"kv_path"`
	// Restart selects which deployments to restart on change. When omitted it
	// defaults to "none" (sync the Secret only). Set restart.mode to
	// references|all or restart.deployments to an explicit list to opt in.
	Restart RestartSpec `mapstructure:"restart"`
}

// Restart modes for a SecretSyncItem.
const (
	// RestartModeReferences restarts only FlinkDeployments whose podTemplate
	// references the Secret (envFrom/secretKeyRef/volume). This is the default.
	RestartModeReferences = "references"
	// RestartModeAll restarts every FlinkDeployment in the namespace.
	RestartModeAll = "all"
	// RestartModeNone never restarts (sync the Secret only).
	RestartModeNone = "none"
)

// RestartSpec selects which FlinkDeployments a secret change should restart.
// Precedence: a non-empty Deployments list wins; otherwise Mode applies, with an
// empty Mode meaning "references".
type RestartSpec struct {
	// Mode is one of references (default) | all | none. Ignored when Deployments
	// is set.
	Mode string `mapstructure:"mode"`
	// Deployments is an explicit list of FlinkDeployment names to restart.
	Deployments []string `mapstructure:"deployments"`
}

// ResolvedMode returns the effective mode after applying precedence rules:
// an explicit Deployments list is treated as a list, otherwise the Mode (empty
// => none, i.e. sync only / no restart).
func (r RestartSpec) ResolvedMode() string {
	if len(r.Deployments) > 0 {
		return "list"
	}
	if r.Mode == "" {
		return RestartModeNone
	}
	return r.Mode
}

// SecretSyncConfig configures the in-process OpenBao->Secret sync loop
// (internal/secretsync). Disabled unless Enabled is true and Items are set.
type SecretSyncConfig struct {
	Enabled     bool             `mapstructure:"enabled"`
	IntervalSec int              `mapstructure:"interval_sec"`
	OpenBao     OpenBaoConfig    `mapstructure:"openbao"`
	Items       []SecretSyncItem `mapstructure:"items"`
}

// ReplayConfig gates the "replay from timestamp" feature (internal/flink replay
// orchestration). It performs OUT-OF-BAND writes to the SQL ConfigMap and the
// FlinkDeployment spec, so it is disabled by default and must be explicitly
// enabled. See docs/replay-from-timestamp-design.md.
type ReplayConfig struct {
	// Enabled turns the replay API on. Default false (writes job logic).
	Enabled bool `mapstructure:"enabled"`
	// GitOpsManaged marks the target environment as continuously reconciled by a
	// GitOps controller (Argo/Flux). When true the API still works but flags the
	// drift risk and requires an explicit confirmDrift on mutating calls, since a
	// live ConfigMap edit would be reverted by the next sync.
	GitOpsManaged bool `mapstructure:"gitops_managed"`
	// SQLVolumeName is the podTemplate volume that mounts the SQL ConfigMap; the
	// ConfigMap name is read from this volume on the CR (naming-convention
	// independent). Matches the base template's "sql-scripts" volume.
	SQLVolumeName string `mapstructure:"sql_volume_name"`
	// SQLFileKey is the ConfigMap data key holding the job SQL (e.g. "job.sql").
	SQLFileKey string `mapstructure:"sql_file_key"`
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

	// SecretSync configures the OpenBao/Vault -> K8s Secret sync loop (no ESO).
	// Disabled by default; enable to have the console keep flink-s3-credentials /
	// flink-job-env in sync and (optionally) restart jobs on change.
	SecretSync SecretSyncConfig `mapstructure:"secret_sync"`

	// Replay gates the "replay from timestamp" feature (out-of-band SQL + spec
	// writes). Disabled by default; see docs/replay-from-timestamp-design.md.
	Replay ReplayConfig `mapstructure:"replay"`
}

// validateRestartSpecs checks each secret_sync item's restart spec: mode must be
// one of references|all|none (empty => references), and mode + deployments must
// not both be set.
func validateRestartSpecs(cfg *Config) error {
	for i, it := range cfg.SecretSync.Items {
		r := it.Restart
		switch r.Mode {
		case "", RestartModeReferences, RestartModeAll, RestartModeNone:
		default:
			return fmt.Errorf("secret_sync.items[%d] (%s): invalid restart.mode %q "+
				"(want references|all|none)", i, it.SecretName, r.Mode)
		}
		if len(r.Deployments) > 0 && r.Mode != "" {
			return fmt.Errorf("secret_sync.items[%d] (%s): set either restart.deployments "+
				"or restart.mode, not both", i, it.SecretName)
		}
	}
	return nil
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
	v.SetDefault("auth.ldap.enabled", false)
	v.SetDefault("auth.ldap.timeout_sec", 10)
	v.SetDefault("secret_sync.enabled", false)
	v.SetDefault("secret_sync.interval_sec", 300)
	v.SetDefault("secret_sync.openbao.kv_mount", "kv")
	v.SetDefault("secret_sync.openbao.auth_mount", "kubernetes")
	v.SetDefault("secret_sync.openbao.role", "vault-app-role")
	v.SetDefault("replay.enabled", false)
	v.SetDefault("replay.gitops_managed", false)
	v.SetDefault("replay.sql_volume_name", "sql-scripts")
	v.SetDefault("replay.sql_file_key", "job.sql")

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
		"cluster.s3.insecure", "cluster.s3.prefix",
		"auth.username", "auth.password", "auth.session_secret", "auth.cookie_secure",
		"auth.ldap.enabled", "auth.ldap.url", "auth.ldap.start_tls",
		"auth.ldap.insecure_skip_verify", "auth.ldap.user_dn_template",
		"auth.ldap.bind_dn", "auth.ldap.bind_password", "auth.ldap.base_dn",
		"auth.ldap.user_filter", "auth.ldap.timeout_sec",
		"ha.self_cluster_id", "ha.default_peer_cluster_id", "ha.auto_all",
		"secret_sync.enabled", "secret_sync.interval_sec",
		"secret_sync.openbao.addr", "secret_sync.openbao.namespace", "secret_sync.openbao.kv_mount",
		"secret_sync.openbao.auth_mount", "secret_sync.openbao.role", "secret_sync.openbao.token",
		"secret_sync.openbao.cacert",
		"replay.enabled", "replay.gitops_managed", "replay.sql_volume_name", "replay.sql_file_key",
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
	if err := validateRestartSpecs(&cfg); err != nil {
		return nil, err
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

// IsLocalHADeployment checks ownership policy for a resource in the console's
// configured namespace. AutoAll applies without a live Kubernetes listing.
func (c *Config) IsLocalHADeployment(deployment string) bool {
	if c.HA.AutoAll {
		return true
	}
	for _, group := range c.HA.Groups {
		group = c.NormalizeGroup(group)
		if group.Namespace == c.Cluster.Namespace && group.Deployment == deployment {
			return true
		}
	}
	return false
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
