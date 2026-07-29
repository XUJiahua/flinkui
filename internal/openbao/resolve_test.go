package openbao

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
)

// vaultStub serves KV v2 reads (static token, no k8s auth) for the given store.
func vaultStub(t *testing.T, kv map[string]map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// path: /v1/<mount>/data/<p>
		const p = "/v1/kv/data/"
		key := r.URL.Path[len(p):]
		data, ok := kv[key]
		if !ok {
			w.WriteHeader(404)
			return
		}
		inner := map[string]any{}
		for k, v := range data {
			inner[k] = v
		}
		_ = writeJSON(w, map[string]any{"data": map[string]any{"data": inner}})
	}))
}

func writeJSON(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(v)
}

func baseVaultCfg(addr string) config.VaultConfig {
	return config.VaultConfig{OpenBaoConfig: config.OpenBaoConfig{Addr: addr, KVMount: "kv", Token: "t"}}
}

func TestResolveConfigRefsNoRefsIsNoop(t *testing.T) {
	cfg := &config.Config{} // no refs, no vault addr
	if err := ResolveConfigRefs(cfg); err != nil {
		t.Fatalf("no refs should be a no-op, got %v", err)
	}
}

func TestResolveConfigRefsFillsEmptyFields(t *testing.T) {
	srv := vaultStub(t, map[string]map[string]string{
		"console/s3":   {"access-key": "AK", "secret-key": "SK"},
		"console/auth": {"session-secret": "hmac-key"},
		"console/ldap": {"bind-password": "ldap-pw"},
	})
	defer srv.Close()

	cfg := &config.Config{Vault: baseVaultCfg(srv.URL)}
	cfg.Cluster.S3.AccessKeyRef = config.SecretRef{Path: "console/s3", Key: "access-key"}
	cfg.Cluster.S3.SecretKeyRef = config.SecretRef{Path: "console/s3", Key: "secret-key"}
	cfg.Auth.SessionSecretRef = config.SecretRef{Path: "console/auth", Key: "session-secret"}
	cfg.Auth.LDAP.BindPasswordRef = config.SecretRef{Path: "console/ldap", Key: "bind-password"}

	if err := ResolveConfigRefs(cfg); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.Cluster.S3.AccessKey != "AK" || cfg.Cluster.S3.SecretKey != "SK" {
		t.Errorf("s3 creds not filled: %q/%q", cfg.Cluster.S3.AccessKey, cfg.Cluster.S3.SecretKey)
	}
	if cfg.Auth.SessionSecret != "hmac-key" {
		t.Errorf("session secret not filled: %q", cfg.Auth.SessionSecret)
	}
	if cfg.Auth.LDAP.BindPassword != "ldap-pw" {
		t.Errorf("ldap bind password not filled: %q", cfg.Auth.LDAP.BindPassword)
	}
}

func TestResolveConfigRefsLiteralWins(t *testing.T) {
	srv := vaultStub(t, map[string]map[string]string{"console/auth": {"session-secret": "from-vault"}})
	defer srv.Close()

	cfg := &config.Config{Vault: baseVaultCfg(srv.URL)}
	cfg.Auth.SessionSecret = "from-env" // literal/env present
	cfg.Auth.SessionSecretRef = config.SecretRef{Path: "console/auth", Key: "session-secret"}

	if err := ResolveConfigRefs(cfg); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.Auth.SessionSecret != "from-env" {
		t.Errorf("literal/env must win over vault ref, got %q", cfg.Auth.SessionSecret)
	}
}

func TestResolveConfigRefsMissingKeyErrors(t *testing.T) {
	srv := vaultStub(t, map[string]map[string]string{"console/s3": {"access-key": "AK"}})
	defer srv.Close()
	cfg := &config.Config{Vault: baseVaultCfg(srv.URL)}
	cfg.Cluster.S3.SecretKeyRef = config.SecretRef{Path: "console/s3", Key: "secret-key"} // key absent
	if err := ResolveConfigRefs(cfg); err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestResolveConfigRefsRequiresAddr(t *testing.T) {
	cfg := &config.Config{} // ref set but no vault.addr
	cfg.Auth.SessionSecretRef = config.SecretRef{Path: "console/auth", Key: "session-secret"}
	if err := ResolveConfigRefs(cfg); err == nil {
		t.Fatal("expected error when a ref is set but vault.addr is empty")
	}
}
