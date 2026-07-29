package openbao

import (
	"fmt"

	"github.com/fko-demo/flinkui/internal/config"
)

// ResolveConfigRefs fills the console's own sensitive config fields from Vault
// for any *_ref that is set AND whose literal/env value is still empty
// (literal/env wins — see the source precedence in the design doc). It is a
// no-op when no ref is set. On success the referenced fields are populated in
// place; on error it returns a descriptive error (the caller decides whether to
// fail-fast or fall back based on cfg.Vault.Optional).
//
// Reads are cached per KV path, so several keys under one path cost one read.
func ResolveConfigRefs(cfg *config.Config) error {
	type binding struct {
		name   string
		ref    config.SecretRef
		target *string
	}
	bindings := []binding{
		{"auth.password_ref", cfg.Auth.PasswordRef, &cfg.Auth.Password},
		{"auth.session_secret_ref", cfg.Auth.SessionSecretRef, &cfg.Auth.SessionSecret},
		{"auth.ldap.bind_password_ref", cfg.Auth.LDAP.BindPasswordRef, &cfg.Auth.LDAP.BindPassword},
		{"cluster.s3.access_key_ref", cfg.Cluster.S3.AccessKeyRef, &cfg.Cluster.S3.AccessKey},
		{"cluster.s3.secret_key_ref", cfg.Cluster.S3.SecretKeyRef, &cfg.Cluster.S3.SecretKey},
	}

	// Which refs are set and still need resolving (literal/env empty)?
	var pending []binding
	for _, b := range bindings {
		if b.ref.IsSet() && *b.target == "" {
			pending = append(pending, b)
		}
	}
	if len(pending) == 0 {
		return nil // nothing to resolve (also covers "no refs configured at all")
	}
	if cfg.Vault.Addr == "" {
		return fmt.Errorf("vault refs are set (e.g. %s) but vault.addr is empty", pending[0].name)
	}

	client, err := New(cfg.Vault.OpenBaoConfig)
	if err != nil {
		return fmt.Errorf("vault client: %w", err)
	}
	token, err := client.Login()
	if err != nil {
		return fmt.Errorf("vault login: %w", err)
	}

	cache := map[string]map[string]string{}
	for _, b := range pending {
		data, ok := cache[b.ref.Path]
		if !ok {
			data, err = client.ReadKV(token, b.ref.Path)
			if err != nil {
				return fmt.Errorf("resolve %s (%s): %w", b.name, b.ref.Path, err)
			}
			cache[b.ref.Path] = data
		}
		val, exists := data[b.ref.Key]
		if !exists {
			return fmt.Errorf("resolve %s: key %q not found at %s", b.name, b.ref.Key, b.ref.Path)
		}
		*b.target = val
	}
	return nil
}
