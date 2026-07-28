package auth

import (
	"errors"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
)

// fakeAuthenticator is a stub Authenticator for exercising the LDAP fallback
// path without a real directory server.
type fakeAuthenticator struct {
	wantUser string
	wantPass string
	calls    int
}

func (f *fakeAuthenticator) Authenticate(user, pass string) error {
	f.calls++
	if user == f.wantUser && pass == f.wantPass {
		return nil
	}
	return errors.New("invalid")
}

func baseCfg() config.AuthConfig {
	return config.AuthConfig{Username: "admin", Password: "local-secret", SessionSecret: "s"}
}

func TestAuthenticateLocalCredential(t *testing.T) {
	a := newWithAuthenticator(baseCfg(), nil)
	if !a.authenticate("admin", "local-secret") {
		t.Fatal("valid local credential should authenticate")
	}
	if a.authenticate("admin", "wrong") {
		t.Fatal("wrong local password must be rejected when LDAP disabled")
	}
	if a.authenticate("nobody", "local-secret") {
		t.Fatal("unknown user must be rejected")
	}
}

func TestAuthenticateEmptyLocalPasswordNeverMatches(t *testing.T) {
	cfg := baseCfg()
	cfg.Password = "" // local login disabled
	a := newWithAuthenticator(cfg, nil)
	if a.authenticate("admin", "") {
		t.Fatal("empty configured password must not authenticate anyone")
	}
}

func TestAuthenticateLDAPFallback(t *testing.T) {
	fake := &fakeAuthenticator{wantUser: "alice", wantPass: "ldap-pass"}
	a := newWithAuthenticator(baseCfg(), fake)

	// Local admin still works and does not hit LDAP.
	if !a.authenticate("admin", "local-secret") {
		t.Fatal("local admin should authenticate")
	}
	if fake.calls != 0 {
		t.Fatalf("local match should not call LDAP, got %d calls", fake.calls)
	}

	// LDAP user authenticates via the directory.
	if !a.authenticate("alice", "ldap-pass") {
		t.Fatal("valid LDAP credential should authenticate")
	}
	// Wrong LDAP password is rejected.
	if a.authenticate("alice", "nope") {
		t.Fatal("wrong LDAP password must be rejected")
	}
}

func TestNewValidatesLDAPConfig(t *testing.T) {
	cfg := baseCfg()
	cfg.LDAP = config.LDAPConfig{Enabled: true} // missing URL
	if _, err := New(cfg); err == nil {
		t.Fatal("expected error for LDAP enabled without url")
	}

	cfg.LDAP = config.LDAPConfig{Enabled: true, URL: "ldap://x:389"} // no bind mode
	if _, err := New(cfg); err == nil {
		t.Fatal("expected error when neither user_dn_template nor base_dn+filter set")
	}

	cfg.LDAP = config.LDAPConfig{Enabled: true, URL: "ldap://x:389", UserDNTemplate: "uid=%s,dc=x"}
	if _, err := New(cfg); err != nil {
		t.Fatalf("valid direct-bind config should succeed: %v", err)
	}

	cfg.LDAP = config.LDAPConfig{Enabled: true, URL: "ldap://x:389", BaseDN: "dc=x", UserFilter: "(uid=%s)"}
	if _, err := New(cfg); err != nil {
		t.Fatalf("valid search-bind config should succeed: %v", err)
	}
}
