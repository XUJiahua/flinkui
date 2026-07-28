package auth

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/fko-demo/flinkui/internal/config"
	"github.com/go-ldap/ldap/v3"
)

// Authenticator verifies a username/password pair against a backend. It returns
// nil on success and a non-nil error when the credentials are rejected or the
// backend is unreachable. Kept as an interface so Login is testable without a
// real directory server.
type Authenticator interface {
	Authenticate(username, password string) error
}

// ldapAuthenticator authenticates against an LDAP/AD server. It supports both
// direct-bind (UserDNTemplate) and search+bind (BaseDN + UserFilter) modes.
type ldapAuthenticator struct {
	cfg config.LDAPConfig
}

// newLDAPAuthenticator validates the config and returns an Authenticator, or an
// error when the config is incomplete.
func newLDAPAuthenticator(cfg config.LDAPConfig) (Authenticator, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("ldap: url is required")
	}
	if cfg.UserDNTemplate == "" {
		// Search+bind mode requires a base DN and a filter.
		if cfg.BaseDN == "" || cfg.UserFilter == "" {
			return nil, fmt.Errorf("ldap: set user_dn_template (direct bind) or base_dn + user_filter (search bind)")
		}
		if !strings.Contains(cfg.UserFilter, "%s") {
			return nil, fmt.Errorf("ldap: user_filter must contain %%s")
		}
	} else if !strings.Contains(cfg.UserDNTemplate, "%s") {
		return nil, fmt.Errorf("ldap: user_dn_template must contain %%s")
	}
	return &ldapAuthenticator{cfg: cfg}, nil
}

func (l *ldapAuthenticator) dial() (*ldap.Conn, error) {
	timeout := time.Duration(l.cfg.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	tlsCfg := &tls.Config{InsecureSkipVerify: l.cfg.InsecureSkipVerify} //nolint:gosec // opt-in for self-signed internal servers
	conn, err := ldap.DialURL(l.cfg.URL, ldap.DialWithTLSConfig(tlsCfg), ldap.DialWithDialer(&net.Dialer{Timeout: timeout}))
	if err != nil {
		return nil, fmt.Errorf("ldap: dial %q: %w", l.cfg.URL, err)
	}
	conn.SetTimeout(timeout)
	if l.cfg.StartTLS && strings.HasPrefix(strings.ToLower(l.cfg.URL), "ldap://") {
		if err := conn.StartTLS(tlsCfg); err != nil {
			conn.Close()
			return nil, fmt.Errorf("ldap: starttls: %w", err)
		}
	}
	return conn, nil
}

// Authenticate binds as the user to verify the password.
func (l *ldapAuthenticator) Authenticate(username, password string) error {
	// An empty password would trigger an LDAP "unauthenticated bind", which many
	// servers accept as success. Reject it up front.
	if username == "" || password == "" {
		return fmt.Errorf("ldap: empty username or password")
	}
	conn, err := l.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	userDN, err := l.resolveUserDN(conn, username)
	if err != nil {
		return err
	}
	if err := conn.Bind(userDN, password); err != nil {
		return fmt.Errorf("ldap: bind as user failed: %w", err)
	}
	return nil
}

// resolveUserDN returns the DN to bind as for the given login name, using either
// the direct-bind template or a service-account search.
func (l *ldapAuthenticator) resolveUserDN(conn *ldap.Conn, username string) (string, error) {
	if l.cfg.UserDNTemplate != "" {
		return fmt.Sprintf(l.cfg.UserDNTemplate, ldap.EscapeDN(username)), nil
	}
	// Search+bind: bind with the service account (anonymous if BindDN empty).
	if l.cfg.BindDN != "" {
		if err := conn.Bind(l.cfg.BindDN, l.cfg.BindPassword); err != nil {
			return "", fmt.Errorf("ldap: service bind failed: %w", err)
		}
	}
	filter := fmt.Sprintf(l.cfg.UserFilter, ldap.EscapeFilter(username))
	res, err := conn.Search(ldap.NewSearchRequest(
		l.cfg.BaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, 0, false,
		filter,
		[]string{"dn"},
		nil,
	))
	if err != nil {
		return "", fmt.Errorf("ldap: search: %w", err)
	}
	if len(res.Entries) == 0 {
		return "", fmt.Errorf("ldap: user %q not found", username)
	}
	if len(res.Entries) > 1 {
		return "", fmt.Errorf("ldap: user %q is ambiguous (%d matches)", username, len(res.Entries))
	}
	return res.Entries[0].DN, nil
}
