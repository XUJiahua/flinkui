// Package openbao is a minimal, dependency-free OpenBao/Vault client (stdlib
// net/http only) implementing the org contract: Kubernetes auth + KV v2 read.
// It is shared by the secret-sync loop (internal/secretsync) and the startup
// resolution of the console's own secrets (config vault refs).
package openbao

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fko-demo/flinkui/internal/config"
)

// saTokenFile is the in-cluster ServiceAccount token path used for Kubernetes auth.
const saTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// Client is a minimal OpenBao/Vault client (stdlib net/http only).
type Client struct {
	cfg  config.OpenBaoConfig
	http *http.Client
}

// New builds a Client from the OpenBao connection config. It configures TLS from
// CACert when provided, or skips verification for a bare https POC endpoint.
func New(cfg config.OpenBaoConfig) (*Client, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("openbao addr is empty")
	}
	tlsCfg := &tls.Config{}
	switch {
	case cfg.CACert != "":
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read cacert %q: %w", cfg.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse cacert %q", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	case strings.HasPrefix(cfg.Addr, "https"):
		// Self-signed POC endpoint without a provided CA. Skips verification;
		// for production set OpenBao.CACert.
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // opt-in POC path
	}
	return &Client{
		cfg:  cfg,
		http: &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsCfg}},
	}, nil
}

func (c *Client) do(method, path string, body any, token string) (map[string]any, error) {
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, strings.TrimRight(c.cfg.Addr, "/")+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", c.cfg.Namespace)
	}
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("openbao %s %s -> HTTP %d: %s", method, path, resp.StatusCode, truncate(data, 300))
	}
	var out map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return out, nil
}

// Login authenticates and returns a client token. Uses a static token when set;
// otherwise the pod ServiceAccount token via the Kubernetes auth method.
func (c *Client) Login() (string, error) {
	if c.cfg.Token != "" {
		return c.cfg.Token, nil
	}
	jwt, err := os.ReadFile(saTokenFile)
	if err != nil {
		return "", fmt.Errorf("read SA token: %w", err)
	}
	mount := strDefault(c.cfg.AuthMount, "kubernetes")
	resp, err := c.do(http.MethodPost, "/v1/auth/"+mount+"/login",
		map[string]any{"role": c.cfg.Role, "jwt": string(jwt)}, "")
	if err != nil {
		return "", err
	}
	auth, _ := resp["auth"].(map[string]any)
	tok, _ := auth["client_token"].(string)
	if tok == "" {
		return "", fmt.Errorf("openbao login: no client_token in response")
	}
	return tok, nil
}

// ReadKV reads a KV v2 secret at <mount>/data/<path> and returns its data map.
func (c *Client) ReadKV(token, path string) (map[string]string, error) {
	mount := strDefault(c.cfg.KVMount, "kv")
	resp, err := c.do(http.MethodGet, "/v1/"+mount+"/data/"+strings.Trim(path, "/"), nil, token)
	if err != nil {
		return nil, err
	}
	outer, _ := resp["data"].(map[string]any)
	inner, _ := outer["data"].(map[string]any)
	out := make(map[string]string, len(inner))
	for k, v := range inner {
		out[k] = fmt.Sprintf("%v", v)
	}
	return out, nil
}

func strDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n])
	}
	return string(b)
}
