package openbao

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fko-demo/flinkui/internal/config"
)

func TestNewRequiresAddr(t *testing.T) {
	if _, err := New(config.OpenBaoConfig{}); err == nil {
		t.Fatal("expected error when addr is empty")
	}
}

func TestLoginStaticTokenSkipsK8sAuth(t *testing.T) {
	c, err := New(config.OpenBaoConfig{Addr: "http://vault:8200", Token: "s.static"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tok, err := c.Login()
	if err != nil || tok != "s.static" {
		t.Fatalf("Login static token: tok=%q err=%v", tok, err)
	}
}

func TestReadKVParsesKVv2Shape(t *testing.T) {
	var gotPath, gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotToken = r.Header.Get("X-Vault-Token")
		_, _ = w.Write([]byte(`{"data":{"data":{"access-key":"ak","secret-key":"sk"}}}`))
	}))
	defer srv.Close()

	c, err := New(config.OpenBaoConfig{Addr: srv.URL, KVMount: "kv"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	data, err := c.ReadKV("tok-123", "config/console/s3")
	if err != nil {
		t.Fatalf("ReadKV: %v", err)
	}
	if data["access-key"] != "ak" || data["secret-key"] != "sk" {
		t.Errorf("unexpected data: %v", data)
	}
	if gotPath != "/v1/kv/data/config/console/s3" {
		t.Errorf("KV v2 path = %q", gotPath)
	}
	if gotToken != "tok-123" {
		t.Errorf("X-Vault-Token = %q", gotToken)
	}
}
