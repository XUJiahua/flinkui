package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fko-demo/flinkui/internal/config"
)

func TestCASProbeRejectsUnsafeBackends(t *testing.T) {
	for _, tc := range []struct {
		name, ignoredHeader, wantError string
	}{
		{name: "conditional storage"},
		{name: "ignores create precondition", ignoredHeader: "If-None-Match", wantError: "accepted duplicate create"},
		{name: "ignores update precondition", ignoredHeader: "If-Match", wantError: "got 8 winners"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var body []byte
			var etag string
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if !strings.HasPrefix(r.URL.Path, "/flink/ha/cas-probes/") {
					t.Errorf("unexpected probe path %s", r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				switch r.Method {
				case http.MethodGet:
					w.Header().Set("ETag", etag)
					_, _ = w.Write(body)
				case http.MethodPut:
					if (tc.ignoredHeader != "If-None-Match" && r.Header.Get("If-None-Match") == "*" && body != nil) ||
						(tc.ignoredHeader != "If-Match" && r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != etag) {
						w.WriteHeader(http.StatusPreconditionFailed)
						return
					}
					value, err := io.ReadAll(r.Body)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					body = value
					etag = fmt.Sprintf("%q", sha256.Sum256(body))
					w.Header().Set("ETag", etag)
				case http.MethodDelete:
					body = nil
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer endpoint.Close()
			ctx := context.Background()
			coord, err := NewCoord(ctx, config.S3Config{
				Endpoint: endpoint.URL, Bucket: "flink", Prefix: "ha", PathStyle: true, AccessKey: "test", SecretKey: "test",
			})
			if err != nil {
				t.Fatal(err)
			}
			err = coord.ProbeCAS(ctx)
			if tc.wantError == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("got %v, want %q", err, tc.wantError)
			}
			mu.Lock()
			defer mu.Unlock()
			if body != nil {
				t.Fatal("probe object was not cleaned up")
			}
		})
	}
}

func TestTargetS3CASProbe(t *testing.T) {
	if os.Getenv("FKO_RUN_S3_CAS_PROBE") != "1" {
		t.Skip("set FKO_RUN_S3_CAS_PROBE=1 to run against the configured S3 bucket")
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	coord, err := NewCoord(ctx, cfg.Cluster.S3)
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.ProbeCAS(ctx); err != nil {
		t.Fatal(err)
	}
}
