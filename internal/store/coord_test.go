package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	"errors"
	"github.com/fko-demo/flinkui/internal/config"
	"testing"
)

func TestCoordKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{"no prefix", "", "fencing/g/active-cluster", "fencing/g/active-cluster"},
		{"no prefix strips leading slash", "", "/fencing/g/handoff", "fencing/g/handoff"},
		{"with prefix", "tenant-a", "fencing/g/handoff", "tenant-a/fencing/g/handoff"},
		{"with deep prefix", "tenant-a/sit", "fencing/g/active-cluster", "tenant-a/sit/fencing/g/active-cluster"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Coord{prefix: tt.prefix}
			if got := c.key(tt.key); got != tt.want {
				t.Errorf("key(%q) with prefix %q = %q, want %q", tt.key, tt.prefix, got, tt.want)
			}
		})
	}
}

func TestResolveWithDefaultPrefix(t *testing.T) {
	s := &Store{defaultBucket: "flink", defaultPrefix: "tenant-a"}
	// Empty dir uses default bucket + prefixed fallback key.
	if b, p := s.resolve("", "savepoints/codes"); b != "flink" || p != "tenant-a/savepoints/codes/" {
		t.Errorf("resolve fallback = (%q,%q), want (flink, tenant-a/savepoints/codes/)", b, p)
	}
	// Absolute s3:// dir is used verbatim (no prefixing).
	if b, p := s.resolve("s3://other/x/y", "savepoints/codes"); b != "other" || p != "x/y/" {
		t.Errorf("resolve absolute = (%q,%q), want (other, x/y/)", b, p)
	}
}

func TestIsPreconditionFailed(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("connection refused"), false},
		{"named 412", errors.New("api error PreconditionFailed: At least one of the preconditions you specified did not hold"), true},
		{"StatusCode 412", errors.New("operation error S3: PutObject, https response error StatusCode: 412"), true},
		{"lowercase status code 412", errors.New("status code: 412"), true},
		{"404 is not a precondition failure", errors.New("StatusCode: 404"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPreconditionFailed(tt.err); got != tt.want {
				t.Errorf("isPreconditionFailed(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestConcurrentHandoffCASHasOneWinner(t *testing.T) {
	var mu sync.Mutex
	var object []byte
	var etag string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			if object == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", etag)
			_, _ = w.Write(object)
		case http.MethodPut:
			if (object == nil && r.Header.Get("If-None-Match") != "*") ||
				(object != nil && r.Header.Get("If-Match") != etag) {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			object = body
			etag = fmt.Sprintf("%q", sha256.Sum256(body))
			w.Header().Set("ETag", etag)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer endpoint.Close()
	ctx := context.Background()
	coord, err := NewCoord(ctx, config.S3Config{
		Endpoint: endpoint.URL, Bucket: "flink", PathStyle: true, AccessKey: "test", SecretKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	const key = "handoff"
	for _, epoch := range []int64{1, 2} {
		_, expected, _, err := coord.ReadHandoffWithETag(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		const contenders = 32
		results := make(chan error, contenders)
		for i := range contenders {
			go func() {
				results <- coord.WriteHandoffCAS(ctx, key, &HandoffRecord{
					Group: "orders", Epoch: epoch, Phase: PhasePromoting, ActiveClusterID: fmt.Sprint(i),
				}, expected)
			}()
		}
		winners := 0
		for range contenders {
			switch err := <-results; {
			case err == nil:
				winners++
			case errors.Is(err, ErrHandoffConflict):
			default:
				t.Errorf("unexpected CAS result: %v", err)
			}
		}
		rec, _, ok, err := coord.ReadHandoffWithETag(ctx, key)
		if winners != 1 || err != nil || !ok || rec.Epoch != epoch {
			t.Fatalf("epoch %d: winners=%d, handoff=%+v, present=%v, error=%v", epoch, winners, rec, ok, err)
		}
	}
}
