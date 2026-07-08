package store

import (
	"errors"
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
