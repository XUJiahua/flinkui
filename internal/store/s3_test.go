package store

import "testing"

func TestEnsureSlash(t *testing.T) {
	tests := map[string]string{
		"":                  "",
		"checkpoints/j":     "checkpoints/j/",
		"checkpoints/j/":    "checkpoints/j/",
		"a/b/c":             "a/b/c/",
	}
	for in, want := range tests {
		if got := ensureSlash(in); got != want {
			t.Errorf("ensureSlash(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolve(t *testing.T) {
	s := &Store{defaultBucket: "fallback-bucket"}
	tests := []struct {
		name        string
		dirURI      string
		fallbackKey string
		wantBucket  string
		wantPrefix  string
	}{
		{
			name:       "s3 uri with cluster prefix",
			dirURI:     "s3://flink/evofinder-flink-sit/checkpoints/codes",
			wantBucket: "flink",
			wantPrefix: "evofinder-flink-sit/checkpoints/codes/",
		},
		{
			name:       "s3a scheme",
			dirURI:     "s3a://bucket/savepoints/job",
			wantBucket: "bucket",
			wantPrefix: "savepoints/job/",
		},
		{
			name:        "empty uri falls back to default bucket + key",
			dirURI:      "",
			fallbackKey: "savepoints/codes",
			wantBucket:  "fallback-bucket",
			wantPrefix:  "savepoints/codes/",
		},
		{
			name:       "bucket only, no prefix",
			dirURI:     "s3://onlybucket",
			wantBucket: "onlybucket",
			wantPrefix: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, p := s.resolve(tt.dirURI, tt.fallbackKey)
			if b != tt.wantBucket || p != tt.wantPrefix {
				t.Errorf("resolve(%q,%q) = (%q,%q), want (%q,%q)",
					tt.dirURI, tt.fallbackKey, b, p, tt.wantBucket, tt.wantPrefix)
			}
		})
	}
}
