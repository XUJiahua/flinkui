package config

import "testing"

func TestBucketPrefix(t *testing.T) {
	tests := []struct {
		name       string
		bucket     string
		prefix     string
		wantBucket string
		wantPrefix string
	}{
		{"plain bucket", "flink", "", "flink", ""},
		{"bucket with path suffix", "flink/tenant-a", "", "flink", "tenant-a"},
		{"bucket with deep path", "flink/tenant-a/sit", "", "flink", "tenant-a/sit"},
		{"explicit prefix", "flink", "tenant-a/sit", "flink", "tenant-a/sit"},
		{"bucket path + explicit prefix merged", "flink/a", "b", "flink", "a/b"},
		{"trims slashes", "/flink/", "/tenant-a/", "flink", "tenant-a"},
		{"empty", "", "", "", ""},
		{"prefix only, empty bucket", "", "x", "", "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, p := S3Config{Bucket: tt.bucket, Prefix: tt.prefix}.BucketPrefix()
			if b != tt.wantBucket || p != tt.wantPrefix {
				t.Errorf("BucketPrefix(bucket=%q,prefix=%q) = (%q,%q), want (%q,%q)",
					tt.bucket, tt.prefix, b, p, tt.wantBucket, tt.wantPrefix)
			}
		})
	}
}
