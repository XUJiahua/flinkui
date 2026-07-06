// Package store lists Flink savepoints and checkpoints from S3/MinIO to power
// the rollback recovery-point selector (design §4.2).
package store

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/fko-demo/flinkui/internal/config"
)

// RecoveryPoint is a savepoint or checkpoint usable for rollback.
type RecoveryPoint struct {
	Type     string    `json:"type"` // "savepoint" | "checkpoint"
	Path     string    `json:"path"` // full s3:// path to pass to rollback
	Name     string    `json:"name"` // e.g. savepoint-abc123 or chk-42
	Modified time.Time `json:"modified"`
}

// Store lists recovery points from an S3-compatible object store.
type Store struct {
	client        *s3.Client
	defaultBucket string // fallback bucket when a deployment dir is not provided
}

// httpClient returns an HTTP client for the S3 SDK. When insecure is set it
// skips TLS verification, for internal MinIO endpoints with self-signed certs.
func httpClient(insecure bool) *http.Client {
	if !insecure {
		return http.DefaultClient
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // internal MinIO self-signed cert
	return &http.Client{Transport: tr}
}

// New builds a Store. It is intended to be called only when S3 is configured
// (endpoint or credentials present).
func New(ctx context.Context, cfg config.S3Config) (*Store, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
		awsconfig.WithHTTPClient(httpClient(cfg.Insecure)),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	})
	return &Store{client: client, defaultBucket: cfg.Bucket}, nil
}

// ListRecoveryPoints lists savepoints and checkpoints for a job, newest first.
// savepointsDir/checkpointsDir are the deployment's configured s3:// dirs
// (spec.flinkConfiguration state.savepoints.dir / state.checkpoints.dir); when
// empty they fall back to <defaultBucket>/savepoints|checkpoints/<job>.
func (s *Store) ListRecoveryPoints(ctx context.Context, job, savepointsDir, checkpointsDir string) ([]RecoveryPoint, error) {
	var points []RecoveryPoint

	spBucket, spPrefix := s.resolve(savepointsDir, "savepoints/"+job)
	cpBucket, cpPrefix := s.resolve(checkpointsDir, "checkpoints/"+job)

	if spBucket != "" {
		sps, err := s.listSavepoints(ctx, spBucket, spPrefix)
		if err != nil {
			return nil, err
		}
		points = append(points, sps...)
	}
	if cpBucket != "" {
		cps, err := s.listCheckpoints(ctx, cpBucket, cpPrefix)
		if err != nil {
			return nil, err
		}
		points = append(points, cps...)
	}

	sort.Slice(points, func(i, j int) bool { return points[i].Modified.After(points[j].Modified) })
	return points, nil
}

// resolve turns a possibly-empty s3:// dir URI into (bucket, keyPrefix). When
// the URI is empty it uses the configured default bucket with fallbackKey.
func (s *Store) resolve(dirURI, fallbackKey string) (bucket, prefix string) {
	if strings.HasPrefix(dirURI, "s3://") || strings.HasPrefix(dirURI, "s3a://") {
		rest := dirURI
		rest = strings.TrimPrefix(rest, "s3://")
		rest = strings.TrimPrefix(rest, "s3a://")
		parts := strings.SplitN(rest, "/", 2)
		bucket = parts[0]
		if len(parts) == 2 {
			prefix = parts[1]
		}
		return bucket, ensureSlash(prefix)
	}
	return s.defaultBucket, ensureSlash(fallbackKey)
}

func ensureSlash(p string) string {
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

// listSavepoints lists directories directly under the savepoints prefix
// (each is a savepoint-xxxx directory).
func (s *Store) listSavepoints(ctx context.Context, bucket, prefix string) ([]RecoveryPoint, error) {
	var out []RecoveryPoint
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list savepoints: %w", err)
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			dir := strings.TrimSuffix(*cp.Prefix, "/")
			name := dir[strings.LastIndex(dir, "/")+1:]
			out = append(out, RecoveryPoint{
				Type:     "savepoint",
				Path:     fmt.Sprintf("s3://%s/%s", bucket, dir),
				Name:     name,
				Modified: s.dirModified(ctx, bucket, *cp.Prefix),
			})
		}
	}
	return out, nil
}

// listCheckpoints finds chk-N directories under the checkpoints prefix by
// locating their _metadata files (design §4.2). The layout is
// <prefix>/<jobId>/chk-N/_metadata.
func (s *Store) listCheckpoints(ctx context.Context, bucket, prefix string) ([]RecoveryPoint, error) {
	var out []RecoveryPoint
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list checkpoints: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || !strings.HasSuffix(*obj.Key, "/_metadata") {
				continue
			}
			dir := strings.TrimSuffix(*obj.Key, "/_metadata")
			name := dir[strings.LastIndex(dir, "/")+1:]
			var mod time.Time
			if obj.LastModified != nil {
				mod = *obj.LastModified
			}
			out = append(out, RecoveryPoint{
				Type:     "checkpoint",
				Path:     fmt.Sprintf("s3://%s/%s", bucket, dir),
				Name:     name,
				Modified: mod,
			})
		}
	}
	return out, nil
}

// dirModified returns the newest LastModified among the first page of objects
// under a savepoint directory (best-effort timestamp).
func (s *Store) dirModified(ctx context.Context, bucket, prefix string) time.Time {
	page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1000),
	})
	if err != nil {
		return time.Time{}
	}
	var newest time.Time
	for _, obj := range page.Contents {
		if obj.LastModified != nil && obj.LastModified.After(newest) {
			newest = *obj.LastModified
		}
	}
	return newest
}
