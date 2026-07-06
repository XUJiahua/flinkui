// Package store lists Flink savepoints and checkpoints from S3/MinIO to power
// the rollback recovery-point selector (design §4.2).
package store

import (
	"context"
	"fmt"
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
	client *s3.Client
	bucket string
}

// New builds a Store, or returns (nil, error) if S3 is not configured.
func New(ctx context.Context, cfg config.S3Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket not configured")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
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
	return &Store{client: client, bucket: cfg.Bucket}, nil
}

// ListRecoveryPoints returns savepoints and checkpoints for a job, newest first.
func (s *Store) ListRecoveryPoints(ctx context.Context, job string) ([]RecoveryPoint, error) {
	var points []RecoveryPoint

	sps, err := s.listSavepoints(ctx, job)
	if err != nil {
		return nil, err
	}
	points = append(points, sps...)

	cps, err := s.listCheckpoints(ctx, job)
	if err != nil {
		return nil, err
	}
	points = append(points, cps...)

	sort.Slice(points, func(i, j int) bool { return points[i].Modified.After(points[j].Modified) })
	return points, nil
}

// listSavepoints lists directories under savepoints/<job>/ (each is a savepoint).
func (s *Store) listSavepoints(ctx context.Context, job string) ([]RecoveryPoint, error) {
	prefix := fmt.Sprintf("savepoints/%s/", job)
	var out []RecoveryPoint

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
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
				Path:     fmt.Sprintf("s3://%s/%s", s.bucket, dir),
				Name:     name,
				Modified: s.dirModified(ctx, *cp.Prefix),
			})
		}
	}
	return out, nil
}

// listCheckpoints finds chk-N directories under checkpoints/<job>/ by locating
// their _metadata files (design §4.2).
func (s *Store) listCheckpoints(ctx context.Context, job string) ([]RecoveryPoint, error) {
	prefix := fmt.Sprintf("checkpoints/%s/", job)
	var out []RecoveryPoint

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
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
				Path:     fmt.Sprintf("s3://%s/%s", s.bucket, dir),
				Name:     name,
				Modified: mod,
			})
		}
	}
	return out, nil
}

// dirModified returns the newest LastModified among the first page of objects
// under a savepoint directory (best-effort timestamp).
func (s *Store) dirModified(ctx context.Context, prefix string) time.Time {
	page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
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
