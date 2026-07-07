package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/fko-demo/flinkui/internal/config"
)

// FencingStore reads and writes the S3 fencing token that marks the active
// cluster of an HA group (design failover §2; mirrors scripts/failover.sh's
// write_fencing_token). The token object holds a clusterId (or the neutral
// token during a switch). The platform accesses S3 directly (no mc Pod).
type FencingStore struct {
	client *s3.Client
	bucket string
}

// NewFencing builds a FencingStore for the given S3 config. The bucket comes
// from cfg.Bucket; the object key is provided per call (the HA group's
// fencingKey may include any prefix).
func NewFencing(ctx context.Context, cfg config.S3Config) (*FencingStore, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket not configured for fencing")
	}
	client, err := buildClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &FencingStore{client: client, bucket: cfg.Bucket}, nil
}

// ReadToken returns the current fencing token value. A missing object yields
// ("", nil) — meaning the token is unset.
func (f *FencingStore) ReadToken(ctx context.Context, key string) (string, error) {
	out, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return "", nil
		}
		// Some S3-compatible stores return a generic NotFound; treat 404 as unset.
		if strings.Contains(err.Error(), "StatusCode: 404") || strings.Contains(err.Error(), "NoSuchKey") {
			return "", nil
		}
		return "", fmt.Errorf("read fencing token: %w", err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("read fencing token body: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// WriteToken sets the fencing token to the given value (a clusterId, or the
// neutral token to fence both sides).
func (f *FencingStore) WriteToken(ctx context.Context, key, value string) error {
	_, err := f.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(f.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader([]byte(value)),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("write fencing token: %w", err)
	}
	return nil
}
