package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/fko-demo/flinkui/internal/config"
)

// Handoff record phases.
const (
	PhaseStable    = "stable"
	PhaseReleased  = "released"
	PhasePromoting = "promoting"
)

// RecoveryPointRef is the recovery point recorded for a handoff.
type RecoveryPointRef struct {
	Path string `json:"path"`
	Kind string `json:"kind"` // savepoint|checkpoint|none
}

// HandoffRecord is the shared-S3 coordination object for a decentralized HA
// group (design failover-decentralized §3). It carries the monotonic epoch, the
// active cluster, the current phase, and the recovery point published by the
// releasing side.
type HandoffRecord struct {
	Group           string           `json:"group"`
	ActiveClusterID string           `json:"activeClusterId"`
	Epoch           int64            `json:"epoch"`
	Phase           string           `json:"phase"`
	RecoveryPoint   RecoveryPointRef `json:"recoveryPoint"`
	ReleasedBy      string           `json:"releasedBy,omitempty"`
	UpdatedAt       time.Time        `json:"updatedAt"`
}

// Coord is the shared-S3 coordination store: it reads/writes the fencing token
// and the handoff record. The platform accesses S3 directly (no mc Pod).
type Coord struct {
	client *s3.Client
	bucket string
}

// NewCoord builds a coordination store for the given (shared) S3 config.
func NewCoord(ctx context.Context, cfg config.S3Config) (*Coord, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket not configured for fencing/handoff coordination")
	}
	client, err := buildClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Coord{client: client, bucket: cfg.Bucket}, nil
}

func (c *Coord) getObject(ctx context.Context, key string) ([]byte, bool, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) ||
			strings.Contains(err.Error(), "StatusCode: 404") ||
			strings.Contains(err.Error(), "NoSuchKey") {
			return nil, false, nil // missing => unset
		}
		return nil, false, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (c *Coord) putObject(ctx context.Context, key, contentType string, body []byte) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return err
}

// ReadToken returns the fencing token value (empty string when unset).
func (c *Coord) ReadToken(ctx context.Context, key string) (string, error) {
	b, ok, err := c.getObject(ctx, key)
	if err != nil {
		return "", fmt.Errorf("read fencing token: %w", err)
	}
	if !ok {
		return "", nil
	}
	return strings.TrimSpace(string(b)), nil
}

// WriteToken sets the fencing token (a clusterId, or the neutral token).
func (c *Coord) WriteToken(ctx context.Context, key, value string) error {
	if err := c.putObject(ctx, key, "text/plain", []byte(value)); err != nil {
		return fmt.Errorf("write fencing token: %w", err)
	}
	return nil
}

// ReadHandoff returns the handoff record; ok=false when it does not exist yet.
func (c *Coord) ReadHandoff(ctx context.Context, key string) (*HandoffRecord, bool, error) {
	b, ok, err := c.getObject(ctx, key)
	if err != nil {
		return nil, false, fmt.Errorf("read handoff: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	var rec HandoffRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, false, fmt.Errorf("decode handoff: %w", err)
	}
	return &rec, true, nil
}

// WriteHandoff persists the handoff record (stamping UpdatedAt).
func (c *Coord) WriteHandoff(ctx context.Context, key string, rec *HandoffRecord) error {
	rec.UpdatedAt = time.Now().UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode handoff: %w", err)
	}
	if err := c.putObject(ctx, key, "application/json", b); err != nil {
		return fmt.Errorf("write handoff: %w", err)
	}
	return nil
}
