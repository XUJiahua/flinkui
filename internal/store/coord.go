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
	prefix string // optional base key prefix for shared-bucket isolation
}

// NewCoord builds a coordination store for the given (shared) S3 config.
func NewCoord(ctx context.Context, cfg config.S3Config) (*Coord, error) {
	bucket, prefix := cfg.BucketPrefix()
	if bucket == "" {
		return nil, fmt.Errorf("s3 bucket not configured for fencing/handoff coordination")
	}
	client, err := buildClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Coord{client: client, bucket: bucket, prefix: prefix}, nil
}

// key applies the configured base prefix to a coordination key so several
// flinkui instances can share a bucket without colliding.
func (c *Coord) key(k string) string {
	k = strings.TrimPrefix(k, "/")
	if c.prefix == "" {
		return k
	}
	return c.prefix + "/" + k
}

// ErrHandoffConflict is returned by conditional handoff writes when the object
// changed underneath us (S3 returned 412 Precondition Failed). It signals a lost
// race — e.g. the peer promoted at the same instant — and must abort the switch
// rather than blindly overwrite.
var ErrHandoffConflict = errors.New("handoff record changed concurrently (CAS conflict)")

func (c *Coord) getObject(ctx context.Context, key string) (body []byte, etag string, ok bool, err error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.key(key)),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) ||
			strings.Contains(err.Error(), "StatusCode: 404") ||
			strings.Contains(err.Error(), "NoSuchKey") {
			return nil, "", false, nil // missing => unset
		}
		return nil, "", false, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", false, err
	}
	if out.ETag != nil {
		etag = *out.ETag
	}
	return b, etag, true, nil
}

func (c *Coord) putObject(ctx context.Context, key, contentType string, body []byte) error {
	return c.putObjectCond(ctx, key, contentType, body, "", "")
}

// putObjectCond writes an object with optional S3 conditional headers. ifMatch
// requires the current ETag to equal the given value (compare-and-swap update);
// ifNoneMatch="*" requires the object to not already exist (create-only). A 412
// Precondition Failed is surfaced as ErrHandoffConflict. Backends that don't
// support conditional writes simply ignore the headers, degrading to
// last-write-wins (no worse than before).
func (c *Coord) putObjectCond(ctx context.Context, key, contentType string, body []byte, ifMatch, ifNoneMatch string) error {
	in := &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(c.key(key)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	}
	if ifMatch != "" {
		in.IfMatch = aws.String(ifMatch)
	}
	if ifNoneMatch != "" {
		in.IfNoneMatch = aws.String(ifNoneMatch)
	}
	_, err := c.client.PutObject(ctx, in)
	if err != nil && isPreconditionFailed(err) {
		return ErrHandoffConflict
	}
	return err
}

// isPreconditionFailed detects an S3 412 response (failed If-Match/If-None-Match).
func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "PreconditionFailed") ||
		strings.Contains(msg, "StatusCode: 412") ||
		strings.Contains(msg, "status code: 412")
}

// ReadToken returns the fencing token value (empty string when unset).
func (c *Coord) ReadToken(ctx context.Context, key string) (string, error) {
	b, _, ok, err := c.getObject(ctx, key)
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
	rec, _, ok, err := c.ReadHandoffWithETag(ctx, key)
	return rec, ok, err
}

// ReadHandoffWithETag is ReadHandoff plus the object's ETag, for callers that
// will conditionally update it (CAS) via WriteHandoffCAS. The ETag is empty when
// the record does not exist yet.
func (c *Coord) ReadHandoffWithETag(ctx context.Context, key string) (*HandoffRecord, string, bool, error) {
	b, etag, ok, err := c.getObject(ctx, key)
	if err != nil {
		return nil, "", false, fmt.Errorf("read handoff: %w", err)
	}
	if !ok {
		return nil, "", false, nil
	}
	var rec HandoffRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, "", false, fmt.Errorf("decode handoff: %w", err)
	}
	return &rec, etag, true, nil
}

// WriteHandoff persists the handoff record (stamping UpdatedAt) unconditionally.
func (c *Coord) WriteHandoff(ctx context.Context, key string, rec *HandoffRecord) error {
	return c.writeHandoff(ctx, key, rec, "", "")
}

// WriteHandoffCAS persists the handoff record only if it has not changed since
// it was read. Pass the ETag from ReadHandoffWithETag; an empty ETag means the
// record did not exist and the write is create-only (If-None-Match: *). On a
// lost race it returns ErrHandoffConflict. This turns the epoch race from a
// best-effort "higher wins" reconciliation into a hard guarantee that exactly
// one side claims the handoff.
func (c *Coord) WriteHandoffCAS(ctx context.Context, key string, rec *HandoffRecord, expectedETag string) error {
	if expectedETag == "" {
		return c.writeHandoff(ctx, key, rec, "", "*")
	}
	return c.writeHandoff(ctx, key, rec, expectedETag, "")
}

func (c *Coord) writeHandoff(ctx context.Context, key string, rec *HandoffRecord, ifMatch, ifNoneMatch string) error {
	rec.UpdatedAt = time.Now().UTC()
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode handoff: %w", err)
	}
	if err := c.putObjectCond(ctx, key, "application/json", b, ifMatch, ifNoneMatch); err != nil {
		if errors.Is(err, ErrHandoffConflict) {
			return err
		}
		return fmt.Errorf("write handoff: %w", err)
	}
	return nil
}
