package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

const (
	phaseLeased       = "leased"
	phaseLeaseRevoked = "lease_revoked"
)

var (
	ErrLeaseBusy = errors.New("lease is held or its safety window has not elapsed")
	ErrLeaseLost = errors.New("lease authority is unavailable or expired")
)

// LeaseState is persisted alongside the fencing epoch in one CAS object.
type LeaseState struct {
	Session   string    `json:"session"`
	Mutation  string    `json:"mutation"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// LeaseGrant identifies one acquisition. Its deadline is local to the process
// that received it. A restarted process must acquire a new epoch.
type LeaseGrant struct {
	Epoch    int64
	session  string
	deadline time.Time
	cutoff   time.Time
}

func (g LeaseGrant) validAt(now time.Time) bool {
	return now.Before(g.deadline) && now.Before(g.cutoff)
}

type leaseStore interface {
	ReadHandoffWithETag(context.Context, string) (*HandoffRecord, string, bool, error)
	WriteHandoffCAS(context.Context, string, *HandoffRecord, string) error
}

// Lease coordinates one group using a dedicated S3 object. The object must
// never be deleted or restored to an older version. Every participant must
// share the same TTL and maximum absolute clock-error bound.
type Lease struct {
	store             leaseStore
	key, group, owner string
	ttl, clockError   time.Duration
	now               func() time.Time
}

func NewLease(coord *Coord, key, group, owner string, ttl, clockError time.Duration) (*Lease, error) {
	if coord == nil {
		return nil, fmt.Errorf("lease requires S3 coordination")
	}
	return newLease(coord, key, group, owner, ttl, clockError, time.Now)
}

func newLease(store leaseStore, key, group, owner string, ttl, clockError time.Duration, now func() time.Time) (*Lease, error) {
	if key == "" || group == "" || owner == "" || ttl <= 0 || clockError <= 0 || clockError >= ttl/2 {
		return nil, fmt.Errorf("lease requires a key, group, owner and TTL greater than twice the positive clock error bound")
	}
	return &Lease{store: store, key: key, group: group, owner: owner, ttl: ttl, clockError: clockError, now: now}, nil
}

func (l *Lease) Acquire(ctx context.Context) (LeaseGrant, error) {
	rec, etag, exists, err := l.read(ctx)
	if err != nil {
		return LeaseGrant{}, err
	}
	now := l.now()
	epoch := int64(1)
	if exists {
		if now.Before(rec.Lease.ExpiresAt.Add(l.clockError)) {
			return LeaseGrant{}, ErrLeaseBusy
		}
		if rec.Epoch == math.MaxInt64 {
			return LeaseGrant{}, fmt.Errorf("lease epoch exhausted")
		}
		epoch = rec.Epoch + 1
	}
	next := &HandoffRecord{
		Group: l.group, ActiveClusterID: l.owner, Epoch: epoch, Phase: phaseLeased,
		Lease: &LeaseState{Session: uuid.NewString(), ExpiresAt: now.Add(l.ttl).UTC()},
	}
	if err := l.commit(ctx, next, etag); err != nil {
		return LeaseGrant{}, err
	}
	return l.confirmGrant(now, next)
}

func (l *Lease) Renew(ctx context.Context, grant LeaseGrant) (LeaseGrant, error) {
	rec, etag, now, err := l.readOwned(ctx, grant)
	if err != nil {
		return LeaseGrant{}, err
	}
	expires := now.Add(l.ttl).UTC()
	if expires.After(rec.Lease.ExpiresAt) {
		rec.Lease.ExpiresAt = expires
	}
	if err := l.commit(ctx, rec, etag); err != nil {
		return LeaseGrant{}, err
	}
	if !grant.validAt(l.now()) {
		return LeaseGrant{}, ErrLeaseLost
	}
	return l.confirmGrant(now, rec)
}

// Revoke retires the session without erasing its epoch. Successors still wait
// through the original expiry window, so a cached grant cannot overlap them.
func (l *Lease) Revoke(ctx context.Context, grant LeaseGrant) error {
	rec, etag, _, err := l.readOwned(ctx, grant)
	if err != nil {
		return err
	}
	rec.Phase = phaseLeaseRevoked
	return l.commit(ctx, rec, etag)
}

func (l *Lease) read(ctx context.Context) (*HandoffRecord, string, bool, error) {
	rec, etag, exists, err := l.store.ReadHandoffWithETag(ctx, l.key)
	if err != nil {
		return nil, "", false, fmt.Errorf("%w: read lease: %w", ErrLeaseLost, err)
	}
	if exists && (rec == nil || etag == "" || rec.Group != l.group || rec.Epoch < 1 || rec.ActiveClusterID == "" ||
		rec.Lease == nil || rec.Lease.Session == "" || rec.Lease.Mutation == "" || rec.Lease.ExpiresAt.IsZero() ||
		(rec.Phase != phaseLeased && rec.Phase != phaseLeaseRevoked)) {
		return nil, "", false, fmt.Errorf("%w: invalid lease record", ErrLeaseLost)
	}
	return rec, etag, exists, nil
}

func (l *Lease) readOwned(ctx context.Context, grant LeaseGrant) (*HandoffRecord, string, time.Time, error) {
	if !grant.validAt(l.now()) {
		return nil, "", time.Time{}, ErrLeaseLost
	}
	rec, etag, exists, err := l.read(ctx)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	now := l.now()
	if !exists || rec.Phase != phaseLeased || rec.ActiveClusterID != l.owner || rec.Epoch != grant.Epoch ||
		rec.Lease.Session != grant.session || !grant.validAt(now) ||
		!now.Before(rec.Lease.ExpiresAt.Add(-l.clockError)) {
		return nil, "", time.Time{}, ErrLeaseLost
	}
	return rec, etag, now, nil
}

func (l *Lease) confirmGrant(start time.Time, rec *HandoffRecord) (LeaseGrant, error) {
	cutoff := rec.Lease.ExpiresAt.Add(-l.clockError)
	// Retain the local monotonic clock while also checking the wall clock
	// against the persisted cutoff, including after a system suspend.
	deadline := start.Add(cutoff.Sub(start))
	now := l.now()
	if !now.Before(deadline) || !now.Before(cutoff) {
		return LeaseGrant{}, ErrLeaseLost
	}
	return LeaseGrant{Epoch: rec.Epoch, session: rec.Lease.Session, deadline: deadline, cutoff: cutoff}, nil
}

// commit resolves a lost acknowledgement by reading back the unique mutation.
// A conflicting writer's state can never be mistaken for this operation.
func (l *Lease) commit(ctx context.Context, rec *HandoffRecord, etag string) error {
	rec.Lease.Mutation = uuid.NewString()
	if err := l.store.WriteHandoffCAS(ctx, l.key, rec, etag); err != nil {
		current, _, exists, readErr := l.read(ctx)
		if readErr == nil && exists && current.Lease.Mutation == rec.Lease.Mutation {
			return nil
		}
		return fmt.Errorf("%w: lease CAS unconfirmed: %w", ErrLeaseLost, err)
	}
	return nil
}
