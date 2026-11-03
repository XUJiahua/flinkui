package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type leaseMemory struct {
	mu          sync.Mutex
	record      *HandoffRecord
	revision    int
	beforeWrite func()
	writeError  error
	loseAck     bool
	failRead    bool
}

func (m *leaseMemory) ReadHandoffWithETag(context.Context, string) (*HandoffRecord, string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failRead {
		return nil, "", false, errors.New("storage unavailable")
	}
	if m.record == nil {
		return nil, "", false, nil
	}
	return copyLeaseRecord(m.record), fmt.Sprint(m.revision), true, nil
}

func (m *leaseMemory) WriteHandoffCAS(_ context.Context, _ string, rec *HandoffRecord, expected string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.beforeWrite != nil {
		m.beforeWrite()
	}
	if m.writeError != nil {
		return m.writeError
	}
	if (m.record == nil && expected != "") || (m.record != nil && expected != fmt.Sprint(m.revision)) {
		return ErrHandoffConflict
	}
	m.record = copyLeaseRecord(rec)
	m.revision++
	if m.loseAck {
		return context.DeadlineExceeded
	}
	return nil
}

func copyLeaseRecord(rec *HandoffRecord) *HandoffRecord {
	copy := *rec
	state := *rec.Lease
	copy.Lease = &state
	return &copy
}

func testLease(t *testing.T, backend *leaseMemory, owner string, now func() time.Time) *Lease {
	t.Helper()
	l, err := newLease(backend, "orders-lease", "orders", owner, 10*time.Second, time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestLeaseContractKeepAliveExpiryAndTakeover(t *testing.T) {
	ctx := context.Background()
	backend := &leaseMemory{}
	base := time.Unix(1000, 0)
	elapsed := time.Duration(0)
	// Worst permitted clock separation: A is slow, B is fast.
	a := testLease(t, backend, "a", func() time.Time { return base.Add(elapsed - time.Second) })
	b := testLease(t, backend, "b", func() time.Time { return base.Add(elapsed + time.Second) })
	first, err := a.Acquire(ctx)
	if err != nil || first.Epoch != 1 {
		t.Fatalf("acquire = %+v, %v", first, err)
	}
	elapsed = 5 * time.Second
	renewed, err := a.Renew(ctx, first)
	if err != nil || renewed.Epoch != first.Epoch || renewed.session != first.session {
		t.Fatalf("renew = %+v, %v", renewed, err)
	}
	elapsed = 10 * time.Second
	if _, err := b.Acquire(ctx); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("renewed lease was stealable: %v", err)
	}
	elapsed = 14*time.Second - time.Nanosecond
	if !renewed.validAt(a.now()) {
		t.Fatal("holder expired before its safety deadline")
	}
	if _, err := b.Acquire(ctx); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("clock skew allowed overlapping grants: %v", err)
	}
	elapsed = 14 * time.Second
	if renewed.validAt(a.now()) {
		t.Fatal("old grant is still valid at the takeover boundary")
	}
	second, err := b.Acquire(ctx)
	if err != nil || second.Epoch != 2 {
		t.Fatalf("takeover = %+v, %v", second, err)
	}
	if _, err := a.Renew(ctx, renewed); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old owner renewed after takeover: %v", err)
	}
}

func TestLeaseContractRevokeRetainsEpochAcrossRestart(t *testing.T) {
	ctx := context.Background()
	backend := &leaseMemory{}
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	a := testLease(t, backend, "a", clock)
	first, err := a.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Revoke(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Renew(ctx, first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("revoked session renewed: %v", err)
	}
	restarted := testLease(t, backend, "a", clock)
	if _, err := restarted.Acquire(ctx); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("revoke skipped the cached grant safety window: %v", err)
	}
	now = now.Add(11 * time.Second)
	second, err := restarted.Acquire(ctx)
	if err != nil || second.Epoch != 2 || second.session == first.session {
		t.Fatalf("restart acquisition = %+v, %v", second, err)
	}
}

func TestLeaseResolvesLostAcknowledgementAndFailsClosedOnUnavailableStorage(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	backend := &leaseMemory{loseAck: true}
	l := testLease(t, backend, "a", func() time.Time { return now })
	first, err := l.Acquire(ctx)
	if err != nil || first.Epoch != 1 {
		t.Fatalf("lost acknowledgement was not resolved: %+v, %v", first, err)
	}
	backend.loseAck = false
	backend.writeError = errors.New("storage unavailable")
	now = now.Add(time.Second)
	if got, err := l.Renew(ctx, first); !errors.Is(err, ErrLeaseLost) || got.Epoch != 0 {
		t.Fatalf("renewal during outage issued a grant: %+v, %v", got, err)
	}
	backend.failRead = true
	if got, err := l.Acquire(ctx); !errors.Is(err, ErrLeaseLost) || got.Epoch != 0 {
		t.Fatalf("acquisition during outage issued a grant: %+v, %v", got, err)
	}
}

func TestLeaseLateRenewalDoesNotReauthorizeExpiredSession(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	backend := &leaseMemory{}
	l := testLease(t, backend, "a", func() time.Time { return now })
	first, err := l.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(8 * time.Second)
	backend.beforeWrite = func() { now = now.Add(2 * time.Second) }
	if got, err := l.Renew(ctx, first); !errors.Is(err, ErrLeaseLost) || got.Epoch != 0 {
		t.Fatalf("late renewal revived the expired grant: %+v, %v", got, err)
	}
}

func TestLeaseConcurrentAcquireHasOneWinner(t *testing.T) {
	ctx := context.Background()
	backend := &leaseMemory{}
	now := time.Unix(1000, 0)
	const contenders = 32
	results := make(chan error, contenders)
	for i := range contenders {
		l := testLease(t, backend, fmt.Sprint(i), func() time.Time { return now })
		go func() {
			_, err := l.Acquire(ctx)
			results <- err
		}()
	}
	winners := 0
	for range contenders {
		switch err := <-results; {
		case err == nil:
			winners++
		case errors.Is(err, ErrLeaseBusy), errors.Is(err, ErrHandoffConflict):
		default:
			t.Errorf("unexpected acquisition result: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("got %d winners, want 1", winners)
	}
}
