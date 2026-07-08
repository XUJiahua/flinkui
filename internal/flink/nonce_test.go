package flink

import (
	"sync"
	"testing"
)

func TestNextRedeployNonceMonotonicUnique(t *testing.T) {
	const n = 5000
	var wg sync.WaitGroup
	results := make([]int64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = NextRedeployNonce()
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]struct{}, n)
	for _, v := range results {
		if _, dup := seen[v]; dup {
			t.Fatalf("duplicate nonce %d generated under concurrency", v)
		}
		seen[v] = struct{}{}
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique nonces, got %d", n, len(seen))
	}

	// Sequential calls must be strictly increasing.
	a := NextRedeployNonce()
	b := NextRedeployNonce()
	if b <= a {
		t.Fatalf("nonce not increasing: a=%d b=%d", a, b)
	}
}

func TestClassifyHealth(t *testing.T) {
	tests := []struct {
		name      string
		jobState  string
		lifecycle string
		specState string
		want      string
	}{
		{"running stable", "RUNNING", "STABLE", "running", HealthHealthy},
		{"job failed", "FAILED", "DEPLOYED", "running", HealthDegraded},
		{"job failing", "FAILING", "STABLE", "running", HealthDegraded},
		{"lifecycle failed", "RUNNING", "FAILED", "running", HealthDegraded},
		{"rolled back", "RUNNING", "ROLLED_BACK", "running", HealthDegraded},
		{"upgrading", "RECONCILING", "UPGRADING", "running", HealthProgressing},
		{"rolling back", "RECONCILING", "ROLLING_BACK", "running", HealthProgressing},
		{"suspended spec", "", "SUSPENDED", "suspended", HealthSuspended},
		{"suspended jobstate", "SUSPENDED", "STABLE", "running", HealthSuspended},
		{"reconciling", "RECONCILING", "DEPLOYED", "running", HealthProgressing},
		{"running not yet stable", "RUNNING", "DEPLOYED", "running", HealthProgressing},
		{"finished", "FINISHED", "STABLE", "running", HealthStopped},
		{"canceled", "CANCELED", "STABLE", "running", HealthStopped},
		{"empty coming up", "", "", "running", HealthProgressing},
		{"empty unknown", "", "", "", HealthUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyHealth(tt.jobState, tt.lifecycle, tt.specState); got != tt.want {
				t.Errorf("classifyHealth(%q,%q,%q) = %q, want %q",
					tt.jobState, tt.lifecycle, tt.specState, got, tt.want)
			}
		})
	}
}
