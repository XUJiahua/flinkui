package flink

import (
	"sync/atomic"
	"time"
)

// lastRedeployNonce backs NextRedeployNonce. Seeded from the wall clock so
// nonces keep increasing across process restarts.
var lastRedeployNonce int64 = time.Now().UnixNano()

// NextRedeployNonce returns a strictly increasing, unique value for
// spec.job.savepointRedeployNonce. A second-resolution timestamp
// (time.Now().Unix()) collides when two redeploys land in the same second: the
// operator sees an unchanged nonce and skips the second redeploy, so the UI
// reports success while nothing happened (silent failure). Using an atomic
// monotonic counter guarantees every redeploy carries a distinct, ordered nonce.
func NextRedeployNonce() int64 {
	for {
		prev := atomic.LoadInt64(&lastRedeployNonce)
		next := time.Now().UnixNano()
		if next <= prev {
			next = prev + 1
		}
		if atomic.CompareAndSwapInt64(&lastRedeployNonce, prev, next) {
			return next
		}
	}
}
