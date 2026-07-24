package cluster

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestListEventsOrderedByTimeDesc verifies events come back most-recent-first
// based on the real timestamp, not the lexical order of the formatted "age"
// string (where e.g. "9m ago" would sort after "10m ago").
func TestListEventsOrderedByTimeDesc(t *testing.T) {
	now := time.Now()
	mk := func(reason string, ago time.Duration) *corev1.Event {
		ts := metav1.NewTime(now.Add(-ago))
		return &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: reason, Namespace: "flink-operator"},
			InvolvedObject: corev1.ObjectReference{Name: "job-a"},
			Reason:         reason,
			LastTimestamp:  ts,
			Type:           "Normal",
		}
	}
	// Insert deliberately out of order, including the tricky 9m/10m case.
	cs := fake.NewSimpleClientset(
		mk("ten-min", 10*time.Minute),
		mk("thirty-sec", 30*time.Second),
		mk("nine-min", 9*time.Minute),
		mk("two-hour", 2*time.Hour),
	)
	k := &KubeAccessor{clientset: cs, namespace: "flink-operator"}

	got, err := k.ListEvents(context.Background(), "job-a")
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	want := []string{"thirty-sec", "nine-min", "ten-min", "two-hour"}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Reason != want[i] {
			t.Errorf("position %d: got %q, want %q (full order: %v)", i, got[i].Reason, want[i], reasons(got))
		}
	}
}

func reasons(evs []EventInfo) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Reason
	}
	return out
}
