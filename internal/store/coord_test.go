package store

import (
	"errors"
	"testing"
)

func TestIsPreconditionFailed(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("connection refused"), false},
		{"named 412", errors.New("api error PreconditionFailed: At least one of the preconditions you specified did not hold"), true},
		{"StatusCode 412", errors.New("operation error S3: PutObject, https response error StatusCode: 412"), true},
		{"lowercase status code 412", errors.New("status code: 412"), true},
		{"404 is not a precondition failure", errors.New("StatusCode: 404"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPreconditionFailed(tt.err); got != tt.want {
				t.Errorf("isPreconditionFailed(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
