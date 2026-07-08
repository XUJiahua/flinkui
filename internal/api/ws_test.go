package api

import (
	"net/http"
	"testing"
)

func TestOriginAllowed(t *testing.T) {
	allowed := []string{"https://console.example.com", "dev.local:3000"}
	tests := []struct {
		name   string
		origin string
		host   string
		want   bool
	}{
		{"no origin (non-browser)", "", "flinkui:8080", true},
		{"same origin", "http://flinkui:8080", "flinkui:8080", true},
		{"same origin https", "https://flinkui", "flinkui", true},
		{"cross origin not allowed", "https://evil.example.com", "flinkui:8080", false},
		{"allowlisted full origin", "https://console.example.com", "flinkui:8080", true},
		{"allowlisted host only", "http://dev.local:3000", "flinkui:8080", true},
		{"malformed origin", "://bad", "flinkui:8080", false},
		{"host mismatch different port", "http://flinkui:9090", "flinkui:8080", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &http.Request{Header: http.Header{}, Host: tt.host}
			if tt.origin != "" {
				r.Header.Set("Origin", tt.origin)
			}
			if got := originAllowed(r, allowed); got != tt.want {
				t.Errorf("originAllowed(origin=%q, host=%q) = %v, want %v", tt.origin, tt.host, got, tt.want)
			}
		})
	}
}
