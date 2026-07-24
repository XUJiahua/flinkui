package api

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// statusHub pushes periodic dashboard snapshots over WebSocket (design §3.3).
// MVP uses lightweight polling on an interval; informer/watch is a roadmap item.
type statusHub struct {
	h        *Handlers
	auth     *auth.Auth
	interval time.Duration
	upgrader websocket.Upgrader
}

func newStatusHub(h *Handlers, a *auth.Auth, pollSec int) *statusHub {
	if pollSec <= 0 {
		pollSec = 5
	}
	allowed := h.cfg.AllowedOriginList()
	return &statusHub{
		h:        h,
		auth:     a,
		interval: time.Duration(pollSec) * time.Second,
		upgrader: websocket.Upgrader{
			// Reject cross-origin WebSocket upgrades: only same-origin (the
			// embedded SPA) plus any explicitly allow-listed origins may connect.
			// This closes the CSRF/cross-site WS hijack surface that a blanket
			// "return true" leaves open.
			CheckOrigin: func(r *http.Request) bool { return originAllowed(r, allowed) },
		},
	}
}

// originAllowed reports whether a WebSocket upgrade request's Origin is trusted.
// A missing Origin (non-browser client) is allowed; a browser Origin must match
// the request Host (same-origin) or an entry in the configured allowlist.
func originAllowed(r *http.Request, allowed []string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // non-browser client (no Origin header)
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	// Same-origin: the Origin host must equal the request Host.
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	lowered := strings.ToLower(origin)
	loweredHost := strings.ToLower(u.Host)
	for _, a := range allowed {
		// Match either the full "scheme://host[:port]" or just the host.
		if a == lowered || a == loweredHost {
			return true
		}
	}
	return false
}

// handle upgrades to WebSocket and streams job summaries until the client leaves.
func (s *statusHub) handle(c *gin.Context) {
	// Authenticate via the session cookie (WS can't use the JSON middleware body).
	if token, err := c.Cookie(s.auth.CookieName()); err != nil || !s.auth.TokenValid(token) {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}

	conn, err := s.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ctx := c.Request.Context()
	push := func() bool {
		jobs, err := s.h.svc.List(ctx)
		payload := gin.H{"jobs": jobs}
		if err != nil {
			payload = gin.H{"error": err.Error()}
		}
		// Include OpenBao secret-sync status in the same frame so the Secrets
		// page can render live without a separate REST poll.
		if s.h.ss != nil {
			payload["secretSync"] = s.h.ss.Status()
		}
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteJSON(payload) == nil
	}

	// Immediate first push, then on the interval.
	if !push() {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Reader goroutine to detect client close.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-closed:
			return
		case <-ticker.C:
			if !push() {
				return
			}
		}
	}
}
