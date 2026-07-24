package api

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/fko-demo/flinkui/internal/auth"
	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/failover"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/secretsync"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/gin-gonic/gin"
)

// Server holds the configured gin engine.
type Server struct {
	engine *gin.Engine
	addr   string
}

// New builds the HTTP server: API routes, auth, WebSocket, and SPA static serving.
func New(cfg *config.Config, svc *flink.Service, st *store.Store, fo *failover.Service, ss *secretsync.Syncer, a *auth.Auth, staticFS fs.FS) *Server {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	h := &Handlers{svc: svc, store: st, cfg: cfg, fo: fo, ss: ss}
	hub := newStatusHub(h, a, cfg.StatusPollSec)

	// Public health endpoints (no auth) for liveness/readiness probes.
	r.GET("/healthz", h.healthz)
	r.GET("/readyz", h.readyz)

	// Public auth endpoints.
	r.POST("/api/login", a.Login)
	r.POST("/api/logout", a.Logout)
	r.GET("/api/me", a.Me)

	// Protected API.
	api := r.Group("/api")
	api.Use(a.Middleware())
	// Audit trail for mutating operations (runs after auth so the user is known).
	api.Use(auditMiddleware())
	{
		api.GET("/cluster", h.clusterInfo)
		api.GET("/jobs", h.listJobs)
		api.GET("/jobs/:name", h.getJob)
		api.GET("/jobs/:name/logs", h.getLogs)
		api.GET("/jobs/:name/metrics", h.getMetrics)
		api.GET("/jobs/:name/recovery-points", h.recoveryPoints)
		api.GET("/jobs/:name/flink-ui", h.flinkUIInfo)
		api.Any("/jobs/:name/ui/*path", h.flinkUIProxy)

		api.POST("/jobs/:name/suspend", h.suspend)
		api.POST("/jobs/:name/resume", h.resume)
		api.POST("/jobs/:name/restart", h.restart)
		api.POST("/jobs/:name/savepoint", h.savepoint)
		api.POST("/jobs/:name/rollback", h.rollback)

		// Async operation status (savepoint / restart progress).
		api.GET("/operations/:id", h.getOperation)

		// Decentralized HA (failover-decentralized): local observation + switch.
		api.GET("/ha", h.listHA)
		api.GET("/ha/:name", h.getHA)
		api.POST("/ha/:name/claim", h.claim)
		api.POST("/ha/:name/release", h.release)
		api.POST("/ha/:name/promote", h.promote)
		api.GET("/ha-tasks/:id", h.getHATask)

		// OpenBao/Vault secret-sync (no ESO): status + manual trigger.
		api.GET("/secretsync", h.secretSyncStatus)
		api.POST("/secretsync/sync", h.secretSyncNow)
	}

	// WebSocket status stream (auth handled inside via cookie).
	r.GET("/api/ws/status", hub.handle)

	// Static frontend (embedded) with SPA fallback.
	registerStatic(r, staticFS)

	return &Server{engine: r, addr: cfg.Addr}
}

// Run starts the HTTP server.
func (s *Server) Run() error {
	return s.engine.Run(s.addr)
}

// Handler exposes the underlying http.Handler (useful for tests).
func (s *Server) Handler() http.Handler {
	return s.engine
}

// registerStatic serves the embedded static export and falls back to index.html
// for client-side routes (design "部署": SPA fallback).
func registerStatic(r *gin.Engine, staticFS fs.FS) {
	fileServer := http.FileServer(http.FS(staticFS))

	r.NoRoute(func(c *gin.Context) {
		reqPath := strings.TrimPrefix(c.Request.URL.Path, "/")

		// Never fall back for API routes.
		if strings.HasPrefix(reqPath, "api/") {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}

		// Serve the file if it exists; otherwise fall back to index.html.
		if reqPath == "" {
			reqPath = "index.html"
		}
		if _, err := fs.Stat(staticFS, reqPath); err != nil {
			// Try "<path>.html" (Next.js static export names routes like about.html).
			if _, err2 := fs.Stat(staticFS, reqPath+".html"); err2 == nil {
				c.Request.URL.Path = "/" + reqPath + ".html"
				fileServer.ServeHTTP(c.Writer, c.Request)
				return
			}
			c.Request.URL.Path = "/index.html"
		}
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
}
