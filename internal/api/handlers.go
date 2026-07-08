// Package api wires the gin HTTP server: REST endpoints, auth, WebSocket status
// push, and serving the embedded frontend (design §3.5 / §4).
package api

import (
	"net/http"
	"strconv"

	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/failover"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/gin-gonic/gin"
)

// Handlers bundles the dependencies for API endpoints.
type Handlers struct {
	svc   *flink.Service
	store *store.Store // may be nil if S3 not configured
	cfg   *config.Config
	fo    *failover.Service // decentralized HA (nil if no HA groups)
}

// listJobs handles GET /api/jobs.
func (h *Handlers) listJobs(c *gin.Context) {
	jobs, err := h.svc.List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"jobs": jobs})
}

// getJob handles GET /api/jobs/:name.
func (h *Handlers) getJob(c *gin.Context) {
	detail, err := h.svc.Get(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, detail)
}

// getLogs handles GET /api/jobs/:name/logs?tail=N&component=jobmanager|taskmanager&pod=<name>.
func (h *Handlers) getLogs(c *gin.Context) {
	tail, _ := strconv.ParseInt(c.DefaultQuery("tail", "0"), 10, 64)
	component := c.DefaultQuery("component", "jobmanager")
	pod := c.Query("pod")
	logs, err := h.svc.Logs(c.Request.Context(), c.Param("name"), component, pod, tail)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"logs": logs})
}

// suspend/resume are immediate; restart is async (returns an operation).
func (h *Handlers) suspend(c *gin.Context) {
	h.doOp(c, func() error { return h.svc.Suspend(c.Request.Context(), c.Param("name")) })
}
func (h *Handlers) resume(c *gin.Context) {
	h.doOp(c, func() error { return h.svc.Resume(c.Request.Context(), c.Param("name")) })
}

// restart starts an async restart and returns the tracking operation.
func (h *Handlers) restart(c *gin.Context) {
	op := h.svc.StartRestart(c.Param("name"))
	c.JSON(http.StatusAccepted, op)
}

// doOp runs a mutating operation and returns a uniform response.
func (h *Handlers) doOp(c *gin.Context, fn func() error) {
	if err := fn(); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// savepoint starts an async savepoint and returns the tracking operation.
func (h *Handlers) savepoint(c *gin.Context) {
	op := h.svc.StartSavepoint(c.Param("name"))
	c.JSON(http.StatusAccepted, op)
}

// getOperation handles GET /api/operations/:id (poll async op progress/result).
func (h *Handlers) getOperation(c *gin.Context) {
	op, ok := h.svc.GetOperation(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
		return
	}
	c.JSON(http.StatusOK, op)
}

// rollbackRequest is the JSON body for rollback.
type rollbackRequest struct {
	Path string `json:"path"`
}

// rollback handles POST /api/jobs/:name/rollback.
func (h *Handlers) rollback(c *gin.Context) {
	var req rollbackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	if err := h.svc.Rollback(c.Request.Context(), c.Param("name"), req.Path); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// recoveryPoints handles GET /api/jobs/:name/recovery-points.
func (h *Handlers) recoveryPoints(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "S3 not configured"})
		return
	}
	name := c.Param("name")
	job := h.cfg.JobName(h.cfg.DeploymentName(name))
	// Prefer the deployment's own configured savepoint/checkpoint dirs so the
	// listing works regardless of bucket/prefix layout.
	spDir, cpDir, err := h.svc.RecoveryDirs(c.Request.Context(), name)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	points, err := h.store.ListRecoveryPoints(c.Request.Context(), job, spDir, cpDir)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"recoveryPoints": points})
}

// clusterInfo handles GET /api/cluster.
func (h *Handlers) clusterInfo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"name":             h.cfg.Cluster.Name,
		"namespace":        h.cfg.Cluster.Namespace,
		"s3Configured":     h.store != nil,
		"clusterReachable": h.svc.Reachable(c.Request.Context()),
	})
}

// healthz is a liveness probe: the process is up and serving.
func (h *Handlers) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz is a readiness probe. It reports "ready" normally and "degraded" when
// the target cluster API cannot be reached — but always returns 200 so a
// transient API blip does not remove the console from service (the console can
// still serve cached data and auth). The degraded flag is an operational signal
// for dashboards/alerts, not a hard failure.
func (h *Handlers) readyz(c *gin.Context) {
	reachable := h.svc.Reachable(c.Request.Context())
	status := "ready"
	if !reachable {
		status = "degraded"
	}
	c.JSON(http.StatusOK, gin.H{"status": status, "clusterReachable": reachable})
}
