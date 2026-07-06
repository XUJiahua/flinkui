// Package api wires the gin HTTP server: REST endpoints, auth, WebSocket status
// push, and serving the embedded frontend (design §3.5 / §4).
package api

import (
	"net/http"
	"strconv"

	"github.com/fko-demo/flinkui/internal/config"
	"github.com/fko-demo/flinkui/internal/flink"
	"github.com/fko-demo/flinkui/internal/store"
	"github.com/gin-gonic/gin"
)

// Handlers bundles the dependencies for API endpoints.
type Handlers struct {
	svc   *flink.Service
	store *store.Store // may be nil if S3 not configured
	cfg   *config.Config
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

// getLogs handles GET /api/jobs/:name/logs?tail=N.
func (h *Handlers) getLogs(c *gin.Context) {
	tail, _ := strconv.ParseInt(c.DefaultQuery("tail", "0"), 10, 64)
	logs, err := h.svc.Logs(c.Request.Context(), c.Param("name"), tail)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"logs": logs})
}

// suspend/resume/restart handlers.
func (h *Handlers) suspend(c *gin.Context) {
	h.doOp(c, func() error { return h.svc.Suspend(c.Request.Context(), c.Param("name")) })
}
func (h *Handlers) resume(c *gin.Context) {
	h.doOp(c, func() error { return h.svc.Resume(c.Request.Context(), c.Param("name")) })
}
func (h *Handlers) restart(c *gin.Context) {
	h.doOp(c, func() error { return h.svc.Restart(c.Request.Context(), c.Param("name")) })
}

// doOp runs a mutating operation and returns a uniform response.
func (h *Handlers) doOp(c *gin.Context, fn func() error) {
	if err := fn(); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// savepoint handles POST /api/jobs/:name/savepoint.
func (h *Handlers) savepoint(c *gin.Context) {
	res, err := h.svc.Savepoint(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, res)
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
	job := h.cfg.JobName(h.cfg.DeploymentName(c.Param("name")))
	points, err := h.store.ListRecoveryPoints(c.Request.Context(), job)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"recoveryPoints": points})
}

// clusterInfo handles GET /api/cluster.
func (h *Handlers) clusterInfo(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"name":         h.cfg.Cluster.Name,
		"namespace":    h.cfg.Cluster.Namespace,
		"s3Configured": h.store != nil,
	})
}
