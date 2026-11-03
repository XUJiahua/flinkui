package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// requireUnmanagedJob keeps generic activation paths outside the HA state
// machine. A read-only owner check would race Release between admission and
// the Kubernetes write, especially for queued asynchronous operations.
func (h *Handlers) requireUnmanagedJob(c *gin.Context) {
	if h.cfg.IsLocalHADeployment(h.cfg.DeploymentName(c.Param("name"))) {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": "this operation requires an HA-coordinated workflow for the managed job",
			"code":  "ha_managed_job",
		})
		return
	}
	c.Next()
}

// requireReadOnlyHAProxy preserves the monitoring UI while guarding the
// JobManager REST API methods that can submit or change jobs.
func (h *Handlers) requireReadOnlyHAProxy(c *gin.Context) {
	switch c.Request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		c.Next()
	default:
		h.requireUnmanagedJob(c)
	}
}
