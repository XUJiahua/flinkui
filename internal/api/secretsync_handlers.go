package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// secretSyncStatus handles GET /api/secretsync — returns the OpenBao secret-sync
// loop status, or {enabled:false} when the feature is off.
func (h *Handlers) secretSyncStatus(c *gin.Context) {
	if h.ss == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	c.JSON(http.StatusOK, h.ss.Status())
}

// secretSyncNow handles POST /api/secretsync/sync — triggers an immediate sync
// and returns the resulting status. 409 when the feature is disabled.
func (h *Handlers) secretSyncNow(c *gin.Context) {
	if h.ss == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "secret-sync is not enabled"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	h.ss.SyncNow(ctx)
	c.JSON(http.StatusOK, h.ss.Status())
}
