package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// haGroupsEnabled guards the HA endpoints; returns false (and writes 503) when
// no HA groups are configured.
func (h *Handlers) haGroupsEnabled(c *gin.Context) bool {
	if h.fo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no HA groups configured"})
		return false
	}
	return true
}

// listHAGroups handles GET /api/ha-groups.
func (h *Handlers) listHAGroups(c *gin.Context) {
	if !h.haGroupsEnabled(c) {
		return
	}
	views, err := h.fo.ListViews(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"groups": views})
}

// getHAGroup handles GET /api/ha-groups/:name.
func (h *Handlers) getHAGroup(c *gin.Context) {
	if !h.haGroupsEnabled(c) {
		return
	}
	view, err := h.fo.GroupView(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, view)
}

// getHAGroupFencing handles GET /api/ha-groups/:name/fencing.
func (h *Handlers) getHAGroupFencing(c *gin.Context) {
	if !h.haGroupsEnabled(c) {
		return
	}
	view, err := h.fo.GroupView(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, view.Fencing)
}

// getHAGroupRecoveryPoints handles GET /api/ha-groups/:name/recovery-points.
func (h *Handlers) getHAGroupRecoveryPoints(c *gin.Context) {
	if !h.haGroupsEnabled(c) {
		return
	}
	points, err := h.fo.RecoveryPoints(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"recoveryPoints": points})
}
