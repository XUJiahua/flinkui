package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// haEnabled guards the HA endpoints; 503 when no HA groups are configured.
func (h *Handlers) haEnabled(c *gin.Context) bool {
	if h.fo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no HA groups configured"})
		return false
	}
	return true
}

// listHA handles GET /api/ha (local view of all groups).
func (h *Handlers) listHA(c *gin.Context) {
	if !h.haEnabled(c) {
		return
	}
	views, err := h.fo.ListViews(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"groups": views})
}

// getHA handles GET /api/ha/:name.
func (h *Handlers) getHA(c *gin.Context) {
	if !h.haEnabled(c) {
		return
	}
	view, err := h.fo.LocalView(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, view)
}

type releaseRequest struct {
	Confirm bool `json:"confirm"`
}

type promoteRequest struct {
	Confirm     bool `json:"confirm"`
	Force       bool `json:"force"`
	AckDataLoss bool `json:"ackDataLoss"`
}

// release handles POST /api/ha/:name/release (local step-down).
func (h *Handlers) release(c *gin.Context) {
	if !h.haEnabled(c) {
		return
	}
	var req releaseRequest
	if err := c.ShouldBindJSON(&req); err != nil || !req.Confirm {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmation required: send {\"confirm\": true}"})
		return
	}
	task, err := h.fo.Release(c.Param("name"))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, task)
}

// promote handles POST /api/ha/:name/promote (local take-over).
func (h *Handlers) promote(c *gin.Context) {
	if !h.haEnabled(c) {
		return
	}
	var req promoteRequest
	if err := c.ShouldBindJSON(&req); err != nil || !req.Confirm {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmation required: send {\"confirm\": true}"})
		return
	}
	if req.Force && !req.AckDataLoss {
		c.JSON(http.StatusBadRequest, gin.H{"error": "force promote requires ackDataLoss: true"})
		return
	}
	task, err := h.fo.Promote(c.Param("name"), req.Force, req.AckDataLoss)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, task)
}

// claim handles POST /api/ha/:name/claim — cold-start bootstrap of the fencing
// token to THIS side (idempotent, no job restart).
func (h *Handlers) claim(c *gin.Context) {
	if !h.haEnabled(c) {
		return
	}
	var req releaseRequest // reuse {confirm}
	if err := c.ShouldBindJSON(&req); err != nil || !req.Confirm {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmation required: send {\"confirm\": true}"})
		return
	}
	if err := h.fo.Claim(c.Param("name")); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// getHATask handles GET /api/ha-tasks/:id.
func (h *Handlers) getHATask(c *gin.Context) {
	if !h.haEnabled(c) {
		return
	}
	task, ok := h.fo.GetTask(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "HA task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}
