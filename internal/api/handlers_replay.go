package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Replay-from-timestamp handlers (design docs/replay-from-timestamp-design.md).
// Routes are only registered when config.Replay.Enabled is true.

var (
	errAutoOffsetLatest  = errors.New("autoOffsetReset=latest is unsafe for replay (jumps to topic tail); use earliest")
	errAutoOffsetInvalid = errors.New("autoOffsetReset must be earliest or none")
)

// replayRequest is the body for preview / start. Either fromMillis or hoursAgo
// must be provided (fromMillis wins when both are set).
type replayRequest struct {
	FromMillis      int64   `json:"fromMillis"`
	HoursAgo        float64 `json:"hoursAgo"`
	SourceTable     string  `json:"sourceTable"`
	AutoOffsetReset string  `json:"autoOffsetReset"`
	ConfirmDrift    bool    `json:"confirmDrift"`
}

// resolveFromMillis returns the replay start epoch-millis and whether it is set.
func (r replayRequest) resolveFromMillis() (int64, bool) {
	if r.FromMillis > 0 {
		return r.FromMillis, true
	}
	if r.HoursAgo > 0 {
		return time.Now().Add(-time.Duration(r.HoursAgo * float64(time.Hour))).UnixMilli(), true
	}
	return 0, false
}

// getSQL handles GET /api/jobs/:name/sql.
func (h *Handlers) getSQL(c *gin.Context) {
	res, err := h.svc.GetJobSQL(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, res)
}

// getReplay handles GET /api/jobs/:name/replay (current session + verdict).
func (h *Handlers) getReplay(c *gin.Context) {
	sess, err := h.svc.ReplaySessionInfo(c.Request.Context(), c.Param("name"))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, sess)
}

// replayPreview handles POST /api/jobs/:name/replay/preview (no writes).
func (h *Handlers) replayPreview(c *gin.Context) {
	var req replayRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	from, ok := req.resolveFromMillis()
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fromMillis or hoursAgo is required"})
		return
	}
	aor, err := normalizeAutoOffsetReset(req.AutoOffsetReset)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	plan, err := h.svc.ReplayPreview(c.Request.Context(), c.Param("name"), req.SourceTable, from, aor)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, plan)
}

// startReplay handles POST /api/jobs/:name/replay (async).
func (h *Handlers) startReplay(c *gin.Context) {
	var req replayRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	if h.cfg.Replay.GitOpsManaged && !req.ConfirmDrift {
		c.JSON(http.StatusConflict, gin.H{
			"error":       "target is GitOps-managed: a live SQL edit will be reverted by the next sync. Set confirmDrift=true to proceed.",
			"gitopsDrift": true,
		})
		return
	}
	from, ok := req.resolveFromMillis()
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fromMillis or hoursAgo is required"})
		return
	}
	aor, err := normalizeAutoOffsetReset(req.AutoOffsetReset)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	op := h.svc.StartReplay(c.Param("name"), req.SourceTable, from, aor, c.GetString("user"))
	c.JSON(http.StatusAccepted, op)
}

// revertReplay handles POST /api/jobs/:name/replay/revert (async).
func (h *Handlers) revertReplay(c *gin.Context) {
	op := h.svc.StartRevert(c.Param("name"), c.GetString("user"))
	c.JSON(http.StatusAccepted, op)
}

// normalizeAutoOffsetReset defaults to earliest and rejects latest (the runbook
// warns that latest jumps past the replay window to the topic tail).
func normalizeAutoOffsetReset(v string) (string, error) {
	switch v {
	case "":
		return "earliest", nil
	case "earliest", "none":
		return v, nil
	case "latest":
		return "", errAutoOffsetLatest
	default:
		return "", errAutoOffsetInvalid
	}
}
