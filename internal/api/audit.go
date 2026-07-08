package api

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// auditLogger emits structured audit records to stdout as JSON. Compliance needs
// a "who changed what, when, and with what result" trail for every mutating
// operation (suspend/resume/restart/savepoint/rollback/release/promote). This is
// the minimum bar (structured log); persisting to S3 / CR annotations is a
// straightforward follow-up on top of the same event shape.
var auditLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

// auditMiddleware logs an audit event for every state-changing request. It runs
// after the auth middleware, so the authenticated user is available in context.
// Read-only requests (GET/HEAD/OPTIONS) are skipped to keep the trail signal
// focused on mutations.
func auditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !isMutating(c.Request.Method) {
			c.Next()
			return
		}
		start := time.Now()
		c.Next()

		user, _ := c.Get("user")
		username, _ := user.(string)
		if username == "" {
			username = "anonymous"
		}
		// The :name param covers jobs and HA groups; fall back to empty.
		resource := c.Param("name")

		attrs := []any{
			slog.String("event", "audit"),
			slog.String("user", username),
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("operation", operationFromPath(c.Request.URL.Path)),
			slog.String("resource", resource),
			slog.Int("status", c.Writer.Status()),
			slog.Int64("durationMs", time.Since(start).Milliseconds()),
			slog.String("clientIP", c.ClientIP()),
		}
		if len(c.Errors) > 0 {
			attrs = append(attrs, slog.String("error", c.Errors.String()))
		}
		auditLogger.LogAttrs(c.Request.Context(), slog.LevelInfo, "mutating operation", attrsToAny(attrs)...)
	}
}

func isMutating(method string) bool {
	switch method {
	case "POST", "PUT", "PATCH", "DELETE":
		return true
	default:
		return false
	}
}

// operationFromPath derives a coarse operation label from the request path, e.g.
// "/api/jobs/x/suspend" -> "suspend", "/api/ha/g/promote" -> "promote".
func operationFromPath(path string) string {
	trimmed := strings.Trim(path, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// attrsToAny is a tiny shim so we can build the attribute slice conditionally
// while still calling LogAttrs with slog.Attr values.
func attrsToAny(attrs []any) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if at, ok := a.(slog.Attr); ok {
			out = append(out, at)
		}
	}
	return out
}
