package api

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
)

// flinkUITarget builds the in-cluster JM REST service URL for a deployment.
// The Flink Operator exposes a "<deployment>-rest" Service on 8081.
func (h *Handlers) flinkUITarget(dep string) string {
	return fmt.Sprintf("http://%s-rest.%s:8081", dep, h.cfg.Cluster.Namespace)
}

// flinkUIInfo handles GET /api/jobs/:name/flink-ui and returns the reverse-proxy
// base path plus the underlying target (for display).
func (h *Handlers) flinkUIInfo(c *gin.Context) {
	dep := h.cfg.DeploymentName(c.Param("name"))
	c.JSON(http.StatusOK, gin.H{
		"proxyPath": fmt.Sprintf("/api/jobs/%s/ui/", c.Param("name")),
		"target":    h.flinkUITarget(dep),
	})
}

// flinkUIProxy reverse-proxies to the JobManager REST/Web UI service. This works
// when the backend can reach the ClusterIP (in-cluster form, design §3.4). In
// the out-of-cluster form it may be unreachable; that tradeoff is documented.
func (h *Handlers) flinkUIProxy(c *gin.Context) {
	dep := h.cfg.DeploymentName(c.Param("name"))
	target, err := url.Parse(h.flinkUITarget(dep))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	prefix := fmt.Sprintf("/api/jobs/%s/ui", c.Param("name"))
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, "cannot reach Flink Web UI at %s: %v", target, e)
	}
	// Strip the proxy prefix so the JM sees a normal path.
	c.Request.URL.Path = strings.TrimPrefix(c.Request.URL.Path, prefix)
	if c.Request.URL.Path == "" {
		c.Request.URL.Path = "/"
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}
