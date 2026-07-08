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
//
// Hardening (design §10):
//   - Sets X-Forwarded-{Proto,Host,Prefix} so the upstream can build correct
//     absolute links, and rewrites the Host to the target.
//   - Rewrites redirect Location headers to keep our /api/jobs/:name/ui prefix so
//     a redirect to "/" does not bounce the browser out of the proxied view.
//   - httputil.ReverseProxy transparently proxies WebSocket upgrades (Go ≥1.12),
//     which some Flink UI versions use.
//
// Known limitation: the Flink Web UI serves some root-absolute asset paths
// (e.g. "/assets/...") that a prefix-stripping proxy cannot rewrite without
// parsing/rewriting HTML and JS. Those may 404 under the proxy on some Flink
// versions. Deep-linking to the native UI (flinkUIInfo.target) remains the
// reliable path; per-version behavior should be validated against a live cluster.
func (h *Handlers) flinkUIProxy(c *gin.Context) {
	dep := h.cfg.DeploymentName(c.Param("name"))
	target, err := url.Parse(h.flinkUITarget(dep))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	prefix := fmt.Sprintf("/api/jobs/%s/ui", c.Param("name"))
	proxy := httputil.NewSingleHostReverseProxy(target)

	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		// Strip our proxy prefix so the JobManager sees a normal root path.
		req.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		req.Host = target.Host
		// Forwarded headers for correct upstream link/redirect construction.
		req.Header.Set("X-Forwarded-Host", c.Request.Host)
		req.Header.Set("X-Forwarded-Prefix", prefix)
		proto := "http"
		if c.Request.TLS != nil || strings.EqualFold(c.GetHeader("X-Forwarded-Proto"), "https") {
			proto = "https"
		}
		req.Header.Set("X-Forwarded-Proto", proto)
	}

	// Keep redirects inside the proxied namespace: prepend our prefix to
	// root-relative Location headers that don't already carry it.
	proxy.ModifyResponse = func(resp *http.Response) error {
		loc := resp.Header.Get("Location")
		if loc == "" {
			return nil
		}
		lu, perr := url.Parse(loc)
		if perr != nil {
			return nil
		}
		if !lu.IsAbs() && strings.HasPrefix(lu.Path, "/") && !strings.HasPrefix(lu.Path, prefix+"/") && lu.Path != prefix {
			lu.Path = prefix + lu.Path
			resp.Header.Set("Location", lu.String())
		}
		return nil
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, "cannot reach Flink Web UI at %s: %v", target, e)
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}
