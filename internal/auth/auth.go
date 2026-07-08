// Package auth provides a minimal login/session layer. The platform can operate
// on cluster workloads, so it must not be unauthenticated (design §6). MVP uses
// a single configured credential and an HMAC-signed session cookie. Fine-grained
// RBAC is on the roadmap (design §8).
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/fko-demo/flinkui/internal/config"
	"github.com/gin-gonic/gin"
)

const (
	cookieName    = "fko_session"
	sessionMaxAge = 12 * time.Hour
)

// Auth holds credentials and the signing secret.
type Auth struct {
	username     string
	password     string
	secret       []byte
	cookieSecure bool
}

// New builds an Auth from config.
func New(cfg config.AuthConfig) *Auth {
	return &Auth{
		username:     cfg.Username,
		password:     cfg.Password,
		secret:       []byte(cfg.SessionSecret),
		cookieSecure: cfg.CookieSecure,
	}
}

// LoginRequest is the JSON body for POST /api/login.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// sign returns "<exp>.<hmac>" for the given username and expiry.
func (a *Auth) sign(username string, exp int64) string {
	msg := fmt.Sprintf("%s.%d", username, exp)
	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(msg))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("%s.%d.%s", username, exp, sig)
}

// verify validates a token and returns the username if valid.
func (a *Auth) verify(token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	username, expStr, sig := parts[0], parts[1], parts[2]
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return "", false
	}
	expected := a.sign(username, exp)
	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		return "", false
	}
	_ = sig
	return username, true
}

// Login validates credentials and, on success, sets the session cookie.
func (a *Auth) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(a.username)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(req.Password), []byte(a.password)) == 1
	if !userOK || !passOK {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}
	exp := time.Now().Add(sessionMaxAge).Unix()
	token := a.sign(req.Username, exp)
	// SameSite=Lax mitigates CSRF on the state-changing POST endpoints while
	// keeping the same-origin SPA fully functional. Secure follows the deploy
	// protocol via FKO_AUTH_COOKIE_SECURE (enable behind TLS).
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(cookieName, token, int(sessionMaxAge.Seconds()), "/", "", a.cookieSecure, true)
	c.JSON(http.StatusOK, gin.H{"username": req.Username})
}

// Logout clears the session cookie.
func (a *Auth) Logout(c *gin.Context) {
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(cookieName, "", -1, "/", "", a.cookieSecure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// Me returns the current user if authenticated.
func (a *Auth) Me(c *gin.Context) {
	token, err := c.Cookie(cookieName)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}
	if user, ok := a.verify(token); ok {
		c.JSON(http.StatusOK, gin.H{"username": user})
		return
	}
	c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
}

// Middleware rejects unauthenticated requests to protected API routes.
func (a *Auth) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, err := c.Cookie(cookieName)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		user, ok := a.verify(token)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
			return
		}
		c.Set("user", user)
		c.Next()
	}
}

// TokenValid reports whether a raw cookie value is a valid session (used by the
// WebSocket handler, which reads the cookie directly).
func (a *Auth) TokenValid(token string) bool {
	_, ok := a.verify(token)
	return ok
}

// CookieName exposes the session cookie name.
func (a *Auth) CookieName() string { return cookieName }
