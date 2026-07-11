// Package web provides an embedded web dashboard for Flare — REST API +
// WebSocket real-time push, served on a configurable port alongside the mesh.
package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

// session represents an authenticated browser session.
type session struct {
	token     string
	expiresAt time.Time
}

// sessionExpiry is how long a session lives before requiring re-login.
const sessionExpiry = 24 * time.Hour

// Auth manages dashboard authentication (username/password + session cookies).
type Auth struct {
	username string
	password string
	sessions map[string]*session
	mu       sync.Mutex
}

// NewAuth creates an Auth. Pass empty strings to disable auth entirely.
func NewAuth(username, password string) *Auth {
	return &Auth{
		username: username,
		password: password,
		sessions: make(map[string]*session),
	}
}

// Enabled returns true when username/password are configured.
func (a *Auth) Enabled() bool {
	return a.username != "" && a.password != ""
}

// --- session tokens -----------------------------------------------------------

func (a *Auth) generateToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// Fallback (should never fail on sane systems)
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(b)
}

func (a *Auth) newSession() *session {
	return &session{
		token:     a.generateToken(),
		expiresAt: time.Now().Add(sessionExpiry),
	}
}

// --- HTTP handlers ------------------------------------------------------------

// Login handles POST /api/login — validates credentials and sets a session cookie.
func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	if !a.Enabled() {
		writeJSON(w, 200, map[string]string{"status": "ok"})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, 405, "method not allowed")
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")

	ok := subtle.ConstantTimeCompare([]byte(username), []byte(a.username)) == 1 &&
		subtle.ConstantTimeCompare([]byte(password), []byte(a.password)) == 1

	if !ok {
		writeError(w, 401, "invalid credentials")
		return
	}

	a.mu.Lock()
	sess := a.newSession()
	a.sessions[sess.token] = sess
	a.mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     "flare_session",
		Value:    sess.token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionExpiry.Seconds()),
	})
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// Logout handles POST /api/logout — clears the session cookie.
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("flare_session"); err == nil {
		a.mu.Lock()
		delete(a.sessions, cookie.Value)
		a.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "flare_session",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		MaxAge:   -1,
	})
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// CheckSession returns the authenticated status of the current request.
func (a *Auth) CheckSession(w http.ResponseWriter, r *http.Request) bool {
	if !a.Enabled() {
		return true
	}

	cookie, err := r.Cookie("flare_session")
	if err != nil {
		return false
	}

	a.mu.Lock()
	sess, ok := a.sessions[cookie.Value]
	if !ok || time.Now().After(sess.expiresAt) {
		if ok {
			delete(a.sessions, cookie.Value)
		}
		a.mu.Unlock()
		return false
	}
	a.mu.Unlock()
	return true
}

// Middleware wraps a handler with auth checking.
// Unauthenticated requests to /api/* get a 401 JSON response.
// Unauthenticated requests to everything else get redirected to /login.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Public paths — no auth required
		if r.URL.Path == "/api/login" || r.URL.Path == "/api/logout" ||
			r.URL.Path == "/login" || r.URL.Path == "/login.html" ||
			r.URL.Path == "/logo.svg" {
			next.ServeHTTP(w, r)
			return
		}

		if a.CheckSession(w, r) {
			next.ServeHTTP(w, r)
			return
		}

		// Rejected
		if len(r.URL.Path) >= 4 && r.URL.Path[:4] == "/api" {
			writeError(w, 401, "authentication required")
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	})
}

// cleanupExpired runs periodically to evict stale sessions.
func (a *Auth) cleanupExpired() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		a.mu.Lock()
		now := time.Now()
		for token, sess := range a.sessions {
			if now.After(sess.expiresAt) {
				delete(a.sessions, token)
			}
		}
		a.mu.Unlock()
	}
}
