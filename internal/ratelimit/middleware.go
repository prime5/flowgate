// Package ratelimit wires the limiter, breaker and shedder packages
// into a single net/http middleware, in the order that actually
// matters: shed first (it's the cheapest check and protects the
// gateway itself), then rate-limit per client, then check the
// breaker before letting the request reach the backend.
package ratelimit

import (
	"net/http"
	"time"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/limiter"
	"github.com/prime5/flowgate/internal/metrics"
	"github.com/prime5/flowgate/internal/shedder"
)

// Config bundles the three primitives the middleware coordinates.
type Config struct {
	Limiter *limiter.Registry
	Breaker *breaker.Breaker
	Shedder *shedder.Shedder
	// KeyFunc extracts the rate-limit key (e.g. client IP or API key)
	// from a request. Defaults to r.RemoteAddr if nil.
	KeyFunc func(*http.Request) string
}

// Wrap returns next wrapped with load shedding, per-key rate limiting
// and circuit breaking, in that order, each instrumented with metrics.
func Wrap(cfg Config, next http.Handler) http.Handler {
	keyFunc := cfg.KeyFunc
	if keyFunc == nil {
		keyFunc = func(r *http.Request) string { return r.RemoteAddr }
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics.InFlight.Set(float64(cfg.Shedder.InFlight()))

		if !cfg.Shedder.Acquire() {
			metrics.RequestsTotal.Inc("shed")
			http.Error(w, "server overloaded, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer cfg.Shedder.Release()

		if ok, wait := cfg.Limiter.Allow(keyFunc(r)); !ok {
			metrics.RequestsTotal.Inc("rate_limited")
			w.Header().Set("Retry-After", formatSeconds(wait))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		if !cfg.Breaker.Allow() {
			metrics.RequestsTotal.Inc("breaker_open")
			metrics.BreakerState.Set(breakerStateValue(cfg.Breaker.State()))
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		if rec.status >= 500 {
			cfg.Breaker.RecordFailure()
			metrics.RequestsTotal.Inc("error")
		} else {
			cfg.Breaker.RecordSuccess()
			metrics.RequestsTotal.Inc("allowed")
		}
		metrics.BreakerState.Set(breakerStateValue(cfg.Breaker.State()))
	})
}

func breakerStateValue(s breaker.State) float64 {
	switch s {
	case breaker.Closed:
		return 0
	case breaker.HalfOpen:
		return 1
	case breaker.Open:
		return 2
	default:
		return -1
	}
}

func formatSeconds(d time.Duration) string {
	secs := int(d.Seconds())
	if secs < 1 {
		secs = 1
	}
	return time.Duration(secs * int(time.Second)).String()
}

// statusRecorder captures the status code the handler actually wrote,
// so the breaker can tell success from failure.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
