// Package ratelimit wires the limiter, breaker and shedder packages
// into a single net/http middleware, in the order that actually
// matters: shed first (it's the cheapest check and protects the
// gateway itself), then rate-limit per client, then check the
// breaker before letting the request reach the backend.
package ratelimit

import (
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/limiter"
	"github.com/prime5/flowgate/internal/metrics"
	"github.com/prime5/flowgate/internal/shedder"
)

// Config bundles the three primitives the middleware coordinates.
type Config struct {
	Limiter limiter.Limiter
	Breaker *breaker.Breaker
	Shedder *shedder.Shedder
	// KeyFunc extracts the rate-limit key (e.g. client IP or API key)
	// from a request. Defaults to r.RemoteAddr if nil.
	KeyFunc func(*http.Request) string
}

func defaultKeyFunc(r *http.Request) string {
	if ip := r.Header.Get("Fly-Client-IP"); ip != "" {
		return ip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Wrap returns next wrapped with load shedding, per-key rate limiting
// and circuit breaking, in that order, each instrumented with metrics
// and with a trace span.
//
// Tracing: each request becomes one server span, "flowgate.request",
// parented to the caller's span when the request carries a W3C
// traceparent header. Under it sit one child span per stage that ran:
// shed, ratelimit, breaker, backend. A request rejected early simply
// has fewer children, so the shape of a trace shows where it stopped.
// The tracer comes from the global provider at Wrap time, so the
// provider must be installed (telemetry.Setup) before Wrap is called.
// The trace ID is returned in the X-Trace-Id header so a client or a
// k6 run can quote the exact trace for a slow request.
func Wrap(cfg Config, next http.Handler) http.Handler {
	keyFunc := cfg.KeyFunc
	if keyFunc == nil {
		keyFunc = defaultKeyFunc
	}
	tracer := otel.GetTracerProvider().Tracer("github.com/prime5/flowgate/internal/ratelimit")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, root := tracer.Start(ctx, "flowgate.request",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.target", r.URL.Path),
			))
		defer root.End()
		if sc := root.SpanContext(); sc.IsValid() {
			w.Header().Set("X-Trace-Id", sc.TraceID().String())
		}
		// record notes why a request stopped early. rejection is a
		// normal outcome, not an error, except where the gateway itself
		// failed the caller (shed, breaker open).
		record := func(outcome string, isError bool) {
			root.SetAttributes(attribute.String("flowgate.outcome", outcome))
			if isError {
				root.SetStatus(codes.Error, outcome)
			}
		}

		metrics.InFlight.Set(float64(cfg.Shedder.InFlight()))

		_, shedSpan := tracer.Start(ctx, "shed")
		admitted := cfg.Shedder.Acquire()
		shedSpan.SetAttributes(
			attribute.Bool("flowgate.admitted", admitted),
			attribute.Int("flowgate.in_flight", cfg.Shedder.InFlight()),
		)
		shedSpan.End()
		if !admitted {
			metrics.RequestsTotal.Inc("shed")
			record("shed", true)
			http.Error(w, "server overloaded, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer cfg.Shedder.Release()

		_, rlSpan := tracer.Start(ctx, "ratelimit")
		allowed, wait := cfg.Limiter.Allow(keyFunc(r))
		rlSpan.SetAttributes(attribute.Bool("flowgate.allowed", allowed))
		rlSpan.End()
		if !allowed {
			metrics.RequestsTotal.Inc("rate_limited")
			record("rate_limited", false)
			w.Header().Set("Retry-After", formatSeconds(wait))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		_, brSpan := tracer.Start(ctx, "breaker")
		passed := cfg.Breaker.Allow()
		brSpan.SetAttributes(
			attribute.Bool("flowgate.allowed", passed),
			attribute.String("flowgate.breaker.state", cfg.Breaker.State().String()),
		)
		brSpan.End()
		if !passed {
			metrics.RequestsTotal.Inc("breaker_open")
			metrics.BreakerState.Set(breakerStateValue(cfg.Breaker.State()))
			record("breaker_open", true)
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}

		backendCtx, beSpan := tracer.Start(ctx, "backend")
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(backendCtx))
		beSpan.SetAttributes(attribute.Int("http.status_code", rec.status))

		if rec.status >= 500 {
			beSpan.SetStatus(codes.Error, "backend returned 5xx")
			cfg.Breaker.RecordFailure()
			metrics.RequestsTotal.Inc("error")
			record("error", true)
		} else {
			cfg.Breaker.RecordSuccess()
			metrics.RequestsTotal.Inc("allowed")
			record("allowed", false)
		}
		beSpan.End()
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
