// Package proxy is flowgate's reverse-proxy backend: with it, the
// gateway's shed, rate-limit and breaker stages protect a real
// upstream service instead of the built-in simulated handler.
//
// It does three things beyond forwarding bytes:
//
//   - Trace propagation. The W3C trace context of the *backend span*
//     (the context ratelimit.Wrap hands to its next handler) is injected
//     into the outgoing request, so the upstream's spans join the same
//     trace as flowgate's shed/ratelimit/breaker/backend spans.
//   - Failure mapping. If the upstream is unreachable or times out the
//     proxy answers 502/504. ratelimit.Wrap counts any status >= 500
//     as a backend failure, so a dead upstream trips the breaker and
//     later requests fail fast with 503 instead of queueing up.
//   - Header hygiene. Authorization and the other end-to-end headers
//     pass through untouched (the upstream still does its own auth);
//     hop-by-hop headers are dropped by httputil.ReverseProxy.
package proxy

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Options tunes the upstream connection. Zero values pick defaults.
type Options struct {
	// ResponseHeaderTimeout bounds how long the upstream may take to
	// start answering. Default 5s. Without a bound, a hung upstream
	// holds a shedder slot per request until the client gives up.
	ResponseHeaderTimeout time.Duration
	// Transport overrides the HTTP transport (used by tests).
	Transport http.RoundTripper
}

// New returns a handler that forwards every request to upstream,
// preserving the request path and query.
func New(upstream *url.URL, opt Options) http.Handler {
	if opt.ResponseHeaderTimeout == 0 {
		opt.ResponseHeaderTimeout = 5 * time.Second
	}
	rt := opt.Transport
	if rt == nil {
		rt = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   50,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: opt.ResponseHeaderTimeout,
		}
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			pr.SetXForwarded()
			// Inject from the inbound request's context: Wrap put the
			// backend span there, so the upstream becomes its child.
			otel.GetTextMapPropagator().Inject(pr.In.Context(), propagation.HeaderCarrier(pr.Out.Header))
		},
		Transport: rt,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			status := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
				status = http.StatusGatewayTimeout
			}
			if !errors.Is(err, context.Canceled) {
				log.Printf("proxy: upstream error (%d): %v", status, err)
			}
			http.Error(w, "upstream unavailable", status)
		},
	}
	return rp
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
