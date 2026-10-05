package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// defaultKeyFunc is the only place in the gateway where one client is
// distinguished from another. Everything the per-client rate limit
// claims rests on it, so it gets tested directly rather than only
// through the middleware.
func TestDefaultKeyFunc(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		flyHeader  string
		want       string
	}{
		{
			name:       "fly header present",
			remoteAddr: "172.16.0.1:40000", // the proxy, not the client
			flyHeader:  "203.0.113.7",
			want:       "203.0.113.7",
		},
		{
			name:       "fly header wins over remote addr",
			remoteAddr: "1.2.3.4:5678",
			flyHeader:  "203.0.113.7",
			want:       "203.0.113.7",
		},
		{
			name:       "no header, ipv4 host:port",
			remoteAddr: "1.2.3.4:5678",
			want:       "1.2.3.4",
		},
		{
			name:       "no header, ipv4 without port",
			remoteAddr: "1.2.3.4",
			want:       "1.2.3.4",
		},
		{
			// SplitHostPort strips the brackets along with the port.
			name:       "no header, ipv6 bracketed with port",
			remoteAddr: "[2001:db8::1]:443",
			want:       "2001:db8::1",
		},
		{
			// Bare IPv6 has no port to split and SplitHostPort rejects
			// it ("too many colons"), so the fallback returns it whole.
			// Correct, but note the key keeps no brackets here and does
			// in the bracketed-without-port case below — two spellings
			// of one client would get two buckets.
			name:       "no header, bare ipv6",
			remoteAddr: "2001:db8::1",
			want:       "2001:db8::1",
		},
		{
			name:       "no header, bracketed ipv6 without port",
			remoteAddr: "[2001:db8::1]",
			want:       "[2001:db8::1]",
		},
		{
			// Documents current behaviour: with nothing to key on, every
			// such request shares one bucket. That errs toward limiting
			// more traffic, not less, so it fails safe.
			name:       "no header, empty remote addr",
			remoteAddr: "",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/work", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.flyHeader != "" {
				req.Header.Set("Fly-Client-IP", tt.flyHeader)
			}

			if got := defaultKeyFunc(req); got != tt.want {
				t.Fatalf("defaultKeyFunc() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDefaultKeyFunc_TrustsFlyHeaderUnconditionally pins down a
// property worth being deliberate about: the header is trusted with no
// check that the request actually came through Fly's proxy. Behind the
// proxy that's correct, because Fly overwrites Fly-Client-IP on every
// inbound request. Reachable directly, it means a caller can rotate the
// header and get a fresh bucket per request.
//
// This test asserts the current behaviour so a future change to it is a
// deliberate one, not a surprise.
func TestDefaultKeyFunc_TrustsFlyHeaderUnconditionally(t *testing.T) {
	req := httptest.NewRequest("GET", "/work", nil)
	req.RemoteAddr = "1.2.3.4:5678"
	req.Header.Set("Fly-Client-IP", "not-an-ip-at-all")

	if got := defaultKeyFunc(req); got != "not-an-ip-at-all" {
		t.Fatalf("defaultKeyFunc() = %q, want the header value verbatim", got)
	}
}

// The SLO the postmortem recorded as unmet is behavioural, not
// structural: distinct clients must get distinct buckets, so an
// over-limit client sees 429 while a fresh one still sees 200. These
// two tests assert that through the middleware, once per keying path.

func TestMiddleware_SeparateBucketsPerRemoteAddr(t *testing.T) {
	cfg := newTestConfig(1, 1, 10, 3) // burst 1, so request two is over
	h := Wrap(cfg, http.HandlerFunc(okHandler))

	do := func(remoteAddr string) int {
		req := httptest.NewRequest("GET", "/work", nil)
		req.RemoteAddr = remoteAddr
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := do("1.2.3.4:1111"); code != http.StatusOK {
		t.Fatalf("client A first request: got %d, want 200", code)
	}
	if code := do("1.2.3.4:2222"); code != http.StatusTooManyRequests {
		t.Fatalf("client A second request: got %d, want 429 (same host, new port is the same client)", code)
	}
	if code := do("5.6.7.8:1111"); code != http.StatusOK {
		t.Fatalf("client B first request: got %d, want 200 (B must not inherit A's spent bucket)", code)
	}
}

func TestMiddleware_SeparateBucketsPerFlyClientIP(t *testing.T) {
	cfg := newTestConfig(1, 1, 10, 3)
	h := Wrap(cfg, http.HandlerFunc(okHandler))

	// Every request arrives from the same proxy address; only the
	// header distinguishes the clients. This is the production path,
	// and the one the original defect broke.
	do := func(clientIP string) int {
		req := httptest.NewRequest("GET", "/work", nil)
		req.RemoteAddr = "172.16.0.1:40000"
		req.Header.Set("Fly-Client-IP", clientIP)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := do("203.0.113.7"); code != http.StatusOK {
		t.Fatalf("client A first request: got %d, want 200", code)
	}
	if code := do("203.0.113.7"); code != http.StatusTooManyRequests {
		t.Fatalf("client A second request: got %d, want 429", code)
	}
	if code := do("203.0.113.9"); code != http.StatusOK {
		t.Fatalf("client B first request: got %d, want 200 (B must not inherit A's spent bucket)", code)
	}
}
