package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// keyFuncFor is the only place in the gateway where one client is
// distinguished from another. Everything the per-client rate limit
// claims rests on it, so it gets tested directly rather than only
// through the middleware.
func TestKeyFuncFor_Trusted(t *testing.T) {
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

			if got := keyFuncFor(true)(req); got != tt.want {
				t.Fatalf("keyFuncFor(true) = %q, want %q", got, tt.want)
			}
		})
	}
}

// With the header untrusted (the default) it must be ignored entirely:
// the key is the socket peer, whatever the header says.
func TestKeyFuncFor_UntrustedIgnoresFlyHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/work", nil)
	req.RemoteAddr = "1.2.3.4:5678"
	req.Header.Set("Fly-Client-IP", "203.0.113.7")

	if got := keyFuncFor(false)(req); got != "1.2.3.4" {
		t.Fatalf("keyFuncFor(false) = %q, want the peer address 1.2.3.4", got)
	}
}

// Regression for the bypass: when flowgate is reachable directly, a
// caller that rotates Fly-Client-IP must still hit ONE bucket (its own
// address) unless the operator opted in to trusting the header.
func TestMiddleware_RotatingFlyHeaderDoesNotEvadeLimitWhenUntrusted(t *testing.T) {
	cfg := newTestConfig(1, 1, 10, 3) // burst 1; TrustFlyClientIP left false
	h := Wrap(cfg, http.HandlerFunc(okHandler))

	do := func(spoofed string) int {
		req := httptest.NewRequest("GET", "/work", nil)
		req.RemoteAddr = "198.51.100.9:40000"
		req.Header.Set("Fly-Client-IP", spoofed)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := do("10.0.0.1"); code != http.StatusOK {
		t.Fatalf("first request: got %d, want 200", code)
	}
	for _, ip := range []string{"10.0.0.2", "10.0.0.3", "10.0.0.4"} {
		if code := do(ip); code != http.StatusTooManyRequests {
			t.Fatalf("request with rotated header %s: got %d, want 429 (header must be ignored when untrusted)", ip, code)
		}
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
	cfg.TrustFlyClientIP = true // production path: behind Fly's proxy
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
