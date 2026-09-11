package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/limiter"
	"github.com/prime5/flowgate/internal/shedder"
)

func newTestConfig(burst, ratePerSec float64, maxInFlight, breakerThreshold int) Config {
	return Config{
		Limiter: limiter.NewRegistry(burst, ratePerSec),
		Breaker: breaker.New(breakerThreshold, time.Second),
		Shedder: shedder.New(maxInFlight),
	}
}

func okHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestMiddleware_AllowsFirstRequest(t *testing.T) {
	cfg := newTestConfig(1, 1, 10, 3)
	h := Wrap(cfg, http.HandlerFunc(okHandler))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestMiddleware_RateLimitsSecondImmediateRequest(t *testing.T) {
	cfg := newTestConfig(1, 1, 10, 3)
	h := Wrap(cfg, http.HandlerFunc(okHandler))

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "1.2.3.4:1111"

	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)

	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on second immediate request, got %d", rec2.Code)
	}
}

func TestMiddleware_ShedsWhenSaturated(t *testing.T) {
	cfg := newTestConfig(1000, 1000, 1, 3) // generous rate limit, tiny concurrency cap
	release := make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hold the only slot open until the test releases it
		w.WriteHeader(http.StatusOK)
	})
	h := Wrap(cfg, slow)

	done := make(chan *httptest.ResponseRecorder)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "1.1.1.1:1"
		h.ServeHTTP(rec, req)
		done <- rec
	}()

	// Give the first request time to acquire the only shedder slot.
	time.Sleep(20 * time.Millisecond)

	req2 := httptest.NewRequest("GET", "/", nil)
	req2.RemoteAddr = "2.2.2.2:2"
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)

	close(release)
	<-done

	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected second request shed with 503 while the slot was occupied, got %d", rec2.Code)
	}
}

func TestMiddleware_BreakerOpensAfterBackendFailures(t *testing.T) {
	cfg := newTestConfig(1000, 1000, 10, 2) // trips after 2 failures
	failing := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	h := Wrap(cfg, failing)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "3.3.3.3:3"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "3.3.3.3:3"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected breaker to be open and return 503 after threshold failures, got %d", rec.Code)
	}
}
