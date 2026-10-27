package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestFormatSeconds(t *testing.T) {
	cases := map[time.Duration]string{
		0:                       "1",
		200 * time.Millisecond:  "1",
		time.Second:             "1",
		1100 * time.Millisecond: "2",
		3 * time.Second:         "3",
	}
	for d, want := range cases {
		if got := formatSeconds(d); got != want {
			t.Errorf("formatSeconds(%v) = %q, want %q", d, got, want)
		}
	}
}

// Retry-After on a 429 must parse as a plain integer, the form clients
// (and the aiops console) read.
func TestMiddleware_RetryAfterIsIntegerSeconds(t *testing.T) {
	h := Wrap(newTestConfig(1, 1, 10, 3), http.HandlerFunc(okHandler))
	var last *httptest.ResponseRecorder
	for i := 0; i < 2; i++ {
		last = httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "5.5.5.5:1"
		h.ServeHTTP(last, req)
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("setup: want 429, got %d", last.Code)
	}
	if n, err := strconv.Atoi(last.Header().Get("Retry-After")); err != nil || n < 1 {
		t.Fatalf("Retry-After = %q, want integer >= 1", last.Header().Get("Retry-After"))
	}
}
