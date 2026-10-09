package limiter

import (
	"net"
	"testing"
)

// Failing open is a deliberate choice (a limiter outage must not become
// a gateway outage), so the cost of it has to be visible: every
// fail-open is counted and reported through the OnFailOpen hook.
func TestRedisLimiter_FailOpenIsCountedAndReported(t *testing.T) {
	// Grab a free port, then close it, so connecting is refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	l, err := NewRedisLimiter(addr, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	var reported int
	l.OnFailOpen = func(error) { reported++ }

	for i := 0; i < 3; i++ {
		if ok, wait := l.Allow("k"); !ok || wait != 0 {
			t.Fatalf("request %d: want fail-open (true, 0), got (%v, %v)", i+1, ok, wait)
		}
	}
	if got := l.FailOpenCount(); got != 3 {
		t.Fatalf("FailOpenCount = %d, want 3", got)
	}
	if reported != 3 {
		t.Fatalf("OnFailOpen called %d times, want 3", reported)
	}
}
