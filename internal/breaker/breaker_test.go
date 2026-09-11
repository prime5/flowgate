package breaker

import (
	"testing"
	"time"
)

func TestBreaker_StartsClosedAndAllows(t *testing.T) {
	b := New(3, time.Second)
	if !b.Allow() {
		t.Fatal("new breaker should start closed and allow requests")
	}
	if b.State() != Closed {
		t.Fatalf("expected Closed, got %s", b.State())
	}
}

func TestBreaker_OpensAfterThreshold(t *testing.T) {
	b := New(3, time.Second)
	for i := 0; i < 3; i++ {
		b.Allow()
		b.RecordFailure()
	}
	if b.State() != Open {
		t.Fatalf("expected Open after 3 consecutive failures, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker should not allow requests during cooldown")
	}
}

func TestBreaker_SuccessResetsCount(t *testing.T) {
	b := New(3, time.Second)
	b.Allow()
	b.RecordFailure()
	b.Allow()
	b.RecordFailure()
	b.Allow()
	b.RecordSuccess() // resets consecutive-failure count
	b.Allow()
	b.RecordFailure()
	if b.State() != Closed {
		t.Fatalf("expected still Closed (only 1 failure since reset), got %s", b.State())
	}
}

func TestBreaker_HalfOpenAfterCooldown(t *testing.T) {
	fixed := time.Unix(0, 0)
	b := New(1, 10*time.Second)
	b.now = func() time.Time { return fixed }

	b.Allow()
	b.RecordFailure() // trips open at fixed time
	if b.State() != Open {
		t.Fatalf("expected Open, got %s", b.State())
	}

	fixed = fixed.Add(5 * time.Second)
	if b.Allow() {
		t.Fatal("should still be within cooldown, expected denied")
	}

	fixed = fixed.Add(10 * time.Second) // now 15s after opening, past the 10s cooldown
	if !b.Allow() {
		t.Fatal("expected one trial request allowed after cooldown elapses")
	}
	if b.State() != HalfOpen {
		t.Fatalf("expected HalfOpen during the trial, got %s", b.State())
	}
}

func TestBreaker_HalfOpenSuccessCloses(t *testing.T) {
	fixed := time.Unix(0, 0)
	b := New(1, time.Second)
	b.now = func() time.Time { return fixed }

	b.Allow()
	b.RecordFailure()
	fixed = fixed.Add(2 * time.Second)
	b.Allow() // trial request, now half-open
	b.RecordSuccess()

	if b.State() != Closed {
		t.Fatalf("expected Closed after successful trial, got %s", b.State())
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	fixed := time.Unix(0, 0)
	b := New(1, time.Second)
	b.now = func() time.Time { return fixed }

	b.Allow()
	b.RecordFailure()
	fixed = fixed.Add(2 * time.Second)
	b.Allow() // trial request, now half-open
	b.RecordFailure()

	if b.State() != Open {
		t.Fatalf("expected Open again after failed trial, got %s", b.State())
	}
}
