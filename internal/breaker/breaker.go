// Package breaker implements a simple three-state circuit breaker
// (closed -> open -> half-open -> closed) guarding a downstream call.
package breaker

import (
	"sync"
	"time"
)

// State is one of the breaker's three states.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Breaker trips to Open after FailureThreshold consecutive failures,
// stays Open for CooldownPeriod, then allows one trial request through
// (HalfOpen). A trial success closes it; a trial failure reopens it.
type Breaker struct {
	mu sync.Mutex

	FailureThreshold int
	CooldownPeriod   time.Duration
	now              func() time.Time

	state        State
	consecutive  int
	openedAt     time.Time
	halfOpenBusy bool // a trial request is already in flight
}

// New returns a closed breaker with the given failure threshold and
// cooldown period before it allows a trial request.
func New(failureThreshold int, cooldown time.Duration) *Breaker {
	return &Breaker{
		FailureThreshold: failureThreshold,
		CooldownPeriod:   cooldown,
		now:              time.Now,
		state:            Closed,
	}
}

// Allow reports whether a request may proceed right now. Every call that
// Allow lets through MUST be followed by exactly one call to RecordSuccess
// or RecordFailure once the downstream call completes.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Sub(b.openedAt) < b.CooldownPeriod {
			return false
		}
		// Cooldown elapsed: move to half-open and let exactly one
		// trial request through.
		b.state = HalfOpen
		b.halfOpenBusy = true
		return true
	case HalfOpen:
		// Only the one trial request in flight is allowed; everything
		// else is rejected until that trial resolves.
		return false
	default:
		return false
	}
}

// RecordSuccess reports that an allowed request succeeded.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.state = Closed
	b.halfOpenBusy = false
}

// RecordFailure reports that an allowed request failed.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == HalfOpen {
		// The trial failed: back to open for another full cooldown.
		b.state = Open
		b.openedAt = b.now()
		b.halfOpenBusy = false
		return
	}

	b.consecutive++
	if b.consecutive >= b.FailureThreshold {
		b.state = Open
		b.openedAt = b.now()
	}
}

// State returns the breaker's current state, for metrics/logging.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
