// Package shedder implements load shedding by bounding the number of
// requests in flight. When the bound is reached, new requests are
// rejected immediately instead of queueing — the point is to protect
// latency for the requests already being served, not to be fair to
// the ones that arrive after saturation.
package shedder

// Shedder bounds concurrent in-flight work to MaxInFlight.
type Shedder struct {
	slots chan struct{}
}

// New returns a Shedder that admits at most maxInFlight requests at once.
func New(maxInFlight int) *Shedder {
	return &Shedder{slots: make(chan struct{}, maxInFlight)}
}

// Acquire tries to reserve one in-flight slot without blocking.
// It reports whether the slot was granted. Every granted Acquire MUST
// be paired with exactly one Release.
func (s *Shedder) Acquire() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release frees a slot previously granted by Acquire.
func (s *Shedder) Release() {
	select {
	case <-s.slots:
	default:
		// Release without a matching Acquire is a caller bug; dropping
		// silently here would hide it, but panicking would take down
		// the server it's meant to protect, so this is intentionally
		// a documented precondition instead of a runtime check.
	}
}

// InFlight returns the current number of occupied slots.
func (s *Shedder) InFlight() int {
	return len(s.slots)
}

// Capacity returns the maximum number of concurrent slots.
func (s *Shedder) Capacity() int {
	return cap(s.slots)
}
