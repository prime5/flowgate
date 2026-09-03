// Package limiter implements token-bucket rate limiting.
package limiter

import (
	"errors"
	"time"
)

// Bucket is a single token bucket: one client's rate-limit state.
// It is not safe for concurrent use; see session 2.
type Bucket struct {
	capacity float64 // maximum tokens held; the burst size
	rate     float64 // tokens added per second; the steady-state rate
	tokens   float64 // current tokens, always in [0, capacity]
	last     time.Time
	now      func() time.Time // injected for tests
}

// NewBucket returns a bucket that starts full.
// capacity is the burst size; ratePerSec is the sustained refill rate.
func NewBucket(capacity, ratePerSec float64) (*Bucket, error) {
	if capacity <= 0 {
		return nil, errors.New("limiter: capacity must be positive")
	}
	if ratePerSec <= 0 {
		return nil, errors.New("limiter: rate must be positive")
	}
	return &Bucket{
		capacity: capacity,
		rate:     ratePerSec,
		tokens:   capacity,
		last:     time.Now(),
		now:      time.Now,
	}, nil
}

// Allow reports whether one token was available and consumed.
// When it returns false, the duration is how long until the next
// token becomes available; when true, it is zero.
func (b *Bucket) Allow() (bool, time.Duration) {
	// TODO
	// 1. now := b.now(); elapsed := now.Sub(b.last), clamped at >= 0
	// 2. credit elapsed*rate tokens, clamp to capacity
	// 3. set b.last — unconditionally, on both paths
	// 4. spend one token if tokens >= 1, else compute the wait
	now := b.now()
	elapsed := now.Sub(b.last)
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens += elapsed.Seconds() * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now

	if b.tokens >= 1 {
		b.tokens -= 1
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
	return false, wait
}
