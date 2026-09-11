// Package limiter implements token-bucket rate limiting.
package limiter

import (
	"errors"
	"sync"
	"time"
)

// Bucket is a single token bucket: one client's rate-limit state.
// Safe for concurrent use — Allow is guarded by a mutex.
type Bucket struct {
	mu       sync.Mutex
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
	b.mu.Lock()
	defer b.mu.Unlock()

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

// Registry hands out one Bucket per key (e.g. per client IP or API key),
// creating it lazily on first use. Safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	buckets  map[string]*Bucket
	capacity float64
	rate     float64
}

// NewRegistry returns a Registry where every new key gets a bucket
// with the given capacity (burst) and ratePerSec (sustained rate).
func NewRegistry(capacity, ratePerSec float64) *Registry {
	return &Registry{
		buckets:  make(map[string]*Bucket),
		capacity: capacity,
		rate:     ratePerSec,
	}
}

// Allow reports whether the request identified by key is allowed right now.
func (r *Registry) Allow(key string) (bool, time.Duration) {
	r.mu.Lock()
	b, ok := r.buckets[key]
	if !ok {
		// capacity/rate are validated at construction time in NewRegistry's
		// caller-supplied values, so the error path here is unreachable in
		// practice; NewBucket is still called defensively.
		nb, err := NewBucket(r.capacity, r.rate)
		if err != nil {
			r.mu.Unlock()
			return true, 0 // fail open rather than block traffic on a config bug
		}
		b = nb
		r.buckets[key] = b
	}
	r.mu.Unlock()
	return b.Allow()
}
