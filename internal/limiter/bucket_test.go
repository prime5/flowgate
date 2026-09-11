package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestNewBucket_RejectsBadInput(t *testing.T) {
	if _, err := NewBucket(0, 1); err == nil {
		t.Error("expected error for zero capacity")
	}
	if _, err := NewBucket(1, 0); err == nil {
		t.Error("expected error for zero rate")
	}
}

func TestBucket_StartsFullAndDrains(t *testing.T) {
	b, err := NewBucket(3, 1) // burst of 3, refills 1/sec
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Unix(0, 0)
	b.now = func() time.Time { return fixed }

	for i := 0; i < 3; i++ {
		ok, wait := b.Allow()
		if !ok {
			t.Fatalf("request %d: expected allowed, got denied (wait=%v)", i, wait)
		}
	}
	ok, wait := b.Allow()
	if ok {
		t.Fatal("4th immediate request should be denied — bucket should be empty")
	}
	if wait <= 0 {
		t.Errorf("expected positive wait, got %v", wait)
	}
}

func TestBucket_RefillsOverTime(t *testing.T) {
	b, err := NewBucket(1, 2) // burst of 1, refills 2/sec
	if err != nil {
		t.Fatal(err)
	}
	current := time.Unix(0, 0)
	b.now = func() time.Time { return current }

	ok, _ := b.Allow()
	if !ok {
		t.Fatal("first request should be allowed (bucket starts full)")
	}
	ok, _ = b.Allow()
	if ok {
		t.Fatal("second immediate request should be denied")
	}

	current = current.Add(600 * time.Millisecond) // 0.6s * 2/s = 1.2 tokens credited
	ok, _ = b.Allow()
	if !ok {
		t.Fatal("expected allowed after enough time elapsed to refill one token")
	}
}

func TestBucket_NeverExceedsCapacity(t *testing.T) {
	b, err := NewBucket(2, 100) // fast refill
	if err != nil {
		t.Fatal(err)
	}
	current := time.Unix(0, 0)
	b.now = func() time.Time { return current }

	current = current.Add(time.Hour) // huge elapsed time, would overflow tokens if unclamped
	ok, _ := b.Allow()
	if !ok {
		t.Fatal("expected allowed")
	}
	if b.tokens > b.capacity {
		t.Fatalf("tokens (%v) exceeded capacity (%v)", b.tokens, b.capacity)
	}
}

func TestBucket_ConcurrentAccessIsSafe(t *testing.T) {
	b, err := NewBucket(1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _ := b.Allow()
			if ok {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed > 1000 {
		t.Fatalf("allowed %d requests, more than the 1000-token capacity — race in Allow()", allowed)
	}
}

func TestRegistry_IsolatesKeysIndependently(t *testing.T) {
	r := NewRegistry(1, 1)
	okA, _ := r.Allow("client-a")
	okB, _ := r.Allow("client-b")
	if !okA || !okB {
		t.Fatal("first request from two different clients should both be allowed")
	}
	okA2, _ := r.Allow("client-a")
	if okA2 {
		t.Fatal("second immediate request from client-a should be denied by its own bucket")
	}
}
