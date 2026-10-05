package pool

import (
	"sync"
	"testing"
	"time"
)

func TestSetSizeScalesWorkers(t *testing.T) {
	p := New(time.Millisecond, 100)
	if prev := p.SetSize(4); prev != 0 {
		t.Fatalf("prev = %d, want 0", prev)
	}
	if got := p.Size(); got != 4 {
		t.Fatalf("size = %d, want 4", got)
	}
	if prev := p.SetSize(2); prev != 4 {
		t.Fatalf("prev = %d, want 4", prev)
	}
	if got := p.Size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
	// Floor is 1, never 0: a deployment always has at least one replica.
	p.SetSize(0)
	if got := p.Size(); got != 1 {
		t.Fatalf("size = %d, want 1", got)
	}
}

func TestSubmitDrainsAndCounts(t *testing.T) {
	p := New(time.Millisecond, 100)
	p.SetSize(2)
	const n = 50
	for i := 0; i < n; i++ {
		if !p.Submit() {
			t.Fatalf("submit %d dropped, queue should have room", i)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Processed == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := p.Stats()
	if s.Processed != n {
		t.Fatalf("processed = %d, want %d", s.Processed, n)
	}
	if s.Queued != 0 {
		t.Fatalf("queued = %d, want 0 after drain", s.Queued)
	}
	if s.AvgWaitMs < 0 {
		t.Fatalf("avg wait = %v, want >= 0", s.AvgWaitMs)
	}
}

func TestUnderProvisionedPoolBuildsBacklog(t *testing.T) {
	// One slow worker, fast arrivals: the queue must grow. This is the
	// directory-sync stall in miniature.
	p := New(50*time.Millisecond, 1000)
	p.SetSize(1)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.Submit() }()
	}
	wg.Wait()
	s := p.Stats()
	if s.Queued == 0 && s.Processed == 20 {
		t.Fatalf("expected backlog with 1 slow worker, got none (queued=%d processed=%d)", s.Queued, s.Processed)
	}
	// Scaling up must drain it: the fix for the real incident.
	p.SetSize(8)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Processed == 20 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := p.Stats().Processed; got != 20 {
		t.Fatalf("after scale-up processed = %d, want 20", got)
	}
}

func TestFullQueueDrops(t *testing.T) {
	p := New(time.Hour, 2) // workers effectively parked
	p.SetSize(1)
	accepted := 0
	for i := 0; i < 10; i++ {
		if p.Submit() {
			accepted++
		}
	}
	if accepted > 3 { // 2 queued + 1 in worker's hands, at most
		t.Fatalf("accepted = %d, want <= 3 with cap 2 and one parked worker", accepted)
	}
	if d := p.Stats().Dropped; d == 0 {
		t.Fatalf("dropped = 0, want > 0 when queue is full")
	}
}
