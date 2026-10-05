// Package pool is a dynamically resizable in-process worker pool:
// goroutines reading from a bounded channel, with SetSize adding or
// stopping them at runtime.
//
// It is an in-process model, not an orchestrator. The workers are
// goroutines, not pods; SetSize changes this process's own
// concurrency and does not call Kubernetes. The real Kubernetes layer
// lives in deploy/k8s (Deployment, HPA, k6 Job) and
// scripts/pod-kill.sh, which act on actual pods.
//
// What the model does reproduce is the dynamic behind the Cloud
// Identity Engine directory-sync stall: when processing capacity is
// below the arrival rate, the queue grows monotonically until batches
// miss their window, and the fix is to raise capacity. That
// relationship between capacity, arrival rate and backlog is the same
// whether the unit of capacity is a goroutine or a pod, which is why
// an in-process pool is enough to make the failure deterministic and
// reproducible on demand. The queue dynamics are real; the processing
// time is simulated.
package pool

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stats is a point-in-time snapshot of the pool.
type Stats struct {
	Replicas  int
	Queued    int
	Processed int64
	AvgWaitMs float64 // mean time jobs spent queued before a worker picked them up
	Dropped   int64   // jobs refused because the queue was full
}

// job is one unit of queued work.
type job struct {
	enqueued time.Time
}

// Pool is a worker pool whose size can change at runtime.
type Pool struct {
	jobs    chan job
	mu      sync.Mutex
	workers []chan struct{} // one stop channel per worker
	work    time.Duration   // simulated processing time per job

	processed atomic.Int64
	dropped   atomic.Int64
	waitSum   atomic.Int64 // nanoseconds of total queue wait, for the mean
	waitCount atomic.Int64
}

// New returns a pool with no workers; call SetSize before submitting.
// work is the simulated processing time per job; queueCap bounds the
// backlog (a full queue means Submit drops, loudly).
func New(work time.Duration, queueCap int) *Pool {
	return &Pool{jobs: make(chan job, queueCap), work: work}
}

// SetSize changes the pool to n worker goroutines, starting or
// stopping them as needed, and returns the previous size. Stopping is
// graceful: a worker finishes the job in hand before exiting.
//
// This is the in-process analogue of changing a deployment's replica
// count — it varies processing capacity against a fixed arrival rate —
// but it is goroutines in this process, not pods, and it calls no
// orchestrator.
func (p *Pool) SetSize(n int) (prev int) {
	if n < 1 {
		n = 1
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	prev = len(p.workers)
	for len(p.workers) < n {
		stop := make(chan struct{})
		p.workers = append(p.workers, stop)
		go p.run(stop)
	}
	for len(p.workers) > n {
		w := p.workers[len(p.workers)-1]
		p.workers = p.workers[:len(p.workers)-1]
		close(w)
	}
	return prev
}

// Size returns the current worker count.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

// Submit enqueues one job. It returns false — and counts a drop —
// when the queue is full, the way a sync batch misses its window
// instead of queueing forever.
func (p *Pool) Submit() bool {
	select {
	case p.jobs <- job{enqueued: time.Now()}:
		return true
	default:
		p.dropped.Add(1)
		return false
	}
}

func (p *Pool) run(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case j := <-p.jobs:
			wait := time.Since(j.enqueued)
			time.Sleep(p.work)
			p.waitSum.Add(int64(wait))
			p.waitCount.Add(1)
			p.processed.Add(1)
		}
	}
}

// Stats snapshots the pool. Queued is the live channel depth.
func (p *Pool) Stats() Stats {
	s := Stats{
		Replicas:  p.Size(),
		Queued:    len(p.jobs),
		Processed: p.processed.Load(),
		Dropped:   p.dropped.Load(),
	}
	if n := p.waitCount.Load(); n > 0 {
		s.AvgWaitMs = float64(p.waitSum.Load()) / float64(n) / 1e6
	}
	return s
}
