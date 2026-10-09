// Package metrics is flowgate's own minimal Prometheus exposition
// writer. It intentionally has zero third-party dependencies: the
// Prometheus text format is just documented plain text
// (https://prometheus.io/docs/instrumenting/exposition_formats/),
// and hand-writing it here means every line on /metrics is something
// this project actually implements, not a library doing it silently.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// counterVec is a set of named counters distinguished by one label
// value (e.g. "outcome"), safe for concurrent use.
type counterVec struct {
	mu     sync.Mutex
	counts map[string]*int64
}

func newCounterVec() *counterVec {
	return &counterVec{counts: make(map[string]*int64)}
}

func (c *counterVec) Inc(label string) {
	c.mu.Lock()
	p, ok := c.counts[label]
	if !ok {
		var v int64
		p = &v
		c.counts[label] = p
	}
	c.mu.Unlock()
	atomic.AddInt64(p, 1)
}

func (c *counterVec) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.counts))
	for k, p := range c.counts {
		out[k] = atomic.LoadInt64(p)
	}
	return out
}

// gauge is a single float64 value, safe for concurrent use.
type gauge struct {
	bits uint64 // float64 bit pattern, accessed atomically
}

func (g *gauge) Set(v float64) {
	atomic.StoreUint64(&g.bits, math.Float64bits(v))
}

func (g *gauge) Get() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.bits))
}

var (
	// RequestsTotal counts every request the gateway saw, labeled by
	// the decision made about it: allowed | rate_limited | shed |
	// breaker_open | error.
	RequestsTotal = newCounterVec()

	// BreakerState reports the circuit breaker's current state:
	// 0=closed, 1=half-open, 2=open.
	BreakerState = &gauge{}

	// InFlight reports how many requests currently occupy a shedder slot.
	InFlight = &gauge{}

	// SyncQueueDepth reports how many directory-sync batches are
	// waiting for a worker: the backlog that grows during the
	// under-provisioned stall.
	SyncQueueDepth = &gauge{}

	// SyncReplicas reports the current worker count of the sync
	// pool — the capacity dimension the stall experiment varies.
	SyncReplicas = &gauge{}

	// FaultInjectedTotal counts deliberately injected faults, labeled
	// by the primitive that caused one: latency | error | timeout |
	// cpuburn | blackhole. A call a primitive sampled out is not an
	// injected fault and is not counted here.
	FaultInjectedTotal = newCounterVec()

	// LimiterFailOpenTotal counts requests the shared (Redis) limiter
	// admitted only because it could not reach Redis. Failing open is a
	// deliberate availability choice (see DECISIONS.md), so its cost has
	// to be visible: a non-zero rate means the per-client limit is NOT
	// being enforced. Labeled by backend; only "redis" is used today.
	LimiterFailOpenTotal = newCounterVec()
)

// Register exists to keep call sites symmetric with libraries that
// require it (e.g. cmd/flowgate/main.go calls metrics.Register at
// startup); this implementation has no registry to populate since
// the package-level vars above are the whole metric set.
func Register() {}

// WriteTo writes all flowgate metrics to w in Prometheus text
// exposition format.
func WriteTo(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "# HELP flowgate_requests_total Total requests handled by flowgate, labeled by outcome.\n"); err != nil {
		return err
	}
	fmt.Fprintf(w, "# TYPE flowgate_requests_total counter\n")
	snap := RequestsTotal.snapshot()
	labels := make([]string, 0, len(snap))
	for l := range snap {
		labels = append(labels, l)
	}
	sort.Strings(labels) // deterministic output, easier to diff/test
	for _, l := range labels {
		fmt.Fprintf(w, "flowgate_requests_total{outcome=%q} %d\n", l, snap[l])
	}

	fmt.Fprintf(w, "# HELP flowgate_breaker_state Circuit breaker state: 0=closed, 1=half-open, 2=open.\n")
	fmt.Fprintf(w, "# TYPE flowgate_breaker_state gauge\n")
	fmt.Fprintf(w, "flowgate_breaker_state %v\n", BreakerState.Get())

	fmt.Fprintf(w, "# HELP flowgate_in_flight_requests Current number of requests occupying a shedder slot.\n")
	fmt.Fprintf(w, "# TYPE flowgate_in_flight_requests gauge\n")
	fmt.Fprintf(w, "flowgate_in_flight_requests %v\n", InFlight.Get())

	fmt.Fprintf(w, "# HELP flowgate_sync_queue_depth Directory-sync batches waiting for a worker.\n")
	fmt.Fprintf(w, "# TYPE flowgate_sync_queue_depth gauge\n")
	fmt.Fprintf(w, "flowgate_sync_queue_depth %v\n", SyncQueueDepth.Get())

	fmt.Fprintf(w, "# HELP flowgate_sync_replicas Current worker count of the directory-sync pool.\n")
	fmt.Fprintf(w, "# TYPE flowgate_sync_replicas gauge\n")
	fmt.Fprintf(w, "flowgate_sync_replicas %v\n", SyncReplicas.Get())

	fmt.Fprintf(w, "# HELP flowgate_fault_injected_total Deliberately injected faults, labeled by primitive.\n")
	fmt.Fprintf(w, "# TYPE flowgate_fault_injected_total counter\n")
	faults := FaultInjectedTotal.snapshot()
	names := make([]string, 0, len(faults))
	for n := range faults {
		names = append(names, n)
	}
	sort.Strings(names) // deterministic output, easier to diff/test
	var err error
	for _, n := range names {
		_, err = fmt.Fprintf(w, "flowgate_fault_injected_total{primitive=%q} %d\n", n, faults[n])
	}

	fmt.Fprintf(w, "# HELP flowgate_limiter_fail_open_total Requests admitted because the shared limiter could not reach Redis (limit not enforced).\n")
	fmt.Fprintf(w, "# TYPE flowgate_limiter_fail_open_total counter\n")
	// Always emit the redis series, including at 0, so an alert on
	// rate(...) > 0 has a series to evaluate from the first scrape.
	_, err2 := fmt.Fprintf(w, "flowgate_limiter_fail_open_total{backend=\"redis\"} %d\n", LimiterFailOpenTotal.snapshot()["redis"])
	if err == nil {
		err = err2
	}
	return err
}
