// Command faultdemo exercises internal/fault directly and narrates
// what happens, so the library's behaviour is observable without
// reading the tests.
//
// Run it with:
//
//	go run ./cmd/faultdemo
//
// Nothing here is load-bearing for the service; it exists to be read
// and run. Each section prints what it is about to do, does it, and
// prints what actually happened, so a claim and its evidence sit next
// to each other.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/prime5/flowgate/internal/fault"
	"github.com/prime5/flowgate/internal/metrics"
)

func main() {
	section("1. A primitive is a no-op until you give it a blast radius")
	demoRateZeroAndOne()

	section("2. Blast radius: Rate(p) confines the fault to p of calls")
	demoBlastRadius()

	section("3. Every primitive respects context cancellation")
	demoCancellation()

	section("4. Chain order is observable: slow failure vs fast failure")
	demoChainOrder()

	section("5. One line of middleware, and the status comes from the fault")
	demoMiddleware()

	section("6. Resource faults: the shape that is not a wrapped call")
	demoResource()

	section("7. The counter measures faults caused, not faults considered")
	demoMetrics()

	fmt.Println()
	fmt.Println(strings.Repeat("=", 68))
	fmt.Println("Done. Every number above was produced by this run.")
}

// --- 1 -----------------------------------------------------------------

func demoRateZeroAndOne() {
	explain("A fault library has to fail safe: a primitive with no blast",
		"radius must let every call through untouched.")

	for _, tc := range []struct {
		label string
		s     fault.Sampler
	}{
		{"Rate(0)", fault.Rate(0)},
		{"nil sampler", nil},
		{"Rate(1)", fault.Rate(1)},
	} {
		p := fault.Blackhole(tc.s)
		ran := false
		err := p.Inject(context.Background(), func(context.Context) error {
			ran = true
			return nil
		})
		fmt.Printf("  Blackhole(%-12s)  work ran: %-5v  err: %v\n", tc.label, ran, errStr(err))
	}

	note("A forgotten sampler disables the fault. Enabling by default",
		"would make every accidental wiring an outage.")
}

// --- 2 -----------------------------------------------------------------

func demoBlastRadius() {
	explain("Rate(p) is the primitive-level blast-radius control. Same idea",
		"as rate limiting, pointed at the experiment instead of traffic.")

	const n = 10000
	for _, rate := range []float64{0.01, 0.1, 0.5, 0.9} {
		p := fault.Blackhole(fault.Rate(rate))
		hit := 0
		for i := 0; i < n; i++ {
			if err := p.Inject(context.Background(), noop); err != nil {
				hit++
			}
		}
		pct := 100 * float64(hit) / n
		fmt.Printf("  Rate(%.2f) over %d calls -> %5d faulted (%.1f%%)  %s\n",
			rate, n, hit, pct, bar(pct))
	}
}

// --- 3 -----------------------------------------------------------------

func demoCancellation() {
	explain("A fault that ignores cancellation is a bug, not a fault: the",
		"caller's timeout budget still has to mean something.")

	// A 10-second delay against a 50ms deadline.
	p := fault.Latency(10*time.Second, fault.Always())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	ran := false
	err := p.Inject(ctx, func(context.Context) error { ran = true; return nil })
	fmt.Printf("  Latency(10s) under a 50ms deadline\n")
	fmt.Printf("    returned after : %v\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("    error          : %v\n", errStr(err))
	fmt.Printf("    work ran       : %v\n", ran)

	// Timeout's maxHold is 10s, but the caller's deadline is nearer.
	t := fault.Timeout(10*time.Second, fault.Always())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel2()
	start = time.Now()
	err = t.Inject(ctx2, noop)
	fmt.Printf("  Timeout(maxHold=10s) under a 40ms deadline\n")
	fmt.Printf("    returned after : %v\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("    error          : %v\n", errStr(err))

	note("The nearer deadline always wins, so an injected hang can never",
		"become an unbounded one.")
}

// --- 4 -----------------------------------------------------------------

func demoChainOrder() {
	explain("Chain composes outside-in: the first argument is outermost.",
		"That ordering is behaviour, not cosmetics.")

	slow := fault.Chain(
		fault.Latency(300*time.Millisecond, fault.Always()),
		fault.Error(nil, fault.Always()),
	)
	start := time.Now()
	err := slow.Inject(context.Background(), noop)
	fmt.Printf("  Chain(Latency(300ms), Error)  -> %v after %v   [slow failure]\n",
		errStr(err), time.Since(start).Round(10*time.Millisecond))

	fast := fault.Chain(
		fault.Error(nil, fault.Always()),
		fault.Latency(300*time.Millisecond, fault.Always()),
	)
	start = time.Now()
	err = fast.Inject(context.Background(), noop)
	fmt.Printf("  Chain(Error, Latency(300ms))  -> %v after %v   [fast failure]\n",
		errStr(err), time.Since(start).Round(10*time.Millisecond))

	note("Same two primitives, opposite order, completely different",
		"failure mode. The second never reaches the delay at all.")

	// Independent sampling compounds.
	explain("Each primitive samples independently, so blast radii compound.")
	const n = 10000
	c := fault.Chain(
		fault.Blackhole(fault.Rate(0.5)),
		fault.Blackhole(fault.Rate(0.5)),
	)
	hit := 0
	for i := 0; i < n; i++ {
		if err := c.Inject(context.Background(), noop); err != nil {
			hit++
		}
	}
	fmt.Printf("  Chain of two Rate(0.50) -> %.1f%% faulted (not 50%%: 1-0.5*0.5 = 75%%)\n",
		100*float64(hit)/n)
}

// --- 5 -----------------------------------------------------------------

func demoMiddleware() {
	explain("Any Go handler opts in with one line. The status comes from",
		"the injected error, not from a guess in the middleware.")

	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "real response")
	})

	cases := []struct {
		label string
		p     fault.Primitive
	}{
		{"Passthrough()", fault.Passthrough()},
		{"Blackhole(Always())", fault.Blackhole(fault.Always())},
		{"Error(nil, Always())", fault.Error(nil, fault.Always())},
		{`ErrorStatus("…", 502, Always())`, fault.ErrorStatus("upstream refused", 502, fault.Always())},
		{"Timeout(20ms, Always())", fault.Timeout(20*time.Millisecond, fault.Always())},
	}

	for _, tc := range cases {
		h := fault.Middleware(tc.p)(backend)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		body := strings.TrimSpace(rec.Body.String())
		if len(body) > 34 {
			body = body[:34] + "…"
		}
		fmt.Printf("  %-34s -> %d  %s\n", tc.label, rec.Code, body)
	}

	note("Blackhole is 503, a bare error is 500, an explicit status is",
		"honoured, and a hung dependency surfaces as 504.")
}

// --- 6 -----------------------------------------------------------------

func demoResource() {
	explain("Removing capacity perturbs no individual call — it changes the",
		"rate at which all of them are served. That needs a different shape.")

	// A stand-in for pool.SetSize: apply a new value, return the old.
	replicas := 6
	setReplicas := func(n int) int { prev := replicas; replicas = n; return prev }

	f := fault.Capacity("capacity", setReplicas, 2)
	fmt.Printf("  before Apply   : replicas = %d\n", replicas)
	restore := f.Apply()
	fmt.Printf("  after Apply    : replicas = %d   <- the fault\n", replicas)
	restore()
	fmt.Printf("  after restore  : replicas = %d\n", replicas)

	restore()
	restore()
	fmt.Printf("  after 2 more restores: replicas = %d   (idempotent)\n", replicas)

	explain("Apply's signature is already exp.Fault — func() (rollback func()) —",
		"so an experiment consumes it with no adapter:")
	fmt.Println("      Inject: capacityFault.Apply")

	// Overlapping applies must not corrupt the saved previous value.
	var mu sync.Mutex
	size := 8
	guarded := func(n int) int { mu.Lock(); defer mu.Unlock(); prev := size; size = n; return prev }
	g := fault.Capacity("capacity", guarded, 1)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r := g.Apply(); time.Sleep(time.Millisecond); r() }()
	}
	wg.Wait()
	mu.Lock()
	final := size
	mu.Unlock()
	fmt.Printf("  8 overlapping apply/restore pairs from 8 goroutines -> final = %d (want 8)\n", final)

	note("Without serialization the second Apply would capture the first",
		"fault's value as 'previous', and its restore would reinstate the",
		"fault instead of undoing it.")
}

// --- 7 -----------------------------------------------------------------

func demoMetrics() {
	explain("A call the sampler declined is not an injected fault, so it is",
		"not counted. Watch Rate(0) move nothing.")

	before := counters()

	// 1000 calls at Rate(0): no faults, no counts.
	p0 := fault.Latency(time.Microsecond, fault.Rate(0))
	for i := 0; i < 1000; i++ {
		_ = p0.Inject(context.Background(), noop)
	}
	fmt.Printf("  1000 calls through Latency(Rate(0)) -> counter delta: %d\n",
		counters()["latency"]-before["latency"])

	// 25 calls at Rate(1): 25 faults, 25 counts.
	p1 := fault.Latency(time.Microsecond, fault.Rate(1))
	for i := 0; i < 25; i++ {
		_ = p1.Inject(context.Background(), noop)
	}
	fmt.Printf("  25 calls through Latency(Rate(1))   -> counter delta: %d\n",
		counters()["latency"]-before["latency"])

	fmt.Println()
	fmt.Println("  What /metrics exposes after this run:")
	var sb strings.Builder
	_ = metrics.WriteTo(&sb)
	for _, line := range strings.Split(sb.String(), "\n") {
		if strings.HasPrefix(line, "flowgate_fault_injected_total{") {
			fmt.Printf("    %s\n", line)
		}
	}
}

// --- helpers ------------------------------------------------------------

func noop(context.Context) error { return nil }

func errStr(err error) string {
	if err == nil {
		return "<nil>"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context.DeadlineExceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "context.Canceled"
	}
	return err.Error()
}

// counters parses the current fault counters out of the exposition
// output — the same text a Prometheus scrape would read.
func counters() map[string]int64 {
	var sb strings.Builder
	_ = metrics.WriteTo(&sb)
	out := map[string]int64{}
	const prefix = `flowgate_fault_injected_total{primitive="`
	for _, line := range strings.Split(sb.String(), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		rest := line[len(prefix):]
		end := strings.Index(rest, `"`)
		if end < 0 {
			continue
		}
		name := rest[:end]
		var v int64
		_, _ = fmt.Sscanf(strings.TrimSpace(rest[end:]), `"} %d`, &v)
		out[name] = v
	}
	return out
}

func section(title string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 68))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", 68))
}

func explain(lines ...string) {
	fmt.Println()
	for _, l := range lines {
		fmt.Println("  " + l)
	}
	fmt.Println()
}

func note(lines ...string) {
	fmt.Println()
	for _, l := range lines {
		fmt.Println("  -> " + l)
	}
}

// bar draws a crude proportion bar so the sampled percentages are
// comparable at a glance rather than by reading digits.
func bar(pct float64) string {
	n := int(pct/2 + 0.5)
	if n > 50 {
		n = 50
	}
	return strings.Repeat("#", n)
}
