// Package fault is flowgate's fault-injection primitives library: the
// standard-issue, reusable ways of causing a failure on purpose,
// packaged so another Go service can import them and opt in with one
// line of middleware.
//
// # The three-piece model
//
// flowgate's resilience story has three parts, and this package is the
// middle one:
//
//  1. Containment — internal/breaker and internal/shedder. These run
//     continuously in production and bound the damage a failing
//     dependency or an overload can do. They are the defenses.
//  2. Primitives — this package. Reusable ways to cause a fault. They
//     come in two shapes, because faults do:
//     Primitive is call-level (latency, error, timeout, CPU burn,
//     blackhole) — it wraps a unit of work and carries its own
//     blast-radius control in the form of a Sampler. Resource is
//     system-level (capacity loss) — it changes the environment the
//     system runs in and perturbs no individual call.
//  3. Experiments — internal/exp. The discipline wrapped around a
//     primitive: declare a steady state, inject exactly one fault,
//     observe, always roll back, return a verdict.
//
// A containment primitive is a steady-state hypothesis someone already
// implemented in code; this package exists to put that hypothesis under
// test. Note also that from the caller's side a 429 or a 503 from the
// gateway is itself an injected fault, so flowgate can serve as a fault
// source for someone else's client as well as a system under test.
//
// # Blast radius
//
// Every primitive is constructed with a Sampler. Rate(0) makes the
// primitive a no-op; Rate(1) makes it affect every call. That fraction
// is the primitive-level blast-radius control, and it is deliberately
// the same idea the breaker and shedder apply to production traffic,
// pointed at the experiment instead.
//
// # Composition
//
// Chain composes primitives outside-in: the first argument is the
// outermost wrapper and runs first. See Chain for the full ordering
// rules.
package fault

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/prime5/flowgate/internal/metrics"
)

// Primitive is a reusable fault. It wraps a unit of work: Inject either
// runs work (possibly perturbed — delayed, starved of CPU) or refuses
// to run it and returns an injected error instead.
//
// A Primitive must be safe for concurrent use by multiple goroutines.
type Primitive interface {
	// Name identifies the primitive in metrics and logs, e.g. "latency".
	Name() string
	// Inject runs work with the fault applied, or returns an injected
	// error. When the sampler declines, Inject must run work unchanged
	// and return its error verbatim.
	Inject(ctx context.Context, work func(ctx context.Context) error) error
}

// Resource is the other kind of fault: one that changes the
// environment the system runs in, rather than perturbing any
// individual call. Removing capacity, filling a disk, pausing a
// process — none of these wrap a unit of work, so none of them fit
// Primitive, but all of them are faults.
//
// The two shapes are both first-class here. A fault-injection library
// that could only express call-level faults would be unable to model
// the most common real outage there is: not enough capacity for the
// offered load.
//
// Apply returns its own undo function, so a Resource fault plugs
// directly into an experiment — its signature is exactly exp.Fault.
type Resource interface {
	Name() string
	// Apply injects the fault and returns the function that removes
	// it. Calling restore must return the system to its prior state.
	Apply() (restore func())
}

// Sampler decides whether the fault applies to a given call. This is
// blast-radius control at the primitive level: a sampler that returns
// true for 10% of calls confines the fault to 10% of traffic.
//
// Resource faults do not take a Sampler — they are not per-call, so
// there is no fraction of calls to confine. Their blast radius is the
// magnitude of the change, declared on the experiment.
//
// Sample must be safe for concurrent use.
type Sampler interface{ Sample() bool }

// --- samplers ---------------------------------------------------------

// randPool hands each goroutine its own *rand.Rand instead of sharing
// one.
//
// The top-level math/rand functions are safe for concurrent use, but
// they serialize every caller on a single global mutex — a poor
// property for something that may sit in a hot request path, where the
// fault library would become the contention it is supposed to be
// testing for. Pooling keeps the RNG synchronized without that
// contention, and it is the only place in this package that touches
// math/rand.
type randPool struct {
	pool sync.Pool
	mu   sync.Mutex
	seed int64
}

func newRandPool() *randPool {
	rp := &randPool{seed: time.Now().UnixNano()}
	rp.pool.New = func() any {
		// Each new *rand.Rand gets a distinct seed; the counter is
		// guarded because pool.New can run on several goroutines at
		// once. Determinism is not wanted here — two primitives
		// sampling in lockstep would make faults correlate.
		rp.mu.Lock()
		rp.seed++
		v := rp.seed
		rp.mu.Unlock()
		return rand.New(rand.NewSource(v))
	}
	return rp
}

func (rp *randPool) float64() float64 {
	r := rp.pool.Get().(*rand.Rand)
	v := r.Float64()
	rp.pool.Put(r)
	return v
}

func (rp *randPool) int63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	r := rp.pool.Get().(*rand.Rand)
	v := r.Int63n(n)
	rp.pool.Put(r)
	return v
}

// rateSampler fires with probability p.
type rateSampler struct {
	p  float64
	rp *randPool
}

// Rate returns a Sampler that fires with probability p, clamped to
// [0,1]. Rate(0) never fires and Rate(1) always fires, both without
// consuming randomness.
func Rate(p float64) Sampler {
	if p <= 0 {
		return Never()
	}
	if p >= 1 {
		return Always()
	}
	return &rateSampler{p: p, rp: newRandPool()}
}

func (s *rateSampler) Sample() bool { return s.rp.float64() < s.p }

type alwaysSampler struct{}

func (alwaysSampler) Sample() bool { return true }

// Always returns a Sampler that fires on every call (blast radius 1.0).
func Always() Sampler { return alwaysSampler{} }

type neverSampler struct{}

func (neverSampler) Sample() bool { return false }

// Never returns a Sampler that never fires, which makes any primitive
// built with it a pass-through. Useful as a disabled default.
func Never() Sampler { return neverSampler{} }

// --- injected errors --------------------------------------------------

// StatusError is an injected error that carries the HTTP status
// Middleware should write for it. Primitives that model a remote
// failure implement this so the middleware does not have to guess.
type StatusError interface {
	error
	HTTPStatus() int
}

type statusError struct {
	msg    string
	status int
}

func (e *statusError) Error() string   { return e.msg }
func (e *statusError) HTTPStatus() int { return e.status }

// ErrBlackhole is returned by the Blackhole primitive: the dependency
// is entirely unreachable, as in a DNS failure or severed connectivity.
var ErrBlackhole StatusError = &statusError{
	msg:    "fault: blackhole, dependency unreachable",
	status: http.StatusServiceUnavailable,
}

// ErrInjected is the default error returned by Error when no specific
// error is configured.
var ErrInjected StatusError = &statusError{
	msg:    "fault: injected error",
	status: http.StatusInternalServerError,
}

// --- helpers ----------------------------------------------------------

// fire records one injected fault. Every primitive calls this at the
// moment it decides to perturb a call, and only then — a sampled-out
// call is not an injected fault and must not be counted.
func fire(name string) { metrics.FaultInjectedTotal.Inc(name) }

// sleepCtx sleeps for d, or returns early with ctx.Err() if ctx is
// cancelled first. A fault that ignores cancellation is a bug, not a
// fault: the caller's timeout budget still has to mean something.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// --- Latency ----------------------------------------------------------

type latency struct {
	base    time.Duration
	jitter  time.Duration
	sampler Sampler
	rp      *randPool
}

// Latency returns a primitive that delays work by base before running
// it. The delay respects context cancellation: if ctx is done while
// sleeping, Inject stops and returns ctx.Err() without running work.
//
// This models a slow dependency — the fault a circuit breaker and a
// timeout budget exist to survive.
func Latency(base time.Duration, s Sampler) Primitive {
	return LatencyJitter(base, 0, s)
}

// LatencyJitter is Latency with up to jitter of extra random delay
// added to base, so injected slowness is not perfectly synchronized
// across callers.
func LatencyJitter(base, jitter time.Duration, s Sampler) Primitive {
	l := &latency{base: base, jitter: jitter, sampler: orNever(s)}
	if jitter > 0 {
		l.rp = newRandPool()
	}
	return l
}

func (l *latency) Name() string { return "latency" }

func (l *latency) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	if !l.sampler.Sample() {
		return work(ctx)
	}
	fire(l.Name())
	d := l.base
	if l.rp != nil {
		d += time.Duration(l.rp.int63n(int64(l.jitter)))
	}
	if err := sleepCtx(ctx, d); err != nil {
		return err
	}
	return work(ctx)
}

// --- Error ------------------------------------------------------------

type errorPrim struct {
	err     error
	sampler Sampler
}

// Error returns a primitive that fails a sampled fraction of calls with
// err instead of running work. A nil err means ErrInjected.
//
// To control the HTTP status Middleware writes, pass an error that
// implements StatusError — or use ErrorStatus.
func Error(err error, s Sampler) Primitive {
	if err == nil {
		err = ErrInjected
	}
	return &errorPrim{err: err, sampler: orNever(s)}
}

// ErrorStatus is Error with an explicit HTTP status for the middleware
// to write, e.g. ErrorStatus("upstream refused", 502, Rate(0.1)).
func ErrorStatus(msg string, status int, s Sampler) Primitive {
	return Error(&statusError{msg: msg, status: status}, s)
}

func (e *errorPrim) Name() string { return "error" }

func (e *errorPrim) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	if !e.sampler.Sample() {
		return work(ctx)
	}
	fire(e.Name())
	return e.err
}

// --- Timeout ----------------------------------------------------------

type timeoutPrim struct {
	maxHold time.Duration
	sampler Sampler
}

// Timeout returns a primitive that models a hung dependency: it holds
// the call until the context deadline and then returns the context
// error, never running work.
//
// maxHold bounds the hold when ctx has no deadline of its own, so an
// injected hang can never become an unbounded one. If ctx has a
// deadline nearer than maxHold, the deadline wins.
func Timeout(maxHold time.Duration, s Sampler) Primitive {
	if maxHold <= 0 {
		maxHold = 30 * time.Second
	}
	return &timeoutPrim{maxHold: maxHold, sampler: orNever(s)}
}

func (t *timeoutPrim) Name() string { return "timeout" }

func (t *timeoutPrim) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	if !t.sampler.Sample() {
		return work(ctx)
	}
	fire(t.Name())

	hold := t.maxHold
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d < hold {
			hold = d
		}
	}
	if err := sleepCtx(ctx, hold); err != nil {
		return err
	}
	// Held for the full window without the caller's context firing:
	// report it as a deadline exceeded anyway, since that is what the
	// caller would have seen from a genuinely hung dependency.
	return context.DeadlineExceeded
}

// --- CPUBurn ----------------------------------------------------------

type cpuBurn struct {
	d       time.Duration
	sampler Sampler
}

// CPUBurn returns a primitive that spins the CPU for d before running
// work, modelling resource pressure on the host.
//
// The burn is bounded by d and by context cancellation, and it occupies
// exactly one goroutine per injected call — it does not fan out across
// cores, because a fault primitive that can saturate a whole machine is
// not one with a controllable blast radius. Concurrency comes from the
// number of calls sampled, which the Sampler governs.
func CPUBurn(d time.Duration, s Sampler) Primitive {
	return &cpuBurn{d: d, sampler: orNever(s)}
}

func (c *cpuBurn) Name() string { return "cpuburn" }

func (c *cpuBurn) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	if !c.sampler.Sample() {
		return work(ctx)
	}
	fire(c.Name())

	deadline := time.Now().Add(c.d)
	// Check the clock and the context every few thousand iterations:
	// often enough to stay responsive to cancellation, rarely enough
	// that the loop is actually burning CPU rather than reading clocks.
	for i := 0; ; i++ {
		if i%2048 == 0 {
			if time.Now().After(deadline) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	return work(ctx)
}

// --- Blackhole --------------------------------------------------------

type blackhole struct {
	sampler Sampler
}

// Blackhole returns a primitive that always fails a sampled call with
// ErrBlackhole without running work: the dependency is entirely
// unreachable. This is the DNS-failure / severed-connectivity fault,
// and it is the one that should drive a graceful-degradation path if
// the service has one.
func Blackhole(s Sampler) Primitive { return &blackhole{sampler: orNever(s)} }

func (b *blackhole) Name() string { return "blackhole" }

func (b *blackhole) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	if !b.sampler.Sample() {
		return work(ctx)
	}
	fire(b.Name())
	return ErrBlackhole
}

// --- composition ------------------------------------------------------

type chain struct {
	ps []Primitive
}

// Chain composes primitives outside-in. The first argument is the
// outermost wrapper, so Chain(latency, errorPrim) delays the call and
// then decides whether to fail it, and the work function is innermost:
//
//	Chain(a, b, c).Inject(ctx, work)
//	  == a.Inject(ctx, func(ctx) { b.Inject(ctx, func(ctx) { c.Inject(ctx, work) }) })
//
// The ordering is observable, not cosmetic. Chain(Latency, Error)
// pays the delay and then fails — a slow failure. Chain(Error,
// Latency) fails immediately and never reaches the delay — a fast
// failure. Each primitive samples independently, so a chain of two
// Rate(0.5) primitives perturbs roughly 75% of calls, not 50%.
//
// Chain of nothing is a pass-through; Chain of one is that primitive.
func Chain(ps ...Primitive) Primitive {
	switch len(ps) {
	case 0:
		return passthrough{}
	case 1:
		return ps[0]
	}
	cp := make([]Primitive, len(ps))
	copy(cp, ps)
	return &chain{ps: cp}
}

func (c *chain) Name() string { return "chain" }

func (c *chain) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	// Build the nest from the inside out so the first element ends up
	// outermost.
	next := work
	for i := len(c.ps) - 1; i >= 0; i-- {
		p := c.ps[i]
		inner := next
		next = func(ctx context.Context) error { return p.Inject(ctx, inner) }
	}
	return next(ctx)
}

type passthrough struct{}

func (passthrough) Name() string { return "passthrough" }
func (passthrough) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	return work(ctx)
}

// Passthrough returns a primitive that never injects anything. It
// exists so a service can wire the middleware in permanently and switch
// faults on later by configuration rather than by code change.
func Passthrough() Primitive { return passthrough{} }

// --- resource faults --------------------------------------------------

// capacityFault reduces a numeric capacity and restores it on rollback.
type capacityFault struct {
	name string
	set  func(int) int
	to   int
	mu   sync.Mutex
}

// Capacity returns a Resource fault that changes a numeric capacity to
// `to` and restores the previous value when its restore function runs.
//
// set must apply the new value and return the previous one — which is
// the signature a well-behaved resizer already has (pool.Pool.SetSize,
// for instance). Capacity does not care what the number means: worker
// goroutines, connection-pool slots, replica count.
//
// This is the fault behind the most common real outage there is —
// service rate falls below arrival rate and the backlog grows without
// bound — and it is deliberately not a Primitive, because it perturbs
// no individual call. It changes the rate at which all of them are
// served.
func Capacity(name string, set func(int) int, to int) Resource {
	if name == "" {
		name = "capacity"
	}
	return &capacityFault{name: name, set: set, to: to}
}

func (c *capacityFault) Name() string { return c.name }

func (c *capacityFault) Apply() (restore func()) {
	if c.set == nil {
		return func() {}
	}
	// Serialize apply/restore pairs: two overlapping applications
	// would each capture the other's value as "previous" and the
	// second restore would reinstate the fault, not undo it.
	c.mu.Lock()
	prev := c.set(c.to)
	fire(c.Name())

	var once sync.Once
	return func() {
		once.Do(func() {
			c.set(prev)
			c.mu.Unlock()
		})
	}
}

// --- toggle -----------------------------------------------------------

// Toggle wraps a primitive with a runtime on/off switch, so an
// experiment can inject a fault and roll it back without rebuilding the
// handler chain. A Toggle starts off.
//
// This is the adapter between this package and internal/exp: an
// experiment's Inject closure flips the toggle on and returns a
// rollback that flips it off.
type Toggle struct {
	p  Primitive
	on bool
	mu sync.RWMutex
}

// NewToggle returns a Toggle wrapping p, initially off.
func NewToggle(p Primitive) *Toggle {
	if p == nil {
		p = passthrough{}
	}
	return &Toggle{p: p}
}

// Name reports the wrapped primitive's name.
func (t *Toggle) Name() string { return t.p.Name() }

// On enables the fault and returns a function that disables it again,
// shaped to be used directly as an experiment's rollback.
func (t *Toggle) On() (off func()) {
	t.mu.Lock()
	t.on = true
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		t.on = false
		t.mu.Unlock()
	}
}

// Enabled reports whether the fault is currently active.
func (t *Toggle) Enabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.on
}

// Inject applies the wrapped primitive when the toggle is on, and runs
// work untouched when it is off.
func (t *Toggle) Inject(ctx context.Context, work func(ctx context.Context) error) error {
	t.mu.RLock()
	on := t.on
	t.mu.RUnlock()
	if !on {
		return work(ctx)
	}
	return t.p.Inject(ctx, work)
}

// --- http adapter -----------------------------------------------------

// Middleware adapts a Primitive to net/http so any handler opts in with
// one line:
//
//	handler := fault.Middleware(fault.Latency(100*time.Millisecond, fault.Rate(0.1)))(myHandler)
//
// The request's own context is passed through, so a primitive that
// respects cancellation (all of them do) still respects a client
// disconnect or an upstream deadline.
//
// When the primitive returns an error, the handler is not called and
// the middleware writes a status: the error's own if it implements
// StatusError, context.DeadlineExceeded as 504, anything else as 500.
func Middleware(p Primitive) func(http.Handler) http.Handler {
	if p == nil {
		p = passthrough{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := p.Inject(r.Context(), func(ctx context.Context) error {
				next.ServeHTTP(w, r.WithContext(ctx))
				return nil
			})
			if err == nil {
				return
			}
			http.Error(w, err.Error(), statusFor(err))
		})
	}
}

// statusFor maps an injected error to the HTTP status the middleware
// writes for it.
func statusFor(err error) int {
	var se StatusError
	if errors.As(err, &se) {
		return se.HTTPStatus()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	if errors.Is(err, context.Canceled) {
		// The client went away; nothing useful to send, but a status
		// is required. 499 is nginx's convention for exactly this.
		return 499
	}
	return http.StatusInternalServerError
}

// orNever normalizes a nil Sampler to Never, so a zero-value or
// forgotten sampler disables the fault rather than enabling it. A fault
// library must fail safe.
func orNever(s Sampler) Sampler {
	if s == nil {
		return Never()
	}
	return s
}
