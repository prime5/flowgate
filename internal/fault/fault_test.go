package fault

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prime5/flowgate/internal/metrics"
)

// noopWork is the unit of work primitives wrap in these tests. It
// records that it ran so a test can tell "the fault let the call
// through" from "the fault replaced the call".
func noopWork(ran *bool) func(context.Context) error {
	return func(context.Context) error {
		if ran != nil {
			*ran = true
		}
		return nil
	}
}

// faultCount reads the current value of fault_injected_total for one
// primitive. The counter is process-global, so tests must compare
// deltas rather than absolute values.
func faultCount(t *testing.T, name string) int64 {
	t.Helper()
	var sb strings.Builder
	if err := metrics.WriteTo(&sb); err != nil {
		t.Fatalf("metrics.WriteTo: %v", err)
	}
	want := `flowgate_fault_injected_total{primitive="` + name + `"} `
	for _, line := range strings.Split(sb.String(), "\n") {
		if strings.HasPrefix(line, want) {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, want)), 10, 64)
			if err != nil {
				t.Fatalf("parsing %q: %v", line, err)
			}
			return n
		}
	}
	return 0 // never incremented yet
}

// --- rate 0 / rate 1 for every primitive -------------------------------

func TestPrimitivesAtRateZeroAndOne(t *testing.T) {
	// A primitive at Rate(0) must be a pass-through: work runs, no
	// error, nothing counted. At Rate(1) it must perturb every call.
	// "perturbed" differs per primitive, so each case says what it
	// expects.
	cases := []struct {
		name string
		// build returns the primitive at the given sampler.
		build func(Sampler) Primitive
		// workRunsWhenFiring is false for primitives that replace the
		// call entirely (error, timeout, blackhole) and true for those
		// that merely perturb it (latency, cpuburn).
		workRunsWhenFiring bool
		// wantErr, when non-nil, is the error Inject must return while
		// firing.
		wantErr error
		// metric is the label the primitive increments.
		metric string
	}{
		{
			name:               "latency",
			build:              func(s Sampler) Primitive { return Latency(5*time.Millisecond, s) },
			workRunsWhenFiring: true,
			metric:             "latency",
		},
		{
			name:               "error",
			build:              func(s Sampler) Primitive { return Error(nil, s) },
			workRunsWhenFiring: false,
			wantErr:            ErrInjected,
			metric:             "error",
		},
		{
			name:               "timeout",
			build:              func(s Sampler) Primitive { return Timeout(5*time.Millisecond, s) },
			workRunsWhenFiring: false,
			wantErr:            context.DeadlineExceeded,
			metric:             "timeout",
		},
		{
			name:               "cpuburn",
			build:              func(s Sampler) Primitive { return CPUBurn(5*time.Millisecond, s) },
			workRunsWhenFiring: true,
			metric:             "cpuburn",
		},
		{
			name:               "blackhole",
			build:              Blackhole,
			workRunsWhenFiring: false,
			wantErr:            ErrBlackhole,
			metric:             "blackhole",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+"/rate0", func(t *testing.T) {
			before := faultCount(t, tc.metric)
			p := tc.build(Rate(0))
			ran := false
			if err := p.Inject(context.Background(), noopWork(&ran)); err != nil {
				t.Fatalf("rate 0 returned %v, want nil", err)
			}
			if !ran {
				t.Fatal("rate 0 did not run work; a disabled fault must be a pass-through")
			}
			if got := faultCount(t, tc.metric); got != before {
				t.Fatalf("counter moved %d -> %d at rate 0; a sampled-out call is not an injected fault", before, got)
			}
		})

		t.Run(tc.name+"/rate1", func(t *testing.T) {
			before := faultCount(t, tc.metric)
			p := tc.build(Rate(1))
			ran := false
			err := p.Inject(context.Background(), noopWork(&ran))

			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if ran != tc.workRunsWhenFiring {
				t.Fatalf("work ran = %v, want %v", ran, tc.workRunsWhenFiring)
			}
			if got, want := faultCount(t, tc.metric), before+1; got != want {
				t.Fatalf("counter = %d, want %d (exactly one increment per injected fault)", got, want)
			}
		})
	}
}

// --- context cancellation ----------------------------------------------

func TestLatencyRespectsContextCancellation(t *testing.T) {
	// A fault that ignores cancellation is a bug, not a fault: the
	// caller's timeout budget still has to mean something.
	p := Latency(10*time.Second, Always())
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	ran := false
	start := time.Now()
	err := p.Inject(ctx, noopWork(&ran))
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if ran {
		t.Fatal("work ran after cancellation; the sleep should have aborted first")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took %v to notice cancellation of a 10s sleep", elapsed)
	}
}

func TestTimeoutHonorsNearerDeadline(t *testing.T) {
	// maxHold is 10s but the caller's deadline is 30ms: the deadline
	// wins, so an injected hang can never outlive the caller.
	p := Timeout(10*time.Second, Always())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.Inject(ctx, noopWork(nil))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("held %v, want ~30ms: the nearer deadline should win", elapsed)
	}
}

func TestCPUBurnRespectsContextCancellation(t *testing.T) {
	p := CPUBurn(10*time.Second, Always())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	ran := false
	err := p.Inject(ctx, noopWork(&ran))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if ran {
		t.Fatal("work ran despite cancellation mid-burn")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("burned %v past a 30ms deadline; the loop is not checking ctx often enough", elapsed)
	}
}

// --- error primitive through the middleware ----------------------------

func TestMiddlewareWritesConfiguredStatus(t *testing.T) {
	cases := []struct {
		name       string
		primitive  Primitive
		wantStatus int
		wantCalled bool
	}{
		{
			name:       "error with explicit status",
			primitive:  ErrorStatus("upstream refused", http.StatusBadGateway, Always()),
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "default injected error is 500",
			primitive:  Error(nil, Always()),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "blackhole is 503",
			primitive:  Blackhole(Always()),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "plain error falls back to 500",
			primitive:  Error(errors.New("no status attached"), Always()),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "disabled primitive reaches the handler",
			primitive:  Error(nil, Never()),
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h := Middleware(tc.primitive)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}))

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if called != tc.wantCalled {
				t.Fatalf("handler called = %v, want %v", called, tc.wantCalled)
			}
		})
	}
}

func TestMiddlewareTimeoutIsGatewayTimeout(t *testing.T) {
	h := Middleware(Timeout(10*time.Millisecond, Always()))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("handler must not run when the dependency hangs")
		}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusGatewayTimeout)
	}
}

// --- sampler distribution ----------------------------------------------

func TestRateSamplerDistribution(t *testing.T) {
	// Generous tolerance on purpose: this asserts the sampler is
	// roughly fair, not that it passes a statistics exam. A tight
	// bound here would be a flaky test, which is worse than no test.
	const (
		n    = 10000
		rate = 0.5
		tol  = 0.05 // ±5 percentage points
	)
	s := Rate(rate)
	hits := 0
	for i := 0; i < n; i++ {
		if s.Sample() {
			hits++
		}
	}
	got := float64(hits) / n
	if got < rate-tol || got > rate+tol {
		t.Fatalf("sampled %.3f of calls, want %.2f ±%.2f", got, rate, tol)
	}
}

func TestAlwaysAndNever(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if !Always().Sample() {
			t.Fatal("Always() declined to sample")
		}
		if Never().Sample() {
			t.Fatal("Never() sampled")
		}
	}
	// Rate clamps out of range rather than panicking.
	if Rate(-1).Sample() {
		t.Fatal("Rate(-1) should behave as Never")
	}
	if !Rate(2).Sample() {
		t.Fatal("Rate(2) should behave as Always")
	}
}

func TestNilSamplerFailsSafe(t *testing.T) {
	// A forgotten sampler must disable the fault, not enable it.
	ran := false
	if err := Blackhole(nil).Inject(context.Background(), noopWork(&ran)); err != nil {
		t.Fatalf("nil sampler returned %v, want nil", err)
	}
	if !ran {
		t.Fatal("nil sampler injected a fault; a fault library must fail safe")
	}
}

// --- composition --------------------------------------------------------

func TestChainAppliesInDocumentedOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string

	// Two recording primitives stand in for real ones so the test
	// observes ordering directly rather than inferring it from timing.
	rec := func(name string) Primitive {
		return primitiveFunc{
			name: name,
			fn: func(ctx context.Context, work func(context.Context) error) error {
				mu.Lock()
				order = append(order, "enter:"+name)
				mu.Unlock()
				err := work(ctx)
				mu.Lock()
				order = append(order, "exit:"+name)
				mu.Unlock()
				return err
			},
		}
	}

	ranWork := false
	c := Chain(rec("a"), rec("b"), rec("c"))
	if err := c.Inject(context.Background(), noopWork(&ranWork)); err != nil {
		t.Fatalf("chain returned %v", err)
	}
	if !ranWork {
		t.Fatal("chain never reached the work function")
	}

	want := []string{"enter:a", "enter:b", "enter:c", "exit:c", "exit:b", "exit:a"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v (first argument must be outermost)", order, want)
		}
	}
}

func TestChainShortCircuitsOnError(t *testing.T) {
	// Error outermost: the chain fails fast and the latency primitive
	// inside it never runs. This is the ordering difference the
	// package doc calls out — a fast failure, not a slow one.
	ranWork := false
	start := time.Now()
	err := Chain(
		Error(nil, Always()),
		Latency(5*time.Second, Always()),
	).Inject(context.Background(), noopWork(&ranWork))

	if !errors.Is(err, ErrInjected) {
		t.Fatalf("err = %v, want ErrInjected", err)
	}
	if ranWork {
		t.Fatal("work ran despite an outermost error primitive")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %v; the inner 5s latency should never have been reached", elapsed)
	}
}

func TestChainDegenerateCases(t *testing.T) {
	ran := false
	if err := Chain().Inject(context.Background(), noopWork(&ran)); err != nil || !ran {
		t.Fatalf("empty chain: err = %v, ran = %v; want nil, true", err, ran)
	}
	only := Blackhole(Always())
	if got := Chain(only); got != only {
		t.Fatal("Chain of one should return that primitive unchanged")
	}
}

func TestPassthroughNeverInjects(t *testing.T) {
	ran := false
	if err := Passthrough().Inject(context.Background(), noopWork(&ran)); err != nil || !ran {
		t.Fatalf("passthrough: err = %v, ran = %v; want nil, true", err, ran)
	}
}

// --- resource faults ----------------------------------------------------

func TestCapacityAppliesAndRestores(t *testing.T) {
	before := faultCount(t, "capacity")

	size := 6
	set := func(n int) int { prev := size; size = n; return prev }
	f := Capacity("capacity", set, 2)

	if size != 6 {
		t.Fatalf("size = %d before Apply, want 6", size)
	}
	restore := f.Apply()
	if size != 2 {
		t.Fatalf("size = %d after Apply, want 2", size)
	}
	if got, want := faultCount(t, "capacity"), before+1; got != want {
		t.Fatalf("counter = %d, want %d", got, want)
	}

	restore()
	if size != 6 {
		t.Fatalf("size = %d after restore, want 6", size)
	}
}

func TestCapacityRestoreIsIdempotent(t *testing.T) {
	// A double rollback must not re-apply or double-unlock. exp.Run
	// calls rollback from a defer; a retry path calling it again
	// should be harmless.
	size := 10
	f := Capacity("capacity", func(n int) int { prev := size; size = n; return prev }, 1)

	restore := f.Apply()
	restore()
	restore()
	restore()

	if size != 10 {
		t.Fatalf("size = %d after repeated restore, want 10", size)
	}
}

func TestCapacitySerializesOverlappingApplies(t *testing.T) {
	// Two overlapping applications would each capture the other's
	// value as "previous"; the second restore would then reinstate the
	// fault instead of undoing it. Apply holds a lock until its
	// restore runs, so the second Apply waits.
	var mu sync.Mutex
	size := 8
	set := func(n int) int {
		mu.Lock()
		defer mu.Unlock()
		prev := size
		size = n
		return prev
	}
	f := Capacity("capacity", set, 2)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			restore := f.Apply()
			time.Sleep(time.Millisecond)
			restore()
		}()
	}
	wg.Wait()

	mu.Lock()
	got := size
	mu.Unlock()
	if got != 8 {
		t.Fatalf("size = %d after overlapping apply/restore pairs, want 8", got)
	}
}

func TestCapacityNilSetterIsSafe(t *testing.T) {
	f := Capacity("", nil, 3)
	if f.Name() != "capacity" {
		t.Fatalf("name = %q, want the default %q", f.Name(), "capacity")
	}
	f.Apply()() // apply then restore; must not panic
}

func TestResourceSatisfiesExperimentFaultShape(t *testing.T) {
	// The whole point of Apply's signature: it is already
	// exp.Fault — func() (rollback func()) — so an experiment can take
	// it directly with no adapter. This compiles only if that holds.
	size := 4
	f := Capacity("capacity", func(n int) int { prev := size; size = n; return prev }, 1)

	var asExperimentFault func() func() = f.Apply
	rollback := asExperimentFault()
	if size != 1 {
		t.Fatalf("size = %d, want 1", size)
	}
	rollback()
	if size != 4 {
		t.Fatalf("size = %d, want 4", size)
	}
}

// --- toggle -------------------------------------------------------------

func TestToggleOnOff(t *testing.T) {
	tog := NewToggle(Blackhole(Always()))

	ran := false
	if err := tog.Inject(context.Background(), noopWork(&ran)); err != nil || !ran {
		t.Fatalf("toggle starts off: err = %v, ran = %v; want nil, true", err, ran)
	}
	if tog.Enabled() {
		t.Fatal("toggle should start disabled")
	}

	off := tog.On()
	if !tog.Enabled() {
		t.Fatal("toggle should be enabled after On()")
	}
	ran = false
	if err := tog.Inject(context.Background(), noopWork(&ran)); !errors.Is(err, ErrBlackhole) {
		t.Fatalf("enabled toggle: err = %v, want ErrBlackhole", err)
	}
	if ran {
		t.Fatal("work ran while the toggle was on")
	}

	off()
	if tog.Enabled() {
		t.Fatal("toggle should be disabled after its rollback ran")
	}
	ran = false
	if err := tog.Inject(context.Background(), noopWork(&ran)); err != nil || !ran {
		t.Fatalf("after rollback: err = %v, ran = %v; want nil, true", err, ran)
	}
}

func TestNilToggleIsPassthrough(t *testing.T) {
	ran := false
	if err := NewToggle(nil).Inject(context.Background(), noopWork(&ran)); err != nil || !ran {
		t.Fatalf("nil-wrapped toggle: err = %v, ran = %v; want nil, true", err, ran)
	}
}

// --- concurrency --------------------------------------------------------

func TestPrimitivesAreConcurrencySafe(t *testing.T) {
	// The real assertion here is made by -race, not by the counters:
	// this exercises every primitive and the shared RNG pool from many
	// goroutines at once.
	p := Chain(
		Latency(time.Millisecond, Rate(0.5)),
		LatencyJitter(time.Millisecond, time.Millisecond, Rate(0.5)),
		CPUBurn(time.Millisecond, Rate(0.3)),
		Error(nil, Rate(0.2)),
		Blackhole(Rate(0.1)),
	)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = p.Inject(context.Background(), noopWork(nil))
			}
		}()
	}
	wg.Wait()
}

func TestToggleIsConcurrencySafe(t *testing.T) {
	tog := NewToggle(Latency(time.Microsecond, Always()))
	var wg sync.WaitGroup

	// Flip the toggle while other goroutines read through it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			off := tog.On()
			off()
		}
	}()

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = tog.Inject(context.Background(), noopWork(nil))
				_ = tog.Enabled()
			}
		}()
	}
	wg.Wait()
}

// --- test helper type ---------------------------------------------------

// primitiveFunc adapts a function to Primitive, for tests that need to
// observe ordering.
type primitiveFunc struct {
	name string
	fn   func(context.Context, func(context.Context) error) error
}

func (p primitiveFunc) Name() string { return p.name }
func (p primitiveFunc) Inject(ctx context.Context, work func(context.Context) error) error {
	return p.fn(ctx, work)
}
