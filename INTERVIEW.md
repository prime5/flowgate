# Interview notes: flowgate resilience experiments

Use these notes before talking about this repo. Every experiment
below has a steady state, a fault, a blast radius, and a rollback —
say those four words and the story tells itself.

## The project in one minute

flowgate is a reliability gateway: it takes API requests and applies
rate limiting, circuit breaking, and load shedding before they reach
a backend, with Prometheus metrics so you can watch it behave. This
branch adds a fault-injection playground on top, shaped by three
real systems from my background:

1. **Directory sync (Cloud Identity Engine).** The sync pool
   (`internal/pool`) models the Kubernetes replica set that kept
   directory sync running: `POST /scale?replicas=N` is `kubectl
   scale`. Under-provision it and the backlog grows until batches
   miss their window — the incident this models. The fix, adding
   pods, is the same, and `deploy/k8s/hpa.yaml` automates it.
2. **MPP fan-out (Greenplum).** `GET /query` scatters work across
   shards and gathers. Fault primitives: a straggler shard
   (`&straggler=2&straggler_ms=500`) and a hot partition
   (`&skew=0.9`). The lesson: query latency is the *max* of the
   shards, so one bad shard dominates everything. That is why
   bulkheads and hedged requests exist.
3. **Load and fault infrastructure.** The experiment runner
   (`internal/exp`) enforces the discipline: define steady state,
   inject exactly one fault, cap the blast radius, always roll
   back, give the system a bounded window to recover, and return a
   verdict as JSON. `experiments/sync-stall.js` is the k6 version;
   `scripts/ephemeral-env.sh` runs it all on a throwaway kind
   cluster; `scripts/pod-kill.sh` deletes real pods with a
   blast-radius guard.

## Fault primitives (`internal/fault`)

A **primitives library** is the standard-issue menu of ways to break
things, packaged so nobody writes a bespoke breakage script again.
Without one, every team hand-rolls "sleep here, return 500 there" and
no two faults behave the same. With one, a fault is a dependency you
import.

This is the third piece of the model. The repo now has all three:

| Piece | Package | What it is |
|---|---|---|
| Containment | `internal/breaker`, `internal/shedder` | the defenses, running continuously |
| Primitives | `internal/fault` | the standard ways to cause a fault |
| Experiments | `internal/exp` | the discipline wrapped around one fault |

**Faults come in two shapes, and the library has both.** This is the
design point worth being able to defend:

- **`Primitive`** is *call-level*. It wraps a unit of work and
  perturbs it — latency, error, timeout, CPU burn, blackhole. It takes
  a `Sampler`, because there is a fraction of calls to confine it to.
- **`Resource`** is *system-level*. It changes the environment the
  system runs in and perturbs no individual call — capacity loss is
  the one implemented. No sampler, because there is no per-call
  fraction; its blast radius is the magnitude of the change.

A library with only the first shape couldn't express the most common
real outage there is: service rate falls below arrival rate and the
backlog grows without bound. That is the Cloud Identity Engine
incident, and it is `fault.Capacity`.

The five call-level primitives:

- **Latency** — delay the call. Models a slow dependency. Respects
  context cancellation, so an injected delay never outlives the
  caller's timeout budget.
- **Error** — fail a sampled fraction with a configurable error and
  HTTP status.
- **Timeout** — hold until the deadline, then return the context
  error. Models a hung dependency. Bounded by `maxHold` when the
  caller supplied no deadline, so an injected hang can't become an
  unbounded one.
- **CPUBurn** — spin the CPU for a bounded duration. Models resource
  pressure. One goroutine per injected call, deliberately — a
  primitive that can saturate a whole machine doesn't have a
  controllable blast radius.
- **Blackhole** — always fail, never call the dependency. Models DNS
  failure or severed connectivity.

**The adoption story is one line.** Any Go handler opts in with:

```go
handler := fault.Middleware(fault.Latency(100*time.Millisecond, fault.Rate(0.1)))(myHandler)
```

**Blast radius lives in the primitive.** Every one takes a `Sampler`:
`Rate(0.1)` confines the fault to 10% of calls, `Rate(0)` is a
no-op, `Always()` hits everything. A nil sampler disables the fault
rather than enabling it — a fault library has to fail safe.

**Composition is ordered and the order is observable.**
`Chain(Latency, Error)` pays the delay and *then* fails — a slow
failure. `Chain(Error, Latency)` fails immediately and never reaches
the delay — a fast failure. Each primitive samples independently, so
chaining two `Rate(0.5)` primitives perturbs ~75% of calls, not 50%.

**Observability:** every injected fault increments
`flowgate_fault_injected_total{primitive="..."}`. A call the sampler
declined is *not* an injected fault and is not counted — the counter
measures faults caused, not faults considered.

### Things I would say

- "The defenses were already steady-state hypotheses; this package is
  what puts them under test."
- "Blast radius is the same instinct as rate limiting, pointed at the
  experiment instead of at production traffic."
- "A fault that ignores context cancellation is a bug, not a fault."
- "Faults come in two shapes. Most libraries only ship the call-level
  one, and then can't model the outage that actually happens most —
  not enough capacity for the offered load."
- "From the caller's side a 429 is already an injected fault — so the
  gateway is both the system under test and a fault source for
  somebody else's client."

## Experiments

### pod-kill (`POST /experiments/pod-kill`)
- **Steady state:** sync queue depth < 50.
- **Fault:** seed load at 200 batches/s, then shrink the pool
  6 -> 2 replicas mid-run.
- **Blast radius:** 4/6 of replicas (0.67).
- **Rollback:** restore 6, allow up to 5s for the backlog to drain,
  then restore the pre-experiment count.
- **Verdict:** `Held` is false (steady state violated — that's the
  point), `BaselineOK` and `Recovered` true. Verified locally:

  ```json
  {"Held":false,"BaselineOK":true,"Recovered":true,"BlastRadius":0.667,
   "Detail":"steady state violated during fault window"}
  ```
- Only one experiment runs at a time; a concurrent call gets 409.
- **The fault is `fault.Capacity`** — a `Resource` fault, not one of
  the five call-level primitives, because removing capacity perturbs
  no individual call; it changes the rate at which all of them are
  served. `Apply` already returns its own restore function, so the
  experiment's entire `Inject` closure is the fault itself.

### backend-latency (`POST /experiments/backend-latency`)
The primitive-driven experiment, and the one that shows the library
earning its place.
- **Steady state:** a representative backend call completes inside a
  100ms budget.
- **Fault:** `fault.Latency(250ms, Always())`, already wired in front
  of the backend and switched off. The experiment's entire `Inject`
  closure is `Toggle.On` — its return value is already the rollback,
  so nothing bespoke gets written per experiment. That is the point of
  a primitives library.
- **Blast radius:** 1.0 (`Always()` samples every call).
- **Why the fault sits *inside* `ratelimit.Wrap`:** the shedder,
  limiter and breaker see the request first, so an injected delay is
  the *dependency* being slow — which is exactly the condition the
  circuit breaker exists to survive.

### sync-stall (k6, `experiments/sync-stall.js`)
- 75s, three phases: baseline (t=0-20s), fault (t=20-50s: pool
  scaled to 1 replica), recovery (t=52s+ after a 2s grace window:
  6 replicas).
- Requests are tagged by phase. Thresholds: zero drops in baseline
  and recovery, *some* drops in the fault phase (the fault must
  bite), p95 < 500ms, failures < 1% (a 429 is a loud drop, not a
  failure).
- Local run (30 VUs, ~160 req/s): baseline 0 drops, fault 2,302
  drops with the queue pinned at its 1,000 cap, recovery 0 drops,
  p95 2.1ms, 0% failed.

### MPP faults (`GET /query`)
- `?shards=8&keys=1000` — baseline, max 25ms.
- `?shards=8&keys=1000&straggler=3&straggler_ms=500` — one slow
  shard dominates: max 525ms.
- `?shards=8&keys=1000&skew=0.9` — hot partition dominates: max
  ~183ms.
- Inputs are bounded (shards ≤ 64, keys ≤ 100,000) so the simulator
  can't be turned into a goroutine bomb.

## Things I would say

- "The in-process pool makes the failure deterministic; the
  Kubernetes layer then runs the same shape against real pods."
- "A fault without a rollback is not an experiment, it's an
  outage with a write-up."
- "Recovery is a time-bounded claim, not an instant one: the
  verdict checks the backlog drains within a window after
  rollback."
- "The HPA is the real fix — the human runs kubectl at 2am, the
  platform scales before anyone is paged."

## Known limits (say them first)

- The pool workers are goroutines, not pods; only the k8s
  manifests run real pods. `POST /scale` resizes this process's
  goroutine pool and calls no orchestrator.
- `internal/fault` has no **graceful degradation** counterpart and no
  **canary**. Blackhole is the fault that *should* drive a
  degradation path; flowgate has none to drive, because the breaker
  fails fast rather than serving stale data. The nearest real canary
  experience is staged multi-region release verification, which is a
  different job.
- The backend latency is simulated; the queue dynamics are real.
- `/scale` only affects the flowgate process you hit — there is
  no shared control plane across pods. In-cluster, the k6 Job's
  `/scale` call lands on one pod behind the Service.
- The experiment verdict's timing depends on machine speed;
  thresholds (queue < 50, 8s window, 5s recovery) are tuned for a
  laptop.
- `/scale` and `/experiments/*` are unauthenticated: fine for a
  lab, not something to expose on the public Fly deployment.
