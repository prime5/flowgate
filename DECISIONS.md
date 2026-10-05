# flowgate — design decisions

Not usage docs. Each entry explains *why* something is the way it is, and names
the case against it where there is one.

## Fault primitives

### Why is a forgotten sampler a no-op instead of an error?

A fault library has to fail safe. If forgetting to set a blast radius made the
fault fire at full strength, every accidental wiring would be an outage. A no-op
is visible (nothing happens, the counter doesn't move) and safe. So: no sampler,
no fault.

The general rule: decide which direction an uncaught bug should point. Defaulting
to off costs you a wasted experiment; defaulting to on costs you an incident.
Those aren't symmetric. It also matches Go's convention that zero values are safe
and useful — a zero `Mutex` is unlocked, a zero `Buffer` is empty.

**The case against.** The constructors return `Primitive`, not
`(Primitive, error)`, so rejecting a nil sampler would mean panicking on a caller
mistake. Had they returned errors, rejecting would have been the better call.
Note the layer above does the opposite — the MCP server *refuses* a bad blast
radius rather than defaulting it. A library trusts a programmer; a server facing
an agent doesn't.

### Why does chain order change the failure mode?

Chain composes outside-in: the first primitive is outermost, so it runs first.
`Chain(Latency(300ms), Error)` waits 300ms then fails — a slow failure.
`Chain(Error, Latency(300ms))` fails immediately and never reaches the delay —
a fast failure. Same parts, opposite order, completely different failure. Order
is behavior, not cosmetics.

The mechanism is that primitives split in two. `Error`, `Blackhole` and `Timeout`
*replace* the call: they return without ever invoking the inner function.
`Latency` and `CPUBurn` *perturb* it and then call inner. So a replacing
primitive placed outermost makes everything inside it dead code.

Why the distinction matters: a slow failure consumes the caller's timeout budget
*and* fails, which is how one sick dependency cascades — callers hold threads and
connections for the full delay before getting an answer. A fast failure is
benign; the caller learns immediately and can fall back. Converting the first
into the second is precisely what a circuit breaker is for. So
`Chain(Latency, Error)` is the fault a breaker exists to protect you from, and
`Chain(Error, Latency)` is what the world looks like once it has done its job.

### Why do chained blast radii compound?

Each primitive samples independently. Two primitives at `Rate(0.5)` don't give
50% faulted — they give `1 - (0.5 × 0.5)` = 75%, because a call is faulted if
*either* primitive fires. If you want exactly 50% through a chain, set the rate
on the outer primitive and leave the inner at 1.0.

### Why does the nearer deadline always win?

A fault that ignores context cancellation is a bug, not a fault. The caller's
timeout budget has to mean something, so every primitive watches the context and
returns `ctx.Err()` the moment it's done — even mid-sleep. An injected 10-second
hang under a 50ms deadline returns in 50ms. An injected hang can never become an
unbounded one.

### Why is Blackhole a 503 and a bare error a 500?

The status comes from the fault, not from a guess in the middleware. A blackholed
dependency means "no healthy upstream" — that's 503 Service Unavailable. A bare
injected error is an unspecified failure — 500. If you need a specific status
(say 502 for a bad gateway), `ErrorStatus` lets the fault declare it. The
middleware never invents semantics.

### Why does the counter measure faults caused, not faults considered?

A call the sampler declined was never faulted, so counting it would lie about
what the experiment did. `fault_injected_total` increments only when a fault
actually fires. `Rate(0)` moves nothing — that's the counter telling the truth.

### Why is a capacity fault a different shape from the other primitives?

Latency, error, timeout, and blackhole wrap individual calls. Removing capacity
perturbs no single call — it changes the *rate* at which all calls are served.
So the capacity fault's `Apply` takes no call at all; it resizes the pool and
returns a rollback, matching the `exp.Fault` signature exactly. Different physics,
different shape — which is also why an experiment consumes it with no adapter.

### Why must Apply/restore be serialized?

Without a lock, two overlapping Apply calls race: the second captures the first
fault's reduced size as "previous," and its restore then *reinstates* the fault
instead of undoing it. Serialization plus idempotent restore (restoring twice is
a no-op) keeps overlapping use safe.

## Load generation (k6)

### What requests does k6 actually send?

Two scripts, two jobs.

`loadtest.js` ramps GETs at `/work` — 1 → 20 → 100 → 300 virtual users, then back
to zero — and counts outcomes: 200 served, 429 rate-limited, 503 shed, other 5xx
backend flakes. Each GET walks the whole gateway: shedder, rate limiter, fault
middleware, simulated backend. The ramp deliberately pushes past the ceiling —
the goal is to find where rejection starts, not to avoid it.

`sync-stall.js` is a chaos experiment, not a flood. It POSTs batches to `/sync`
("here's work"), GETs `/sync/status` ("how deep is the queue?"), and POSTs
`/scale?replicas=N` to shrink and restore the worker pool mid-run — the fault and
the rollback, as HTTP calls. GET asks a question; POST hands something over, and
the second script uses that distinction to break the system, watch, fix it, and
verify recovery.

### What makes sync-stall an experiment rather than a load test?

The thresholds. They encode the hypothesis instead of a performance target:

    sync_dropped_total{phase:baseline}   count==0
    sync_dropped_total{phase:fault}      count>0    <- the fault must bite
    sync_dropped_total{phase:recovery}   count==0

A load test asserts the system stays fast. This asserts drops happen *only*
during the fault window — so it fails if the fault didn't land, not just if the
system broke. An experiment where the fault silently never fired is a false pass,
and `count>0` is what catches it.

Two details keep it honest. The 2-second `grace` phase after rollback is tagged
but deliberately *not* asserted: in-flight requests are still settling, so
demanding zero drops there would be flaky rather than meaningful. And 429 is
registered as an expected status rather than a transport failure — the system
shedding loudly is the behavior under test, so counting it against
`http_req_failed` would punish the gateway for working correctly.

`/sync/status` is sampled for visibility, not for pass/fail. It feeds the queue-
depth gauge and the wait trend; the drop counters are what the thresholds judge.

### Do the POSTs write anything? Does the GET read from the codebase?

No to both, and that's deliberate.

Nothing touches disk. POST `/sync` drops a job into an in-memory channel, a worker
goroutine simulates processing for a configured duration and discards it, and GET
`/sync/status` reads live counters from the same process. No database, no file
writes anywhere in the pool or the fault library. Stop the process and it all
vanishes.

The thing under test was never data — it's behavior over time. The requests are
the workload; the responses are the measurement. Like a wind tunnel: the air
isn't stored anywhere, but it shows exactly how the model behaves. And with no
persistent state nothing leaks between runs, so every experiment starts clean and
is repeatable.

## MCP server

### Why does the server refuse instead of clamping?

If you ask for blast radius 1.0 and the server silently clamps to 0.5, you walk
away believing you tested 100% of traffic when you tested half — the server lied
to you. Refusing forces the caller to ask explicitly for something within policy.
A quiet clamp corrupts the experiment's meaning; a loud refusal preserves it.

### Why only one experiment at a time?

Two concurrent faults make attribution impossible: if the steady state breaks,
which fault did it? Serial experiments keep every verdict attributable to exactly
one fault.

### Why does get_verdict return "running" instead of blocking until done?

Blocking would hang the agent on a long experiment with no way to do anything
else — including aborting it. Polling keeps the agent in control: it can check
status, enforce its own timeout, or walk away. The server never holds a caller
hostage.

### Why are policy caps enforced by the server, not trusted to the caller?

The model calling the tools is the least trustworthy part of the loop — it can
misread, hallucinate, or be prompt-injected. Guardrails that live in the caller
are suggestions; guardrails that live in the server are guarantees. The server
tells the model its limits up front and enforces them regardless of what's asked.

### What's the difference between an unknown tool and a refused experiment?

An unknown tool (`no_such_tool`, code -32601) is a protocol error — the caller
asked for something that doesn't exist, which means the caller is confused. A
refused experiment is a *result* — the tool exists, the request was understood,
and the server deliberately said no, with a reason. Confusion vs. policy: the
agent should fix its approach in the first case and adjust its request in the
second.

## Verdicts

### The verdict says Held=false. Is that bad?

Usually that's the point. `Held=false` means the fault broke the steady-state
hypothesis — which is what you ran the experiment to find out. The one that
should worry you is `Recovered=false`: the fault was rolled back and the system
*didn't* come back. A system that breaks under fault is normal; a system that
doesn't recover is broken.

### Why does the verdict record the blast radius?

So the verdict is evidence, not anecdote. "Latency broke the hypothesis at 40%
blast radius, and the system recovered" is a claim someone else can reproduce and
a reviewer can trust. Without the recorded radius, you don't know what was
actually tested.
