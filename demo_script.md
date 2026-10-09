## What is `flowgate`?

[flowgate](file:///Users/pramathesh5/code/goLearn/flowgate/README.md) is a **reliability gateway, fault-injection engine, and MCP chaos lab** written in Go. The gateway path, the MCP server and the Redis client are hand-rolled; OpenTelemetry (tracing only) is the one third-party dependency. 

It serves two complementary purposes:
1. **A Resilient Front Door:** It protects backend services from being overwhelmed by combining three distinct defense patterns: **load shedding**, **token-bucket rate limiting**, and a **three-state circuit breaker**.
2. **A Controlled Chaos Lab:** It provides a fault-injection engine modeled after real production outages (such as directory synchronization stalls and MPP distributed query stragglers), exposed both as HTTP lab endpoints and as **Model Context Protocol (MCP)** tools for autonomous AI agents.

---

## Architecture & Core Components

```
                          Incoming Request
                                 │
                   ┌─────────────▼─────────────┐
                   │  1. Load Shedder (503)   │  Rejects immediately if >50 in-flight
                   └─────────────┬─────────────┘
                                 │
                   ┌─────────────▼─────────────┐
                   │  2. Rate Limiter (429)   │  Token bucket (5 req/s, 20 burst per key)
                   └─────────────┬─────────────┘
                                 │
                   ┌─────────────▼─────────────┐
                   │ 3. Circuit Breaker (503) │  Closed ⇄ Open ⇄ Half-Open
                   └─────────────┬─────────────┘
                                 │
                   ┌─────────────▼─────────────┐
                   │  4. Fault Injection (Lab) │  Latency, Error, Timeout, Blackhole
                   └─────────────┬─────────────┘
                                 │
                   ┌─────────────▼─────────────┐
                   │     Backend Service       │  Target workload (simulated ~2% error)
                   └───────────────────────────┘
```

### 1. The Gateway Defenses ([`internal/ratelimit`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/ratelimit/middleware.go))
The middleware chain enforces an intentional, defensive execution order:
* **Load Shedder ([`internal/shedder`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/shedder/shedder.go)):** Enforces bounded concurrency (e.g., max 50 in-flight requests). It rejects immediately with `503 Service Unavailable` rather than queueing. *Why?* Queueing converts server saturation into invisible latency spirals; shedding immediately protects the gateway process itself from crashing.
* **Rate Limiter ([`internal/limiter`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/limiter/bucket.go)):** Concurrency-safe token-bucket limiter per client key (refilling at 5 req/s with burst capacity of 20).
* **Circuit Breaker ([`internal/breaker`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/breaker/breaker.go)):** 3-state state machine (Closed, Open, Half-Open). Trips after 5 consecutive backend failures with a 10s cooldown to let downstream services recover.
* **Why this order?**
  $$\text{Shedder (cheapest CPU cost)} \longrightarrow \text{Limiter (per-client fairness)} \longrightarrow \text{Breaker (downstream health)}$$

### 2. Fault Primitives & Chaos Engine ([`internal/fault`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/fault/fault.go))
Rather than writing ad-hoc `time.Sleep()` or random `500` hacks in handlers, faults are modeled as formal, composable primitives with **blast radius sampling** (`Rate(0.1)` = 10% of calls):
* **Call-Level Primitives:**
  * `Latency`: Delays requests while strictly respecting `context.Context` cancellation (a fault that ignores deadlines is a bug).
  * `Error`: Injects explicit HTTP error codes.
  * `Timeout`: Holds until the caller's deadline expires, returning `504`.
  * `CPUBurn`: Exercises CPU spin within a bounded goroutine.
  * `Blackhole`: Drops requests immediately (simulating network partition or DNS failure, returning `503`).
* **Resource-Level Faults (`Capacity`):**
  * Changes the environment itself (e.g., shrinking worker replicas from 6 to 2). Unlike call-level primitives, capacity loss perturbs no individual call directly—it degrades system throughput until backlogs explode.
* **Composition:** Faults compose outside-in via `Chain`. `Chain(Latency, Error)` creates a **slow failure** (the worst kind, which exhausts caller connection pools), whereas `Chain(Error, Latency)` creates a **fast failure** (circuit breakers convert the former into the latter).

### 3. Real-World Topologies Simulated
* **Directory Sync Worker Pool ([`internal/pool`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/pool/pool.go)):**
  Models a Cloud Identity Engine incident: a bounded queue processed by dynamic worker goroutines. Under-provisioning capacity causes queue buildup until `POST /sync` returns `429` ("batch missed its window"). Scaling capacity via `POST /scale` or Kubernetes HPA drains the backlog.
* **MPP Scatter-Gather Query ([`internal/fanout`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/fanout/fanout.go)):**
  Models Greenplum-style distributed analytics. The query latency across $N$ shards is:
  $$\text{Latency} = \max(\text{Shard}_1, \dots, \text{Shard}_N)$$
  A single straggler shard or skewed partition dominates total response time, illustrating why bulkheads and hedged requests are necessary.

### 4. Experiment Discipline & Verdicts ([`internal/exp`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/exp/experiment.go))
Every experiment follows four mandatory pillars:
1. **Steady-state hypothesis** (e.g., sync queue depth < 50).
2. **Single fault** with declared blast radius.
3. **Mandatory rollback** (runs even if probes panic).
4. **Bounded recovery window** producing a structured JSON verdict:
   * `held: false` is normal (the fault broke the steady state as expected).
   * `recovered: true` is what matters (the system restored itself after rollback).

### 5. Model Context Protocol (MCP) Server ([`internal/mcp`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/mcp/server.go), [`cmd/flowgate-mcp`](file:///Users/pramathesh5/code/goLearn/flowgate/cmd/flowgate-mcp/main.go))
Exposes the chaos lab over stdio JSON-RPC 2.0 to AI agents (`run_experiment`, `get_verdict`, `list_experiments`):
* **Server-Side Guardrails:** Hard limits on blast radius and duration are enforced in Go code, **not** via LLM prompts (prompts can be bypassed or jailbroken).
* **Loud Refusal vs. Quiet Clamping:** If an agent requests a blast radius of 1.0 against a 0.5 cap, the server explicitly refuses rather than silently clamping. A test verdict claiming 1.0 that actually ran at 0.5 would be invalid test evidence.
* **Asynchronous Tool Design:** Experiments take seconds. Instead of blocking the agent loop, `run_experiment` returns immediately and the agent polls `get_verdict`.

### 6. Observability (hand-rolled Prometheus)
* **Prometheus Exposition ([`internal/metrics`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/metrics/metrics.go)):** The `/metrics` endpoint is completely hand-rolled without pulling in the Prometheus Go client library.
* **Distributed Tracing ([`internal/telemetry`](file:///Users/pramathesh5/code/goLearn/flowgate/internal/telemetry/telemetry.go)):** Supports W3C `traceparent` propagation and Jaeger OTLP export, emitting `flowgate.request` with nested spans for each stage reached (`shed` $\rightarrow$ `ratelimit` $\rightarrow$ `breaker` $\rightarrow$ `backend`).

---

## How to Demo This to a Friend (The 5-Act Playbook)

This demo flow is structured like an interactive story, taking 10 to 12 minutes.

```
Act 0: The Pitch (1 min)
  └─ The smart front door & chaos lab story

Act 1: The Gateway Defenses (2 mins)
  ├─ Launch server: FLOWGATE_LAB=1 PORT=8080 ./flowgate
  ├─ curl /work & curl /metrics (hand-rolled Prometheus)
  └─ Explain the defense order: Shed -> Limit -> Break

Act 2: The Fault Primitives Live Show (2 mins)
  └─ Run: go run ./cmd/faultdemo
     └─ Live demonstration of blast radius, context cancellation & chain ordering

Act 3: Real Outages Re-enacted (3 mins)
  ├─ MPP Straggler: curl /query (watch 1 slow shard dominate the max latency)
  └─ Chaos Experiment: POST /experiments/pod-kill (watch steady state break & auto-recover)

Act 4: The AI / MCP Integration (2 mins)
  └─ Run: python3 scripts/mcp-session.py
     └─ Show server-side guardrails refusing invalid agent calls + async verdicts

Act 5: The Postmortem Mic Drop (1 min)
  └─ Discuss POSTMORTEM.md: 303k k6 requests on Fly.io and the RemoteAddr proxy gotcha
```

---

### Act 0: The 30-Second Hook
> *"Most tutorials show simple web servers that fall over the second traffic spikes or a database lags. I built `flowgate`: it's a reliability gateway and chaos engineering playground built in Go without external frameworks. Think of it as a smart bouncer at the door that sheds load, rate limits, and isolates sick backends—plus a built-in lab where we can simulate real outages and let AI agents test system resilience."*

---

### Act 1: The Gateway in Action
Open Terminal 1 and run the gateway:
```bash
go build -o flowgate ./cmd/flowgate
FLOWGATE_LAB=1 PORT=8080 ./flowgate
```
In Terminal 2, show normal traffic and hand-rolled Prometheus observability:
```bash
# 1. Normal traffic through the defense chain
curl -i http://localhost:8080/work

# 2. Hand-rolled Prometheus metrics
curl -s http://localhost:8080/metrics | grep flowgate_
```
**Talking Points:**
* Point out that `/work` passes through the shedder, rate limiter, circuit breaker, and backend.
* Highlight that the Prometheus metrics output is written by hand with no Prometheus client library.

---

### Act 2: Fault Primitives Live (`faultdemo`)
Run the narrated fault demo:
```bash
go run ./cmd/faultdemo
```
**Key Highlights to Show Your Friend:**
1. **Section 1 & 2 (Blast Radius):** Notice that `Rate(0)` is a no-op. A forgotten sampler fails safe rather than causing an outage.
2. **Section 3 (Context Cancellation):** Notice the 10-second delay under a 50ms deadline returns in 51ms. Injected faults strictly respect caller timeout budgets.
3. **Section 4 (Order Matters):**
   * `Chain(Latency, Error)` is a **slow failure** (waits 300ms, then fails).
   * `Chain(Error, Latency)` is a **fast failure** (fails immediately, never sleeping).
   * Explain: *"Slow failures exhaust thread pools across your architecture; fast failures let clients recover immediately. This is why circuit breakers exist."*

---

### Act 3: Re-enacting Distributed Outages

#### 1. The MPP Query Straggler
Show how distributed queries suffer from straggler shards:
```bash
# Baseline: 8 shards, fast response (~15ms)
curl -s "http://localhost:8080/query?shards=8&keys=1000"

# Fault: Inject 500ms delay into shard 3
curl -s "http://localhost:8080/query?shards=8&keys=1000&straggler=3&straggler_ms=500"
```
**Talking Point:** *"Total query time is always bounded by the slowest shard. One lagging worker bottlenecks the entire distributed query."*

#### 2. The Chaos Experiment & Automatic Recovery
Trigger a live capacity loss experiment:
```bash
curl -X POST http://localhost:8080/experiments/backend-latency
```
The endpoint executes a steady-state check, injects a 250ms backend latency fault, rolls it back, and returns the verdict:
```json
{
  "name": "backend-latency: slow dependency behind the gateway",
  "held": false,
  "baseline_ok": true,
  "recovered": true,
  "blast_radius": 1,
  "detail": "steady state violated during fault window"
}
```
**Talking Point:** *"Notice `held: false` and `recovered: true`. An experiment failing under fault is normal; failing to recover after rollback is what indicates a broken architecture."*

---

### Act 4: The AI & Model Context Protocol (MCP) Angle
Show how autonomous agents interact with the lab:
```bash
python3 scripts/mcp-session.py
```
**Key Highlights to Show:**
1. **Hard Guardrails:** Point out Section 3 where the agent requests a blast radius of `1.0` (cap is `0.5`) or duration of `120s` (cap is `15s`). The server refuses with clear errors rather than clamping silently.
2. **Async Protocol:** The agent initiates the experiment (`run_experiment`), polls status (`get_verdict`), and receives the structured verdict once completed.
3. **Talking Point:** *"If an AI agent is conducting chaos testing, safety guardrails cannot live in prompt instructions—they must be enforced in the server runtime."*

---

### Act 5: The Postmortem Story
Conclude by sharing the production load testing experience documented in [POSTMORTEM.md](file:///Users/pramathesh5/code/goLearn/flowgate/POSTMORTEM.md):
* The service was deployed to [Fly.io](https://flowgate-pramathesh.fly.dev) and load tested using [loadtest.js](file:///Users/pramathesh5/code/goLearn/flowgate/loadtest.js) with k6 (300 virtual users, 303k requests, ~2,025 req/s sustained).
* **What worked:** The shedder discarded ~11% of overload traffic, and p95 latency remained flat at 102ms.
* **The real bug found:** 0 requests received a `429` rate limit!
  * **Why?** Go's `r.RemoteAddr` saw Fly.io's internal reverse proxy IPs rather than the client IP. Each connection appeared as a distinct client, rendering the per-client token bucket ineffective.
* **Talking Point:** *"Finding this in a real load test proves the difference between code that works in a textbook and code tested against actual cloud network topologies."*