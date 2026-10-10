# flowgate

A small reliability gateway: token-bucket rate limiting, a circuit breaker, and
load shedding in front of a backend handler, with a hand-rolled Prometheus
`/metrics` endpoint. Built to learn Go by building and running something real,
not just to pass a tutorial. In layman's terms: flowgate is a project I built and actually deployed online. Think of it as a smart front door for a web service. When a flood of requests arrives, it decides who gets in, who waits, and when to stop accepting visitors so the building itself doesn't collapse. Then I added a chaos-testing playground on top: I can deliberately break parts of it — shut down servers mid-rush, slow down one database — and watch exactly how it behaves, with automatic recovery. It's modeled on real incidents I handled, like a sync system at Palo Alto Networks that stalled when it ran out of server capacity.

## What's implemented (code, tested, race-checked)

- `internal/limiter` — token-bucket rate limiter, one bucket per client key,
  concurrency-safe. Unit tests cover burst behavior, refill over time, the
  capacity clamp, and a concurrent-access race test.
- `internal/breaker` — three-state circuit breaker (closed → open → half-open),
  with tests for the trip threshold, cooldown, and both half-open outcomes.
- `internal/shedder` — bounded in-flight concurrency; rejects immediately
  rather than queueing once the bound is hit. Tested under concurrent load.
- `internal/ratelimit` — the middleware that chains shed → rate-limit →
  breaker → backend, in that order, with metrics on every branch.
- `internal/fault` — fault-injection primitives: latency, error, timeout,
  blackhole, CPU burn, capacity removal, composable with `Chain` and
  gated by a blast-radius sampler. Another Go service can import it and
  opt in with one line of middleware.
- `internal/exp` — the experiment framework: steady-state hypothesis,
  one fault, observation, mandatory rollback. Produces a verdict that
  records the blast radius, so a result is reproducible evidence rather
  than an anecdote.
- `internal/pool` — dynamically resizable in-process worker pool: goroutines
  reading a bounded channel, with `SetSize` adding or stopping them at
  runtime. Models the directory-sync stall — under-provision capacity
  against the arrival rate and the backlog grows until batches miss their
  window. In-process, not an orchestrator: the workers are goroutines, not
  pods. `deploy/k8s` runs the same shape against real pods.
- `internal/fanout` — models a Greenplum-style MPP query: scatter across
  shards, gather results, with straggler and key-skew faults. Shows why
  total latency is the max of the shards and one bad shard dominates —
  the reason bulkheads and hedged requests exist.
- `internal/metrics` — Prometheus text-exposition writer, no external
  dependency: `flowgate_requests_total{outcome=...}`, `flowgate_breaker_state`,
  `flowgate_in_flight_requests`.
- `internal/telemetry` — OpenTelemetry tracing, opt-in. The middleware emits
  one `flowgate.request` server span per request, parented to the caller's
  W3C `traceparent` when present, with a child span per stage that ran
  (`shed`, `ratelimit`, `breaker`, `backend`); a request rejected early has
  fewer children, so the shape of a trace shows where it stopped. The trace
  ID comes back in `X-Trace-Id`. With `OTEL_EXPORTER_OTLP_ENDPOINT` unset
  nothing is exported. To view traces:
  `docker compose -f deploy/tracing/docker-compose.yml up -d`, then
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 ./flowgate` and open
  http://localhost:16686.
- `internal/limiter` — per-key token bucket. The default is in-process, so N
  replicas behind a load balancer enforce N separate limits. With
  `REDIS_ADDR` set, the bucket lives in Redis instead: a small hand-rolled
  RESP client runs one Lua script that refills and takes a token atomically,
  using Redis `TIME` so replicas with skewed clocks agree. If Redis is
  unreachable the limiter **fails open** (the gateway keeps serving, the
  limit is not enforced); every such request is counted in
  `flowgate_limiter_fail_open_total` and logged at most once per 10s. See
  DECISIONS.md for why. The Lua script is exercised against a real Redis in
  `redis_integration_test.go`, which runs only when `REDIS_ADDR` is set:
  `docker run --rm -p 6379:6379 redis:7-alpine` then
  `REDIS_ADDR=localhost:6379 go test ./internal/limiter -race -v`.
- Per-client keys: by default the key is the connection's peer address.
  `TRUST_FLY_CLIENT_IP=1` makes it the `Fly-Client-IP` header instead; set it
  only where every request comes through a proxy that sets that header
  (`fly.toml` does; the compose lab does, with k6 supplying the header).
  Left on where clients connect directly, a caller could rotate the header to
  get a fresh bucket per request.
- `internal/runner` — a channel-agnostic message envelope and a multi-turn
  runner that executes MCP tool calls and keeps per-conversation state
  (in-memory store; e.g. `get_verdict` with no `experiment_id` uses the
  experiment the conversation last started). It is a tool-calling loop
  only, not a dialog engine: no language understanding, no intent
  detection. Turns on one conversation are serialised by a per-conversation
  lock, history is capped (`DefaultMaxHistory`, 200 envelopes), and
  different conversations run in parallel.
- Trace propagation through MCP: a client puts a W3C `traceparent` in the
  tool call's `params._meta`; the server extracts it and parents its tool
  span to the caller's trace, so the server-side spans carry the trace ID the
  agent started with.
- `internal/slack` — Slack front end, standard library only. `POST /slack/command`
  verifies Slack's request signature (HMAC-SHA256 over `v0:timestamp:body`,
  5-minute replay window, constant-time compare) before reading anything,
  then serves `/flowgate status`, `/flowgate verdict [id]` and
  `/flowgate run <fault> <blast_radius> <duration_s>` through the same
  `internal/runner` the CLI uses, with one conversation per user per channel.
  A throttled webhook alerter posts when the circuit breaker opens or the
  shared limiter starts failing open. See "Slack" below.
- `internal/mcp` — Model Context Protocol server over JSON-RPC 2.0 on
  stdio, hand-rolled with no third-party dependencies, for the same
  reason `internal/metrics` hand-writes Prometheus exposition. Exposes
  the experiment framework as agent-callable tools with policy caps
  enforced server-side.
- `cmd/flowgate` — the HTTP server. Gateway path: `/work` (simulated
  backend, ~2% error rate, 5-20ms latency) behind the full
  shed → rate-limit → breaker chain, plus `/metrics` and `/healthz`.
  Sync model: `POST /sync` (202 accepted, 429 when the queue is full),
  `GET /sync/status`, `POST /scale?replicas=N`. MPP model: `GET /query`
  with straggler and skew parameters. Experiments:
  `POST /experiments/pod-kill` and `POST /experiments/backend-latency`,
  each one steady-state check, one fault, one rollback. Only `/work`
  goes through the middleware chain; the rest are lab surface and
  bypass the limiter, shedder and breaker. `/scale` and `/experiments/*`
  mutate live state, so they are registered only when `FLOWGATE_LAB=1`
  and return 404 otherwise: `/metrics` is what the SLOs below are
  measured from, and those numbers are only attributable if nothing
  outside the process can move them.
- `cmd/faultdemo` — runs `internal/fault` and narrates each step, so the
  library's behaviour is observable without reading the tests.
- `cmd/flowgate-mcp` — the MCP entry point: exposes the experiment
  framework as three tools (`run_experiment`, `get_verdict`,
  `list_experiments`) over stdio, so an agent can run fault injections
  and read verdicts. The policy limits an agent cannot exceed are set
  here and enforced in `internal/mcp`, not in any prompt.

Run it locally:

```
go build -o flowgate ./cmd/flowgate
PORT=8080 ./flowgate
curl localhost:8080/work
curl localhost:8080/metrics
```

The lab endpoints are off by default. To run the experiments locally —
`experiments/sync-stall.js` POSTs `/scale`, so without the flag the
fault never bites and its `count>0` threshold fails:

```
FLOWGATE_LAB=1 PORT=8080 ./flowgate
```

Run the tests (with the race detector — this project promises concurrency
safety, so the race detector isn't optional):

```
go test ./... -race
```

## Deployed and load-tested

- **Deploy (Fly.io)** — live at `https://flowgate-pramathesh.fly.dev`
  (`fly.toml`, 2 machines, region `sjc`).
- **Load test (k6)** — `loadtest.js` run 2026-09-11 against the live
  deployment: ramp to 300 VUs, 303,761 requests, ~2,025 req/s sustained.
  264,971 succeeded, 33,322 shed (503, ~11%), 5,468 backend errors (~1.8%,
  matches the intentional simulated rate), **0 rate-limited (429)** — see
  `POSTMORTEM.md` for why, and the SLOs below for what this run established.

## SLOs (defined from the 2026-09-11 k6 run, not from theory)

| SLO | Target | Measured | Status |
|---|---|---|---|
| p95 request latency under ~2,000 req/s sustained | ≤ 150ms | 102ms | Met |
| Gateway responds to every request (no timeout/reset) | 100% | 100% | Met |
| Backend success rate for admitted (non-shed) requests | ≥ 95% | 97.98% | Met |
| Load shedding engages before the gateway itself degrades | shed, don't queue, above 50 in-flight | ~11% shed, latency stayed flat | Met |
| Per-client rate limit enforced (5 req/s sustained) | 429s appear for over-limit clients | 0 429s in 303,761 requests | **Not met** — see `POSTMORTEM.md` |

## Resilience experiments (`resilience-experiments` branch)

A fault-injection playground on top of the gateway, shaped by three
real systems from my background. Every experiment has a steady
state, one fault, a blast radius, and a rollback — see
`INTERVIEW.md` for the narration.

- **Directory sync as a resizable worker pool** (`internal/pool`,
  `POST /sync`, `GET /sync/status`, `POST /scale?replicas=N`).
  Workers are pods; scaling is `kubectl scale`. Under-provision it
  and the backlog grows until batches miss their window — the
  Cloud Identity Engine pod-capacity incident, reproducible on
  demand. Scaling back up drains it; `deploy/k8s/hpa.yaml`
  automates the fix.
- **MPP fan-out query** (`internal/fanout`, `GET /query`): scatter
  work across shards, gather results. Fault primitives are a
  straggler shard (`&straggler=3&straggler_ms=500`) and a hot
  partition (`&skew=0.9`) — query latency is the *max* of the
  shards, so one bad shard dominates.
- **Experiment runner** (`internal/exp`, `POST
  /experiments/pod-kill`): steady-state hypothesis, one injected
  fault, declared blast radius, guaranteed rollback, bounded
  recovery window, verdict as JSON. `experiments/sync-stall.js` is
  the 75-second k6 version (baseline → fault → recovery, with
  per-phase drop thresholds); `scripts/ephemeral-env.sh` runs
  everything on a throwaway kind cluster; `scripts/pod-kill.sh`
  deletes real pods with a blast-radius guard.
- New metrics: `flowgate_sync_queue_depth`, `flowgate_sync_replicas`.
- Known limits: pool workers are goroutines, not pods (the k8s
  manifests run real pods); backend latency is simulated, queue
  dynamics are real.

## What's prepared but NOT yet done

- **Fix the rate limiter's client-key derivation** — it currently keys on
  `r.RemoteAddr`, which doesn't reliably identify the real client behind
  Fly.io's proxy layer. `POSTMORTEM.md` has the root cause and the fix
  (key on the `Fly-Client-IP` header, falling back to `RemoteAddr`), plus a
  re-run to confirm 429s show up once it's fixed.

## Design notes

- Order in the middleware is shed → rate-limit → breaker, not the reverse:
  shedding is the cheapest check and protects the gateway process itself
  from being the thing that falls over; rate limiting is per-client and
  should run before spending a breaker check; the breaker is closest to the
  backend because it's reacting to the backend's actual health.
- The shedder rejects immediately instead of queueing. A queue would trade
  visible 503s for invisible latency — worse for the caller, and it hides
  the saturation point this project exists to find.
- `/metrics` has no external dependency on purpose — the Prometheus text
  format is simple enough to hand-write, and doing so means every line on
  that endpoint is something this project actually implements.

## Shared limiter: before and after

Three replicas behind nginx (`deploy/shared-limiter`), hit by `loadtest.js`
with a distinct `Fly-Client-IP` per virtual user.

```
# Before: each replica has its own bucket, so the effective limit is ~3x
REDIS_ADDR= docker compose -f deploy/shared-limiter/docker-compose.yml up --build
# After: one bucket in Redis, shared by all three
docker compose -f deploy/shared-limiter/docker-compose.yml up --build
# Then, for each, send one client key as fast as possible and count admissions:
python3 scripts/limiter-compare.py
```

The script reports admitted requests against the most a single bucket could
admit in the same time (burst plus refill). A ratio near 1 means one shared
bucket; near 3 with three replicas means each replica kept its own.

| Run | Per-client limit | Admitted per client | Result |
| --- | --- | --- | --- |
| In-process (`REDIS_ADDR=`) | burst 20, 5/s | 60 of 300 (59 x 200, 1 x 500) | 3 x the limit: each replica kept its own bucket |
| Redis-backed | burst 20, 5/s | 20 of 300 | One bucket: exactly the burst |

Measured 2026-10-10 with `scripts/limiter-compare.py` on a laptop: one client
key, 300 requests in about 0.1 s, one run each, so refill adds under one
token. The single 500 is the demo backend's deliberate 2% failure, which
happens after the limiter has already admitted the request. This is a local
three-replica check, not a load test and not the Fly deployment.

## Slack

Off unless configured. All three variables are independent.

| Variable | Effect |
| --- | --- |
| `SLACK_SIGNING_SECRET` | Mounts `POST /slack/command` (`status`, `verdict`). |
| `SLACK_ALLOWED_USER_IDS` | Comma-separated Slack member IDs allowed to `run`. |
| `SLACK_WEBHOOK_URL` | Posts alerts: breaker open / recovered, limiter failing open. |

`run` starts fault injection, so it follows the same rule as `/scale` and
`/experiments/*`: it also needs `FLOWGATE_LAB=1`. Without that, or with an
empty allowlist, it answers "disabled". The experiments run in an MCP lab
inside the process that injects faults into itself, not into the live `/work`
path, and the server's blast-radius and duration caps still apply and are
relayed, not bypassed.

Try it without Slack (signs a request the same way Slack does):

```
SLACK_SIGNING_SECRET=local-secret FLOWGATE_LAB=1 SLACK_ALLOWED_USER_IDS=UTEST PORT=8080 ./flowgate
```
```
SLACK_SIGNING_SECRET=local-secret python3 scripts/slack-curl.py status
SLACK_SIGNING_SECRET=local-secret python3 scripts/slack-curl.py "run latency 0.2 5"
SLACK_SIGNING_SECRET=local-secret python3 scripts/slack-curl.py --bad-signature status   # 401
```

Wire up a real workspace (menu names may differ slightly):

1. https://api.slack.com/apps, Create New App, From scratch.
2. Basic Information, App Credentials: copy the Signing Secret.
3. Slash Commands, Create New Command: `/flowgate`, Request URL
   `https://<your-tunnel>/slack/command`. Expose localhost with
   `cloudflared tunnel --url http://localhost:8080` or `ngrok http 8080`.
4. Incoming Webhooks, activate, Add New Webhook to Workspace, pick a channel,
   copy the URL (must be `https://hooks.slack.com/...`).
5. Install the app to the workspace. Your member ID is in your Slack profile,
   under the three-dot menu, Copy member ID.
6. Start flowgate with the three variables set (plus `FLOWGATE_LAB=1` to allow
   `run`).

The endpoint is not behind the per-client rate limiter; it is protected by the
signature check and a 64 KB body cap.

Verified against a real workspace through a Cloudflare quick tunnel:
`/flowgate status`, and `/flowgate run latency 0.2 5` (acknowledged in the
channel, verdict delivered afterwards through Slack's `response_url`). Socket
Mode must be off in the app settings, otherwise Slack never calls the Request URL.

Alerts were also verified against the same workspace (2026-10-09). With
`BREAKER_THRESHOLD=1`, the breaker opening posted `[ALERT] ... OPEN` and
afterwards `[RECOVERED]`. With a local Redis (`REDIS_ADDR`) stopped
mid-run, requests kept returning 200 and one `[ALERT] ... failing open` message
arrived. Alerts for the same condition are throttled, so the count in a message
is the count at the first check; the cumulative total is the metric
`flowgate_limiter_fail_open_total`.
