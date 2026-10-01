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
  manifests run real pods.
- The backend latency is simulated; the queue dynamics are real.
- `/scale` only affects the flowgate process you hit — there is
  no shared control plane across pods. In-cluster, the k6 Job's
  `/scale` call lands on one pod behind the Service.
- The experiment verdict's timing depends on machine speed;
  thresholds (queue < 50, 8s window, 5s recovery) are tuned for a
  laptop.
- `/scale` and `/experiments/*` are unauthenticated: fine for a
  lab, not something to expose on the public Fly deployment.
