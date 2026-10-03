// Command flowgate runs a small reliability gateway: rate limiting,
// load shedding and circuit breaking in front of a backend handler,
// with Prometheus metrics exposed for observability.
//
// The resilience-experiment endpoints (/sync, /scale, /query,
// /experiments/...) model production-shaped topologies: an
// in-process worker pool for directory sync (the Cloud Identity
// Engine capacity story), an MPP fan-out query (the Greenplum
// story), and a fault-injection experiment runner with
// steady-state hypotheses, blast-radius limits, and rollback.
//
// Faults themselves come from internal/fault, so an experiment's
// Inject closure is a primitive being switched on rather than a
// bespoke function written for that one experiment.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/exp"
	"github.com/prime5/flowgate/internal/fanout"
	"github.com/prime5/flowgate/internal/fault"
	"github.com/prime5/flowgate/internal/limiter"
	"github.com/prime5/flowgate/internal/metrics"
	"github.com/prime5/flowgate/internal/pool"
	"github.com/prime5/flowgate/internal/ratelimit"
	"github.com/prime5/flowgate/internal/shedder"
)

func main() {
	port := envOr("PORT", "8080")

	cfg := ratelimit.Config{
		Limiter: limiter.NewRegistry(envFloat("RL_BURST", 20), envFloat("RL_RATE", 5)), // 20 burst, 5 req/s sustained per client
		Breaker: breaker.New(envInt("BREAKER_THRESHOLD", 5), 10*time.Second),
		Shedder: shedder.New(envInt("MAX_IN_FLIGHT", 50)),
	}

	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulated backend work: a little latency, and an occasional
		// failure so the breaker and metrics have something real to
		// react to during load testing.
		time.Sleep(time.Duration(5+rand.Intn(15)) * time.Millisecond)
		if rand.Float64() < 0.02 {
			http.Error(w, "simulated backend error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	// Directory-sync pool: in-process worker goroutines, not pods.
	// Under-provision it against the arrival rate and the backlog
	// grows until batches miss their window — the dynamic behind the
	// real incident, made reproducible. deploy/k8s runs the same
	// shape against actual pods.
	syncPool := pool.New(envDur("SYNC_WORK_MS", 25), envInt("SYNC_QUEUE_CAP", 1000))
	syncPool.SetSize(envInt("SYNC_REPLICAS", 2))
	metrics.SyncReplicas.Set(float64(syncPool.Size()))

	mux := http.NewServeMux()
	// backendFault is a fault primitive sitting between the gateway's
	// defenses and the backend, switched off until an experiment turns
	// it on. Placing it inside ratelimit.Wrap rather than outside is
	// deliberate: the shedder, limiter and breaker see the request
	// first, so an injected delay is the *dependency* being slow,
	// which is exactly the condition the breaker exists to survive.
	backendFault := fault.NewToggle(
		fault.Latency(envDur("FAULT_LATENCY_MS", 250), fault.Always()),
	)
	mux.Handle("/work", ratelimit.Wrap(cfg, fault.Middleware(backendFault)(backend)))
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.SyncQueueDepth.Set(float64(syncPool.Stats().Queued))
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if err := metrics.WriteTo(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	// POST /sync — submit one directory-sync batch. 202 when
	// accepted, 429 when the queue is full: the stall, made visible.
	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !syncPool.Submit() {
			metrics.RequestsTotal.Inc("sync_dropped")
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"accepted": false, "reason": "sync queue full; batch missed its window",
			})
			return
		}
		metrics.RequestsTotal.Inc("sync_accepted")
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
	})

	// GET /sync/status — the steady-state readout experiments probe.
	mux.HandleFunc("/sync/status", func(w http.ResponseWriter, r *http.Request) {
		s := syncPool.Stats()
		writeJSON(w, http.StatusOK, map[string]any{
			"replicas": s.Replicas, "queued": s.Queued,
			"processed": s.Processed, "dropped": s.Dropped,
			"avg_wait_ms": s.AvgWaitMs,
		})
	})

	// POST /scale?replicas=N — resize this process's sync worker pool
	// (goroutines, not pods), with a blast-radius guard: never below
	// 1, never above 32 on this box.
	mux.HandleFunc("/scale", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		n, err := strconv.Atoi(r.URL.Query().Get("replicas"))
		if err != nil || n < 1 || n > 32 {
			http.Error(w, "replicas must be an integer in [1,32]", http.StatusBadRequest)
			return
		}
		prev := syncPool.SetSize(n)
		metrics.SyncReplicas.Set(float64(n))
		log.Printf("scale: replicas %d -> %d", prev, n)
		writeJSON(w, http.StatusOK, map[string]any{"previous": prev, "replicas": n})
	})

	// GET /query — MPP fan-out with fault primitives as query
	// params: ?shards=8&keys=1000&straggler=2&straggler_ms=500&skew=0.9
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		shards, keys := queryInt(q, "shards", 8), queryInt(q, "keys", 1000)
		// Bound the scatter so one request can't spawn millions of
		// goroutines: the simulator's own bulkhead.
		if shards < 1 || shards > 64 || keys < 0 || keys > 100000 {
			http.Error(w, "shards must be in [1,64] and keys in [0,100000]", http.StatusBadRequest)
			return
		}
		res := fanout.Query(
			shards,
			keys,
			queryInt(q, "straggler", -1),
			time.Duration(queryInt(q, "straggler_ms", 0))*time.Millisecond,
			queryFloat(q, "skew", 0),
		)
		// The caller waits for the slowest shard; cap the real
		// sleep so a demo can't park the handler.
		wait := fanout.MaxLatency(res)
		if wait > 250*time.Millisecond {
			wait = 250 * time.Millisecond
		}
		time.Sleep(wait)
		out := make([]map[string]any, len(res))
		for i, s := range res {
			out[i] = map[string]any{
				"shard": s.Shard, "keys": s.Keys,
				"latency_ms": float64(s.Latency) / 1e6,
				"straggler":  s.Straggler,
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"shards": out, "max_latency_ms": float64(fanout.MaxLatency(res)) / 1e6,
		})
	})

	// POST /experiments/pod-kill — seed load, remove two-thirds of the
	// sync workers mid-run (blast radius 0.67), hold, roll back,
	// verify recovery. Returns the verdict as JSON. One experiment at
	// a time: two overlapping runs would fight over the same pool and
	// both verdicts would be noise.
	//
	// The fault is fault.Capacity — a resource-level fault rather than
	// a call-level Primitive, because removing capacity perturbs no
	// individual call; it changes the rate at which all of them are
	// served. Apply already returns its own restore function, so the
	// experiment's Inject closure is the fault itself.
	var experimentMu sync.Mutex
	mux.HandleFunc("/experiments/pod-kill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !experimentMu.TryLock() {
			http.Error(w, "an experiment is already running", http.StatusConflict)
			return
		}
		defer experimentMu.Unlock()

		const full = 6
		original := syncPool.SetSize(full)
		metrics.SyncReplicas.Set(float64(full))

		// Seed load: 300 batches at 200/s, ~1.5s of arrivals.
		go func() {
			for i := 0; i < 300; i++ {
				syncPool.Submit()
				time.Sleep(5 * time.Millisecond)
			}
		}()

		// The capacity fault: drop to 2 workers, restore on rollback.
		// setReplicas is the resizer the fault drives — it applies the
		// new size, keeps the metric honest, and returns the previous
		// size, which is the contract fault.Capacity expects.
		setReplicas := func(n int) int {
			prev := syncPool.SetSize(n)
			metrics.SyncReplicas.Set(float64(syncPool.Size()))
			log.Printf("experiment: sync workers %d -> %d", prev, n)
			return prev
		}
		capacityFault := fault.Capacity("capacity", setReplicas, 2)

		v := exp.Run(exp.Experiment{
			Name:        "pod-kill: lose two-thirds of sync replicas under load",
			SteadyState: func() bool { return syncPool.Stats().Queued < 50 },
			// Apply has the exp.Fault signature already: inject, and
			// hand back the function that undoes it.
			Inject:          capacityFault.Apply,
			BlastRadius:     float64(full-2) / float64(full),
			Duration:        8 * time.Second,
			RecoveryTimeout: 5 * time.Second,
		}, 200*time.Millisecond)

		syncPool.SetSize(original)
		metrics.SyncReplicas.Set(float64(original))
		writeJSON(w, http.StatusOK, v)
	})

	// POST /experiments/backend-latency — the primitive-driven
	// experiment. The fault is internal/fault's Latency primitive,
	// already wired in front of the backend and switched off; the
	// experiment's whole Inject closure is Toggle.On, whose return
	// value is the rollback. Nothing bespoke is written per
	// experiment, which is the point of a primitives library.
	//
	// Steady state: a representative backend call completes inside the
	// 100ms budget. The injected delay is larger than the budget, so
	// the hypothesis should be violated while the fault is on and hold
	// again once it is rolled back.
	mux.HandleFunc("/experiments/backend-latency", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !experimentMu.TryLock() {
			http.Error(w, "an experiment is already running", http.StatusConflict)
			return
		}
		defer experimentMu.Unlock()

		const budget = 100 * time.Millisecond
		probe := func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*budget)
			defer cancel()
			start := time.Now()
			err := backendFault.Inject(ctx, func(context.Context) error { return nil })
			return err == nil && time.Since(start) < budget
		}

		v := exp.Run(exp.Experiment{
			Name:        "backend-latency: slow dependency behind the gateway",
			SteadyState: probe,
			// The Toggle's On already has the Fault signature:
			// flip on, hand back the function that flips it off.
			Inject: backendFault.On,
			// Always() samples every call, so the fault reaches all
			// traffic through this primitive.
			BlastRadius:     1.0,
			Duration:        3 * time.Second,
			RecoveryTimeout: 2 * time.Second,
		}, 200*time.Millisecond)

		writeJSON(w, http.StatusOK, v)
	})

	log.Printf("flowgate listening on :%s (max_in_flight=%d, sync_replicas=%d)", port, cfg.Shedder.Capacity(), syncPool.Size())
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func queryInt(q url.Values, key string, def int) int {
	if vs, ok := q[key]; ok && len(vs) > 0 {
		if n, err := strconv.Atoi(vs[0]); err == nil {
			return n
		}
	}
	return def
}

func queryFloat(q url.Values, key string, def float64) float64 {
	if vs, ok := q[key]; ok && len(vs) > 0 {
		if f, err := strconv.ParseFloat(vs[0], 64); err == nil {
			return f
		}
	}
	return def
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func envDur(key string, defMs int) time.Duration {
	return time.Duration(envInt(key, defMs)) * time.Millisecond
}
