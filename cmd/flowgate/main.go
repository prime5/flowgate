// Command flowgate runs a small reliability gateway: rate limiting,
// load shedding and circuit breaking in front of a backend handler,
// with Prometheus metrics exposed for observability.
package main

import (
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/limiter"
	"github.com/prime5/flowgate/internal/metrics"
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

	mux := http.NewServeMux()
	mux.Handle("/work", ratelimit.Wrap(cfg, backend))
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if err := metrics.WriteTo(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	log.Printf("flowgate listening on :%s (max_in_flight=%d)", port, cfg.Shedder.Capacity())
	log.Fatal(http.ListenAndServe(":"+port, mux))
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
