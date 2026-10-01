// Package fanout models a Greenplum-style MPP query: scatter work
// across shards, gather the results. Two fault primitives — a
// straggler shard and skewed key distribution (a hot partition) —
// demonstrate the central MPP lesson: total latency is the max of
// the shards, so one bad shard dominates the whole query. That is
// why bulkheads and hedged requests exist.
package fanout

import (
	"sync"
	"time"
)

// ShardResult is what one shard did for a query.
type ShardResult struct {
	Shard     int
	Keys      int           // keys routed to this shard
	Latency   time.Duration // simulated shard latency
	Straggler bool          // this shard had the straggler fault injected
}

// perKey is the simulated processing time for one key on a shard.
// Simulated, like the /work backend: the shape is what matters.
const perKey = 200 * time.Microsecond

// Query scatters keys across shards and gathers per-shard results.
// straggler is the index of a shard that takes an extra
// stragglerDelay (-1 disables). skew is the fraction of keys routed
// to shard 0 instead of uniformly (0 disables, clamped to [0,1]): a
// hot partition.
func Query(shards, keys, straggler int, stragglerDelay time.Duration, skew float64) []ShardResult {
	if shards < 1 {
		shards = 1
	}
	if keys < 0 {
		keys = 0
	}
	if skew < 0 {
		skew = 0
	}
	if skew > 1 {
		skew = 1
	}

	// Route keys: skew fraction to shard 0, the rest uniform.
	counts := make([]int, shards)
	hot := int(float64(keys) * skew)
	counts[0] += hot
	rest := keys - hot
	for i := 0; i < rest; i++ {
		counts[i%shards]++
	}

	out := make([]ShardResult, shards)
	var wg sync.WaitGroup
	for s := 0; s < shards; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			lat := time.Duration(counts[s]) * perKey
			isStraggler := s == straggler
			if isStraggler {
				lat += stragglerDelay
			}
			out[s] = ShardResult{Shard: s, Keys: counts[s], Latency: lat, Straggler: isStraggler}
		}(s)
	}
	wg.Wait()
	return out
}

// MaxLatency returns the query's effective latency: the slowest
// shard. This is the number the caller actually waits for.
func MaxLatency(results []ShardResult) time.Duration {
	var slowest time.Duration
	for _, r := range results {
		if r.Latency > slowest {
			slowest = r.Latency
		}
	}
	return slowest
}
