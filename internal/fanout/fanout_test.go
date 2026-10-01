package fanout

import (
	"testing"
	"time"
)

func TestUniformScatterIsBalanced(t *testing.T) {
	res := Query(4, 100, -1, 0, 0)
	total := 0
	for _, r := range res {
		total += r.Keys
		if r.Keys != 25 {
			t.Fatalf("shard %d got %d keys, want 25", r.Shard, r.Keys)
		}
	}
	if total != 100 {
		t.Fatalf("total keys = %d, want 100", total)
	}
}

func TestStragglerDominatesQuery(t *testing.T) {
	// No fault: shards are balanced, max latency is small.
	plain := MaxLatency(Query(4, 100, -1, 0, 0))
	// One straggler: the whole query waits for it.
	withStraggler := MaxLatency(Query(4, 100, 2, 500*time.Millisecond, 0))
	if withStraggler <= plain {
		t.Fatalf("straggler max = %v, plain max = %v; straggler should dominate", withStraggler, plain)
	}
	res := Query(4, 100, 2, 500*time.Millisecond, 0)
	if !res[2].Straggler {
		t.Fatalf("shard 2 should be flagged as the straggler")
	}
}

func TestHotPartitionDominatesQuery(t *testing.T) {
	plain := MaxLatency(Query(4, 1000, -1, 0, 0))
	skewed := MaxLatency(Query(4, 1000, -1, 0, 0.9))
	if skewed <= plain {
		t.Fatalf("skewed max = %v, plain max = %v; hot partition should dominate", skewed, plain)
	}
	res := Query(4, 1000, -1, 0, 0.9)
	if res[0].Keys < 900 {
		t.Fatalf("shard 0 got %d keys, want >= 900 with skew 0.9", res[0].Keys)
	}
}

func TestEdgeCases(t *testing.T) {
	if got := len(Query(0, 10, -1, 0, 0)); got != 1 {
		t.Fatalf("0 shards -> 1 shard, got %d results", got)
	}
	if got := MaxLatency(Query(4, 0, -1, 0, 0)); got != 0 {
		t.Fatalf("0 keys -> 0 latency, got %v", got)
	}
	// Out-of-range skew is clamped, never a negative key count.
	for _, r := range Query(4, 100, -1, 0, -0.5) {
		if r.Keys != 25 {
			t.Fatalf("skew -0.5: shard %d got %d keys, want 25", r.Shard, r.Keys)
		}
	}
	if got := Query(4, 100, -1, 0, 2)[0].Keys; got != 100 {
		t.Fatalf("skew 2: shard 0 got %d keys, want 100", got)
	}
}
