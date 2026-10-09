package limiter

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests run the Lua token bucket in a real Redis. The mock-server
// test checks the wire protocol; only these check that the script
// itself is atomic and refills correctly. They are skipped unless
// REDIS_ADDR is set, the same opt-in pattern as FLOWGATE_MCP_BIN:
//
//	docker run --rm -p 6379:6379 redis:7-alpine
//	REDIS_ADDR=localhost:6379 go test ./internal/limiter -race -run Integration -v

func redisAddrOrSkip(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set: skipping real-Redis integration test")
	}
	return addr
}

// uniqueKey keeps one test's bucket from being another run's leftover:
// keys outlive the test (they expire on a TTL), so a fixed key would
// make the second run start with a half-spent bucket.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func newIntegrationLimiter(t *testing.T, capacity, rate float64) *RedisLimiter {
	t.Helper()
	l, err := NewRedisLimiter(redisAddrOrSkip(t), capacity, rate)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestRedisIntegration_BurstThenDeny(t *testing.T) {
	l := newIntegrationLimiter(t, 5, 1)
	key := uniqueKey(t)

	for i := 0; i < 5; i++ {
		if ok, _ := l.Allow(key); !ok {
			t.Fatalf("request %d of the 5-token burst was denied", i+1)
		}
	}
	ok, wait := l.Allow(key)
	if ok {
		t.Fatal("6th request should be denied once the burst is spent")
	}
	// One token at 1 token/sec is ~1s away, a little less since some
	// time has passed since the last refill.
	if wait <= 0 || wait > time.Second {
		t.Fatalf("wait = %v, want within (0, 1s]", wait)
	}
	if got := l.FailOpenCount(); got != 0 {
		t.Fatalf("FailOpenCount = %d against a live Redis, want 0", got)
	}
}

func TestRedisIntegration_RefillsOverTime(t *testing.T) {
	l := newIntegrationLimiter(t, 2, 10) // 10 tokens/sec, burst 2
	key := uniqueKey(t)

	l.Allow(key)
	l.Allow(key)
	if ok, _ := l.Allow(key); ok {
		t.Fatal("bucket should be empty after draining the burst")
	}
	time.Sleep(250 * time.Millisecond) // ~2.5 tokens, clamped to capacity 2
	if ok, _ := l.Allow(key); !ok {
		t.Fatal("bucket should have refilled after 250ms at 10 tokens/sec")
	}
}

func TestRedisIntegration_KeysAreIsolated(t *testing.T) {
	l := newIntegrationLimiter(t, 1, 0.001)
	a, b := uniqueKey(t)+"-a", uniqueKey(t)+"-b"

	if ok, _ := l.Allow(a); !ok {
		t.Fatal("client A first request denied")
	}
	if ok, _ := l.Allow(a); ok {
		t.Fatal("client A second request should be denied")
	}
	if ok, _ := l.Allow(b); !ok {
		t.Fatal("client B must not inherit client A's spent bucket")
	}
}

// The property the whole shared limiter exists for: several processes
// behind a load balancer enforce ONE limit, not one each. Two separate
// limiter instances stand in for two replicas; hammering both from many
// goroutines must admit exactly the burst, no more. Refill is set so
// slow (0.01/s) that no second token appears during the test, which
// makes "exactly capacity" a sharp assertion about atomicity rather
// than a timing guess.
func TestRedisIntegration_OneLimitAcrossInstances(t *testing.T) {
	const capacity = 10
	replicaA := newIntegrationLimiter(t, capacity, 0.01)
	replicaB := newIntegrationLimiter(t, capacity, 0.01)
	key := uniqueKey(t)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for _, replica := range []*RedisLimiter{replicaA, replicaB} {
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(l *RedisLimiter) {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					if ok, _ := l.Allow(key); ok {
						allowed.Add(1)
					}
				}
			}(replica)
		}
	}
	wg.Wait()

	if got := allowed.Load(); got != capacity {
		t.Fatalf("2 replicas x 8 goroutines x 20 requests admitted %d, want exactly %d (a larger number means the limit is per replica, not shared)", got, capacity)
	}
}
