package limiter

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// luaTokenBucket is the atomic token bucket script executed in Redis.
// It uses Redis's internal monotonic time (via redis.call('TIME')) to avoid
// clock skew across multiple application nodes, refills lazily based on
// elapsed seconds, enforces capacity clamp, and sets a TTL to reclaim memory.
const luaTokenBucket = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3]) or 1

local now_time = redis.call('TIME')
local now = tonumber(now_time[1]) + (tonumber(now_time[2]) / 1000000)

local data = redis.call('HMGET', key, 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])

if not tokens then
    tokens = capacity
    last = now
else
    local elapsed = now - last
    if elapsed > 0 then
        tokens = math.min(capacity, tokens + elapsed * rate)
    end
    last = now
end

local allowed = 0
local wait_ms = 0

if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
    wait_ms = 0
else
    allowed = 0
    wait_ms = math.ceil(((cost - tokens) / rate) * 1000)
end

redis.call('HMSET', key, 'tokens', tokens, 'last', last)
local ttl = math.max(60, math.ceil(capacity / rate) * 2)
redis.call('EXPIRE', key, ttl)

return {allowed, wait_ms}
`

// RedisLimiter implements the Limiter interface using a shared Redis instance.
// Safe for concurrent use across multiple goroutines and processes.
type RedisLimiter struct {
	addr      string
	capacity  float64
	rate      float64
	timeout   time.Duration
	keyPrefix string
	dial      func() (net.Conn, error)

	// OnFailOpen, when set, is called each time a request is admitted
	// only because Redis could not be reached or answered. Without it a
	// Redis outage removes all rate limiting with no sign of it:
	// callers wire this to a metric and a log line. It runs on the
	// request path, so it must be cheap and must not block.
	OnFailOpen func(err error)

	failOpenCount atomic.Uint64

	mu     sync.Mutex
	closed bool
	pool   chan net.Conn
}

// NewRedisLimiter creates a new Redis-backed rate limiter.
// capacity is the burst size; ratePerSec is the refill rate in tokens/sec.
func NewRedisLimiter(addr string, capacity, ratePerSec float64) (*RedisLimiter, error) {
	if capacity <= 0 {
		return nil, errors.New("limiter: capacity must be positive")
	}
	if ratePerSec <= 0 {
		return nil, errors.New("limiter: rate must be positive")
	}
	return &RedisLimiter{
		addr:      addr,
		capacity:  capacity,
		rate:      ratePerSec,
		timeout:   100 * time.Millisecond,
		keyPrefix: "flowgate:rl:",
		pool:      make(chan net.Conn, 64),
	}, nil
}

// Allow reports whether a request for key is allowed right now.
//
// If Redis is unavailable, times out or answers with something
// unexpected, it fails open (returns true, 0) so a limiter outage does
// not become a gateway outage. The tradeoff is that the limit simply
// disappears while Redis is down. Every fail-open is counted
// (FailOpenCount) and reported through OnFailOpen so that is visible.
func (r *RedisLimiter) Allow(key string) (bool, time.Duration) {
	conn, err := r.getConn()
	if err != nil {
		return r.failOpen(err)
	}

	_ = conn.SetDeadline(time.Now().Add(r.timeout))
	fullKey := r.keyPrefix + key
	cmd := encodeRESP(
		"EVAL",
		luaTokenBucket,
		"1",
		fullKey,
		strconv.FormatFloat(r.capacity, 'f', -1, 64),
		strconv.FormatFloat(r.rate, 'f', -1, 64),
		"1",
	)

	if _, err := conn.Write(cmd); err != nil {
		_ = conn.Close()
		return r.failOpen(err)
	}

	reader := bufio.NewReader(conn)
	resp, err := readRESP(reader)
	if err != nil {
		_ = conn.Close()
		return r.failOpen(err)
	}

	r.putConn(conn)

	arr, ok := resp.([]any)
	if !ok || len(arr) < 2 {
		return r.failOpen(errors.New("limiter: unexpected redis reply shape"))
	}

	allowedInt, _ := toInt(arr[0])
	waitMS, _ := toInt(arr[1])

	if allowedInt == 1 {
		return true, 0
	}
	return false, time.Duration(waitMS) * time.Millisecond
}

// failOpen records one fail-open decision and admits the request.
func (r *RedisLimiter) failOpen(err error) (bool, time.Duration) {
	r.failOpenCount.Add(1)
	if r.OnFailOpen != nil {
		r.OnFailOpen(err)
	}
	return true, 0
}

// FailOpenCount returns how many requests were admitted because Redis
// could not be used.
func (r *RedisLimiter) FailOpenCount() uint64 {
	return r.failOpenCount.Load()
}

// Close closes all pooled connections.
func (r *RedisLimiter) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()

	for {
		select {
		case c := <-r.pool:
			_ = c.Close()
		default:
			return nil
		}
	}
}

func (r *RedisLimiter) getConn() (net.Conn, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("limiter: redis connection pool closed")
	}
	r.mu.Unlock()

	select {
	case c := <-r.pool:
		return c, nil
	default:
		if r.dial != nil {
			return r.dial()
		}
		return net.DialTimeout("tcp", r.addr, r.timeout)
	}
}

func (r *RedisLimiter) putConn(c net.Conn) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()

	if closed {
		_ = c.Close()
		return
	}

	select {
	case r.pool <- c:
	default:
		_ = c.Close()
	}
}

// encodeRESP formats a RESP array of bulk strings.
func encodeRESP(args ...string) []byte {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, a := range args {
		b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(a), a))
	}
	return b.Bytes()
}

// readRESP decodes a single RESP message.
func readRESP(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	line, err := readLine(r)
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+': // Simple string
		return string(line), nil
	case '-': // Error
		return nil, fmt.Errorf("redis: %s", string(line))
	case ':': // Integer
		return strconv.ParseInt(string(line), 10, 64)
	case '$': // Bulk string
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil // Null bulk string
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*': // Array
		count, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil // Null array
		}
		arr := make([]any, count)
		for i := 0; i < count; i++ {
			elem, err := readRESP(r)
			if err != nil {
				return nil, err
			}
			arr[i] = elem
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unknown RESP prefix %q", prefix)
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("redis: malformed CRLF")
	}
	return line[:len(line)-2], nil
}

func toInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}
