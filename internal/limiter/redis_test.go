package limiter

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// Ensure RedisLimiter implements the Limiter interface.
var _ Limiter = (*RedisLimiter)(nil)

func TestRedisLimiter_FailOpenWhenDown(t *testing.T) {
	// A non-existent port should fail open immediately without panicking
	rl, err := NewRedisLimiter("127.0.0.1:59999", 10, 5)
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	defer rl.Close()

	allowed, wait := rl.Allow("client-1")
	if !allowed {
		t.Errorf("expected fail-open (allowed=true) when Redis is unreachable, got false")
	}
	if wait != 0 {
		t.Errorf("expected wait=0 on fail-open, got %v", wait)
	}
}

func TestRedisLimiter_MockServer(t *testing.T) {
	var mu sync.Mutex
	remaining := 2

	serverConn, clientConn := net.Pipe()

	go func() {
		reader := bufio.NewReader(serverConn)
		for {
			_, err := readRESP(reader)
			if err != nil {
				return
			}
			mu.Lock()
			var reply []byte
			if remaining > 0 {
				remaining--
				reply = []byte("*2\r\n:1\r\n:0\r\n") // allowed=1, wait=0
			} else {
				reply = []byte("*2\r\n:0\r\n:200\r\n") // allowed=0, wait=200ms
			}
			mu.Unlock()
			if _, err := serverConn.Write(reply); err != nil {
				return
			}
		}
	}()

	rl, err := NewRedisLimiter("in-memory", 2, 1)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}
	// Inject the in-memory client connection
	rl.dial = func() (net.Conn, error) {
		return clientConn, nil
	}
	defer rl.Close()

	// 1st request -> allowed
	allowed, _ := rl.Allow("c1")
	if !allowed {
		t.Errorf("request 1 should be allowed")
	}

	// 2nd request -> allowed
	allowed, _ = rl.Allow("c1")
	if !allowed {
		t.Errorf("request 2 should be allowed")
	}

	// 3rd request -> rejected
	allowed, wait := rl.Allow("c1")
	if allowed {
		t.Errorf("request 3 should be rejected")
	}
	if wait != 200*time.Millisecond {
		t.Errorf("expected wait=200ms, got %v", wait)
	}
}

func TestRedisLimiter_ConcurrentRace(t *testing.T) {
	rl, err := NewRedisLimiter("in-memory", 50, 10)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}
	rl.dial = func() (net.Conn, error) {
		sConn, cConn := net.Pipe()
		go func(c net.Conn) {
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				if _, err := readRESP(r); err != nil {
					return
				}
				if _, err := c.Write([]byte("*2\r\n:1\r\n:0\r\n")); err != nil {
					return
				}
			}
		}(sConn)
		return cConn, nil
	}
	defer rl.Close()

	var wg sync.WaitGroup
	const workers = 10
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				rl.Allow(fmt.Sprintf("user-%d", id))
			}
		}(i)
	}
	wg.Wait()
}

func TestEncodeDecodeRESP(t *testing.T) {
	cmd := encodeRESP("SET", "foo", "bar")
	reader := bufio.NewReader(bytes.NewReader(cmd))
	val, err := readRESP(reader)
	if err != nil {
		t.Fatalf("readRESP error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected array of 3, got %v", val)
	}
	if arr[0] != "SET" || arr[1] != "foo" || arr[2] != "bar" {
		t.Errorf("mismatched decoded values: %v", arr)
	}
}
