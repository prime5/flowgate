package slack

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prime5/flowgate/internal/runner"
)

const testSecret = "unit-test-secret"

// labDispatcher stands in for the MCP server: run_experiment returns an id
// at once, and get_verdict reports "running" until `done` is closed.
type labDispatcher struct {
	mu       sync.Mutex
	refuse   string // if set, run_experiment is refused with this text
	started  []json.RawMessage
	done     bool
	lastArgs map[string]json.RawMessage
}

func (d *labDispatcher) Call(_ context.Context, name string, args json.RawMessage) (any, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch name {
	case "run_experiment":
		if d.refuse != "" {
			return map[string]any{"error": d.refuse}, true, nil
		}
		d.started = append(d.started, args)
		var in struct {
			Fault string  `json:"fault"`
			Blast float64 `json:"blast_radius"`
		}
		_ = json.Unmarshal(args, &in)
		return map[string]any{"experiment_id": "exp-1", "status": "running", "fault": in.Fault, "blast_radius": in.Blast}, false, nil
	case "get_verdict":
		var in struct {
			ID string `json:"experiment_id"`
		}
		_ = json.Unmarshal(args, &in)
		if in.ID == "" {
			return map[string]any{"error": "experiment_id is required"}, true, nil
		}
		if in.ID != "exp-1" {
			return map[string]any{"error": "unknown experiment_id " + strconv.Quote(in.ID)}, true, nil
		}
		if !d.done {
			return map[string]any{"experiment_id": "exp-1", "fault": "latency", "blast_radius": 0.2, "status": "running"}, false, nil
		}
		return map[string]any{
			"experiment_id": "exp-1", "fault": "latency", "blast_radius": 0.2, "status": "done",
			"verdict": map[string]any{"held": false, "baseline_ok": true, "recovered": true, "detail": "p99 exceeded 200ms"},
		}, false, nil
	}
	return nil, false, errors.New("unknown tool: " + name)
}

func (d *labDispatcher) finish() { d.mu.Lock(); d.done = true; d.mu.Unlock() }

type capturePoster struct {
	mu   sync.Mutex
	msgs []Message
	urls []string
	ch   chan struct{}
}

func newCapture() *capturePoster { return &capturePoster{ch: make(chan struct{}, 8)} }
func (c *capturePoster) Post(_ context.Context, u string, m Message) error {
	c.mu.Lock()
	c.msgs, c.urls = append(c.msgs, m), append(c.urls, u)
	c.mu.Unlock()
	c.ch <- struct{}{}
	return nil
}

type fixture struct {
	h      *Handler
	disp   *labDispatcher
	poster *capturePoster
	now    time.Time
}

func newFixture(t *testing.T, mutate func(*Config)) *fixture {
	t.Helper()
	disp := &labDispatcher{}
	poster := newCapture()
	v, _ := NewVerifier(testSecret)
	now := time.Now()
	v.now = func() time.Time { return now }
	cfg := Config{
		Verifier: v,
		Tools:    RunnerCaller{R: runner.New(runner.NewMemoryStore(), disp)},
		Status: func() Status {
			return Status{Breaker: "closed", InFlight: 3, MaxInFlight: 50, LimiterMode: "redis", FailOpen: 4, Tracing: true}
		},
		Poster:          poster,
		RunEnabled:      true,
		AllowedRunUsers: map[string]bool{"UALLOWED": true},
		PollEvery:       5 * time.Millisecond,
		MaxWait:         2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h, err := NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{h: h, disp: disp, poster: poster, now: now}
}

// send builds a correctly signed slash-command request.
func (f *fixture) send(t *testing.T, user, text string) (*httptest.ResponseRecorder, Message) {
	t.Helper()
	form := url.Values{
		"command": {"/flowgate"}, "text": {text}, "user_id": {user},
		"channel_id": {"C1"}, "team_id": {"T1"},
		"response_url": {"https://hooks.slack.com/commands/T1/1/abc"},
	}
	body := []byte(form.Encode())
	req := httptest.NewRequest(http.MethodPost, "/slack/command", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(f.now.Unix(), 10))
	req.Header.Set(HeaderSignature, Sign(testSecret, f.now.Unix(), body))
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	var msg Message
	_ = json.Unmarshal(rec.Body.Bytes(), &msg)
	return rec, msg
}

func TestHandler_RejectsBadRequests(t *testing.T) {
	f := newFixture(t, nil)
	body := []byte("command=%2Fflowgate&text=status&user_id=UALLOWED")

	cases := map[string]func(*http.Request){
		"no signature":    func(r *http.Request) { r.Header.Del(HeaderSignature) },
		"wrong signature": func(r *http.Request) { r.Header.Set(HeaderSignature, Sign("not-the-secret", f.now.Unix(), body)) },
		"stale timestamp": func(r *http.Request) {
			old := f.now.Add(-10 * time.Minute).Unix()
			r.Header.Set(HeaderTimestamp, strconv.FormatInt(old, 10))
			r.Header.Set(HeaderSignature, Sign(testSecret, old, body))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/slack/command", strings.NewReader(string(body)))
			req.Header.Set(HeaderTimestamp, strconv.FormatInt(f.now.Unix(), 10))
			req.Header.Set(HeaderSignature, Sign(testSecret, f.now.Unix(), body))
			mutate(req)
			rec := httptest.NewRecorder()
			f.h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("code = %d, want 401", rec.Code)
			}
			if strings.Contains(rec.Body.String(), "signature") || strings.Contains(rec.Body.String(), "stale") {
				t.Fatalf("response leaks the reason: %q", rec.Body.String())
			}
		})
	}
	t.Run("GET", func(t *testing.T) {
		rec := httptest.NewRecorder()
		f.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/slack/command", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("code = %d, want 405", rec.Code)
		}
	})
	if len(f.disp.started) != 0 {
		t.Fatal("a rejected request must never reach a tool")
	}
}

func TestHandler_Status(t *testing.T) {
	f := newFixture(t, nil)
	rec, msg := f.send(t, "UANYONE", "status")
	if rec.Code != 200 || msg.ResponseType != "ephemeral" {
		t.Fatalf("code=%d type=%q", rec.Code, msg.ResponseType)
	}
	for _, want := range []string{"closed", "3 / 50", "redis", "fail-open so far: 4", "tracing: on"} {
		if !strings.Contains(msg.Text, want) {
			t.Errorf("status missing %q in:\n%s", want, msg.Text)
		}
	}
}

func TestHandler_UsageAndUnknown(t *testing.T) {
	f := newFixture(t, nil)
	if _, msg := f.send(t, "U", ""); !strings.Contains(msg.Text, "Usage") {
		t.Fatalf("empty text should show usage, got %q", msg.Text)
	}
	if _, msg := f.send(t, "U", "frobnicate"); !strings.Contains(msg.Text, "Unknown command") {
		t.Fatalf("got %q", msg.Text)
	}
}

func TestHandler_RunAuthorization(t *testing.T) {
	t.Run("user not on the list", func(t *testing.T) {
		f := newFixture(t, nil)
		_, msg := f.send(t, "USTRANGER", "run latency 0.2 5")
		if !strings.Contains(msg.Text, "not on the list") || len(f.disp.started) != 0 {
			t.Fatalf("msg=%q started=%d", msg.Text, len(f.disp.started))
		}
	})
	t.Run("run disabled", func(t *testing.T) {
		f := newFixture(t, func(c *Config) { c.RunEnabled = false })
		_, msg := f.send(t, "UALLOWED", "run latency 0.2 5")
		if !strings.Contains(msg.Text, "disabled") || len(f.disp.started) != 0 {
			t.Fatalf("msg=%q started=%d", msg.Text, len(f.disp.started))
		}
	})
	t.Run("empty allowlist allows nobody", func(t *testing.T) {
		f := newFixture(t, func(c *Config) { c.AllowedRunUsers = nil })
		_, msg := f.send(t, "UALLOWED", "run latency 0.2 5")
		if len(f.disp.started) != 0 {
			t.Fatalf("started with an empty allowlist: %q", msg.Text)
		}
	})
}

func TestHandler_RunThenVerdictPostedToResponseURL(t *testing.T) {
	f := newFixture(t, nil)
	_, msg := f.send(t, "UALLOWED", "run latency 0.2 5 latency_ms=300")

	if msg.ResponseType != "in_channel" || !strings.Contains(msg.Text, "exp-1") || !strings.Contains(msg.Text, "<@UALLOWED>") {
		t.Fatalf("ack = %+v", msg)
	}
	var sent map[string]any
	_ = json.Unmarshal(f.disp.started[0], &sent)
	if sent["fault"] != "latency" || sent["blast_radius"] != 0.2 || sent["duration_s"] != 5.0 || sent["latency_ms"] != 300.0 {
		t.Fatalf("run_experiment args = %v", sent)
	}

	select {
	case <-f.poster.ch:
		t.Fatal("verdict posted before the experiment finished")
	case <-time.After(40 * time.Millisecond):
	}
	f.disp.finish()
	select {
	case <-f.poster.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("verdict never posted")
	}
	f.poster.mu.Lock()
	defer f.poster.mu.Unlock()
	got := f.poster.msgs[0]
	if got.ResponseType != "in_channel" || !strings.Contains(got.Text, "held during the fault: no") || !strings.Contains(got.Text, "p99 exceeded 200ms") {
		t.Fatalf("verdict message = %+v", got)
	}
	if f.poster.urls[0] != "https://hooks.slack.com/commands/T1/1/abc" {
		t.Fatalf("posted to %q", f.poster.urls[0])
	}
}

// The runner is what makes "run" then "verdict" a pair: with no id the
// runner supplies the experiment this conversation started.
func TestHandler_VerdictWithoutIDUsesLastRunInThisConversation(t *testing.T) {
	f := newFixture(t, nil)

	_, msg := f.send(t, "UALLOWED", "verdict")
	if !strings.Contains(msg.Text, "haven't started one") {
		t.Fatalf("before any run: %q", msg.Text)
	}
	f.send(t, "UALLOWED", "run latency 0.2 5")
	_, msg = f.send(t, "UALLOWED", "verdict")
	if !strings.Contains(msg.Text, "exp-1") || !strings.Contains(msg.Text, "running") {
		t.Fatalf("after a run, verdict with no id: %q", msg.Text)
	}
	// Another person in the same channel has their own conversation.
	_, msg = f.send(t, "UOTHER", "verdict")
	if !strings.Contains(msg.Text, "haven't started one") {
		t.Fatalf("a different user inherited someone else's experiment: %q", msg.Text)
	}
}

func TestHandler_VerdictByIDAndUnknownID(t *testing.T) {
	f := newFixture(t, nil)
	f.disp.finish()
	_, msg := f.send(t, "UANYONE", "verdict exp-1")
	if !strings.Contains(msg.Text, "recovered after rollback: yes") {
		t.Fatalf("got %q", msg.Text)
	}
	_, msg = f.send(t, "UANYONE", "verdict exp-99")
	if !strings.HasPrefix(msg.Text, "Refused:") || !strings.Contains(msg.Text, "unknown experiment_id") {
		t.Fatalf("got %q", msg.Text)
	}
}

func TestHandler_ServerRefusalIsRelayedNotRetried(t *testing.T) {
	f := newFixture(t, nil)
	f.disp.refuse = "blast_radius 0.9 exceeds the server limit of 0.5"
	_, msg := f.send(t, "UALLOWED", "run latency 0.9 5")
	if msg.ResponseType != "ephemeral" || !strings.Contains(msg.Text, "exceeds the server limit of 0.5") {
		t.Fatalf("got %+v", msg)
	}
}

func TestParseRunArgs_Errors(t *testing.T) {
	for name, args := range map[string][]string{
		"too few":          {"latency", "0.2"},
		"non-numeric":      {"latency", "lots", "5"},
		"nan":              {"latency", "NaN", "5"},
		"inf duration":     {"latency", "0.2", "Inf"},
		"unknown option":   {"latency", "0.2", "5", "colour=red"},
		"option no equals": {"latency", "0.2", "5", "latency_ms"},
		"bad option value": {"latency", "0.2", "5", "latency_ms=fast"},
	} {
		if _, err := parseRunArgs(args); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	raw, err := parseRunArgs([]string{"CAPACITY", "0.5", "10", "capacity_to=2", "recovery_s=7"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if m["fault"] != "capacity" || m["capacity_to"] != 2.0 || m["recovery_timeout_s"] != 7.0 {
		t.Fatalf("args = %v", m)
	}
}

func TestHTTPPoster_DestinationCheck(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()

	p := HTTPPoster{}
	for _, bad := range []string{srv.URL, "http://hooks.slack.com/x", "https://evil.example.com/x", "https://slack.com.evil.com/x", "file:///etc/passwd"} {
		if err := p.Post(context.Background(), bad, ephemeral("x")); err == nil {
			t.Errorf("Post(%q) should be refused", bad)
		}
	}
	if hits != 0 {
		t.Fatalf("refused destinations were contacted %d time(s)", hits)
	}
	ok := HTTPPoster{Allow: func(*url.URL) bool { return true }}
	if err := ok.Post(context.Background(), srv.URL, ephemeral("hi")); err != nil || hits != 1 {
		t.Fatalf("err=%v hits=%d", err, hits)
	}
}
