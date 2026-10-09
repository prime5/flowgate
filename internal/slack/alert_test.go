package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type hookServer struct {
	srv    *httptest.Server
	mu     sync.Mutex
	texts  []string
	status int
}

func newHook(t *testing.T) *hookServer {
	t.Helper()
	h := &hookServer{status: 200}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]string
		_ = json.NewDecoder(r.Body).Decode(&m)
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.status/100 == 2 {
			h.texts = append(h.texts, m["text"])
		}
		w.WriteHeader(h.status)
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *hookServer) got() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.texts...)
}

func TestAlerter_ThrottlesPerKey(t *testing.T) {
	h := newHook(t)
	a := NewAlerter(h.srv.URL, nil)
	clock := time.Unix(1700000000, 0)
	a.now = func() time.Time { return clock }
	ctx := context.Background()

	if !a.Notify(ctx, "breaker-open", "one") {
		t.Fatal("first alert should send")
	}
	if a.Notify(ctx, "breaker-open", "two") {
		t.Fatal("same key inside the window should be suppressed")
	}
	if !a.Notify(ctx, "limiter-fail-open", "other key") {
		t.Fatal("a different key has its own window")
	}
	clock = clock.Add(DefaultAlertGap + time.Second)
	if !a.Notify(ctx, "breaker-open", "three") {
		t.Fatal("after the window the key should fire again")
	}
	if got := h.got(); len(got) != 3 || got[0] != "one" || got[1] != "other key" || got[2] != "three" {
		t.Fatalf("delivered %v", got)
	}
}

func TestAlerter_FailedSendDoesNotStartQuietPeriod(t *testing.T) {
	h := newHook(t)
	a := NewAlerter(h.srv.URL, nil)
	ctx := context.Background()

	h.status = 500
	if a.Notify(ctx, "k", "lost") {
		t.Fatal("a 500 must not count as sent")
	}
	h.mu.Lock()
	h.status = 200
	h.mu.Unlock()
	if !a.Notify(ctx, "k", "retry") {
		t.Fatal("after a failed send the next attempt must not be throttled")
	}
}

func TestAlerter_NotifyNowIgnoresThrottle(t *testing.T) {
	h := newHook(t)
	a := NewAlerter(h.srv.URL, nil)
	ctx := context.Background()
	a.Notify(ctx, "k", "alert")
	a.NotifyNow(ctx, "recovered")
	if got := h.got(); len(got) != 2 {
		t.Fatalf("delivered %v, want alert then recovery", got)
	}
}

func TestValidateWebhookURL(t *testing.T) {
	for url, ok := range map[string]bool{
		"https://hooks.slack.com/services/T0/B0/xyz": true,
		"http://hooks.slack.com/services/T0/B0/xyz":  false,
		"https://evil.example.com/services/T0/B0/x":  false,
		"https://hooks.slack.com.evil.com/x":         false,
		"":                                           false,
	} {
		if err := ValidateWebhookURL(url); (err == nil) != ok {
			t.Errorf("ValidateWebhookURL(%q) err=%v, want ok=%v", url, err, ok)
		}
	}
}

type fakeNotifier struct {
	alerts, now []string
}

func (f *fakeNotifier) Notify(_ context.Context, key, text string) bool {
	f.alerts = append(f.alerts, key)
	return true
}
func (f *fakeNotifier) NotifyNow(_ context.Context, text string) bool {
	f.now = append(f.now, text)
	return true
}

func TestWatcher_BreakerOpenThenRecovered(t *testing.T) {
	state := "closed"
	n := &fakeNotifier{}
	w := NewWatcher(n, Probe{BreakerState: func() string { return state }})
	ctx := context.Background()

	w.Step(ctx) // closed -> closed: quiet
	state = "open"
	w.Step(ctx)
	w.Step(ctx) // still open: no second alert from the watcher
	state = "half-open"
	w.Step(ctx) // trial: neither an alert nor a recovery
	state = "closed"
	w.Step(ctx)

	if len(n.alerts) != 1 || n.alerts[0] != "breaker-open" {
		t.Fatalf("alerts = %v, want exactly one breaker-open", n.alerts)
	}
	if len(n.now) != 1 {
		t.Fatalf("recoveries = %v, want exactly one", n.now)
	}
}

func TestWatcher_HalfOpenFailingAgainAlertsAgain(t *testing.T) {
	state := "open"
	n := &fakeNotifier{}
	w := NewWatcher(n, Probe{BreakerState: func() string { return state }})
	ctx := context.Background()
	w.Step(ctx)
	state = "half-open"
	w.Step(ctx)
	state = "open" // the trial request failed
	w.Step(ctx)
	if len(n.alerts) != 2 || len(n.now) != 0 {
		t.Fatalf("alerts=%v recoveries=%v", n.alerts, n.now)
	}
}

func TestWatcher_FailOpenAlertsOnNewFailuresOnly(t *testing.T) {
	count := uint64(7) // history from before the watcher started
	n := &fakeNotifier{}
	w := NewWatcher(n, Probe{FailOpenCount: func() uint64 { return count }})
	ctx := context.Background()

	w.Step(ctx)
	if len(n.alerts) != 0 {
		t.Fatalf("pre-existing count must not alert, got %v", n.alerts)
	}
	count = 12
	w.Step(ctx)
	w.Step(ctx) // no change since last step
	if len(n.alerts) != 1 || n.alerts[0] != "limiter-fail-open" {
		t.Fatalf("alerts = %v, want one limiter-fail-open", n.alerts)
	}
}
