package slack

import (
	"context"
	"fmt"
	"time"
)

// Probe exposes the two things worth waking someone for. They are plain
// functions so the watcher does not import the breaker or the limiter.
type Probe struct {
	// BreakerState returns "closed", "half-open" or "open".
	BreakerState func() string
	// FailOpenCount returns the shared limiter's cumulative fail-open
	// count, or nil when the limiter is not Redis-backed.
	FailOpenCount func() uint64
}

// Watcher turns state changes into alerts. It polls rather than hooking
// the request path: polling cannot slow or break a request, and a
// breaker that stays open for ten seconds is comfortably longer than a
// one-second poll.
type Watcher struct {
	n Notifier
	p Probe

	lastState    string
	lastFailOpen uint64
}

func NewWatcher(n Notifier, p Probe) *Watcher {
	w := &Watcher{n: n, p: p, lastState: "closed"}
	if p.FailOpenCount != nil {
		w.lastFailOpen = p.FailOpenCount() // alert on new failures, not history
	}
	return w
}

// Step checks once and sends any alerts that are due.
func (w *Watcher) Step(ctx context.Context) {
	if w.p.BreakerState != nil {
		state := w.p.BreakerState()
		if state != w.lastState {
			switch {
			case state == "open":
				w.n.Notify(ctx, "breaker-open",
					"[ALERT] flowgate circuit breaker is OPEN: the backend is failing and requests are being rejected with 503.")
			case state == "closed" && w.lastState != "closed":
				w.n.NotifyNow(ctx, "[RECOVERED] flowgate circuit breaker is closed again.")
			}
			w.lastState = state
		}
	}
	if w.p.FailOpenCount != nil {
		if n := w.p.FailOpenCount(); n > w.lastFailOpen {
			w.n.Notify(ctx, "limiter-fail-open", fmt.Sprintf(
				"[ALERT] flowgate shared rate limiter cannot reach Redis and is failing open: %d request(s) admitted without a limit since the last check (%d total). Per-client limits are NOT being enforced.",
				n-w.lastFailOpen, n))
			w.lastFailOpen = n
		}
	}
}

// Run steps every interval until ctx is done.
func (w *Watcher) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.Step(ctx)
		}
	}
}
