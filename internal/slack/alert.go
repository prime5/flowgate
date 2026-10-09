package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Notifier is what the watcher needs from an alert sink.
type Notifier interface {
	// Notify sends text unless an alert with the same key was sent within
	// the throttle window. It reports whether a message went out.
	Notify(ctx context.Context, key, text string) bool
	// NotifyNow sends text unconditionally (used for recoveries, which
	// must not be swallowed by the throttle that suppressed repeat alerts).
	NotifyNow(ctx context.Context, text string) bool
}

// Alerter posts to a Slack incoming webhook, throttled per key so a
// flapping breaker produces one message per window, not one per flap.
type Alerter struct {
	url    string
	client *http.Client
	minGap time.Duration
	now    func() time.Time
	logf   func(format string, args ...any)

	mu   sync.Mutex
	last map[string]time.Time
}

// DefaultAlertGap is how long a given alert key stays quiet after firing.
const DefaultAlertGap = 5 * time.Minute

// ValidateWebhookURL accepts only an https Slack webhook. The URL comes
// from the environment, but refusing anything else means a typo or a
// pasted-in wrong URL fails at startup instead of silently sending
// alert text to some other host.
func ValidateWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("slack: bad webhook url: %w", err)
	}
	if u.Scheme != "https" || u.Host != "hooks.slack.com" {
		return fmt.Errorf("slack: webhook url must be https://hooks.slack.com/..., got %s://%s", u.Scheme, u.Host)
	}
	return nil
}

// NewAlerter returns an Alerter for a webhook URL.
func NewAlerter(webhookURL string, logf func(string, ...any)) *Alerter {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Alerter{
		url:    webhookURL,
		client: &http.Client{Timeout: 5 * time.Second},
		minGap: DefaultAlertGap,
		now:    time.Now,
		logf:   logf,
		last:   map[string]time.Time{},
	}
}

func (a *Alerter) Notify(ctx context.Context, key, text string) bool {
	a.mu.Lock()
	if t, ok := a.last[key]; ok && a.now().Sub(t) < a.minGap {
		a.mu.Unlock()
		return false
	}
	a.mu.Unlock()

	if !a.post(ctx, text) {
		return false // a failed send must not start the quiet period
	}
	a.mu.Lock()
	a.last[key] = a.now()
	a.mu.Unlock()
	return true
}

func (a *Alerter) NotifyNow(ctx context.Context, text string) bool {
	return a.post(ctx, text)
}

func (a *Alerter) post(ctx context.Context, text string) bool {
	body, _ := json.Marshal(map[string]string{"text": text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		a.logf("slack alert: build request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		a.logf("slack alert: send failed: %v", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		a.logf("slack alert: webhook returned %d", resp.StatusCode)
		return false
	}
	return true
}
