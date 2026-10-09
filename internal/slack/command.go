package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Message is the JSON Slack accepts as a slash-command response and at a
// response_url. in_channel is visible to everyone in the channel;
// ephemeral only to the person who typed the command.
type Message struct {
	ResponseType string `json:"response_type"`
	Text         string `json:"text"`
}

func ephemeral(text string) Message { return Message{ResponseType: "ephemeral", Text: text} }
func inChannel(text string) Message { return Message{ResponseType: "in_channel", Text: text} }

// Status is the snapshot behind "/flowgate status".
type Status struct {
	Breaker     string
	InFlight    int
	MaxInFlight int
	LimiterMode string // "memory" or "redis"
	FailOpen    uint64
	Tracing     bool
}

// Poster delivers a delayed message to a response_url.
type Poster interface {
	Post(ctx context.Context, responseURL string, msg Message) error
}

// HTTPPoster posts to Slack response URLs. It refuses any URL that is not
// https on slack.com: the URL arrives inside the request body, and a
// forged-but-correctly-signed request is not the only way a bad value
// could land there, so the destination is checked rather than trusted.
type HTTPPoster struct {
	Client *http.Client
	// Allow overrides the destination check; tests use it to point at a
	// local server. Nil means "https and a slack.com host".
	Allow func(*url.URL) bool
}

func slackHost(u *url.URL) bool {
	h := strings.ToLower(u.Hostname())
	return u.Scheme == "https" && (h == "slack.com" || strings.HasSuffix(h, ".slack.com"))
}

func (p HTTPPoster) Post(ctx context.Context, responseURL string, msg Message) error {
	u, err := url.Parse(responseURL)
	if err != nil {
		return fmt.Errorf("slack: bad response_url: %w", err)
	}
	allow := p.Allow
	if allow == nil {
		allow = slackHost
	}
	if !allow(u) {
		return fmt.Errorf("slack: refusing response_url on %q", u.Host)
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	body, _ := json.Marshal(msg)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, responseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("slack: response_url returned %d", resp.StatusCode)
	}
	return nil
}

// Config wires a Handler.
type Config struct {
	Verifier *Verifier
	Tools    ToolCaller
	Status   func() Status
	Poster   Poster

	// RunEnabled turns on "/flowgate run". It is false unless the caller
	// has both opted in to experiments (FLOWGATE_LAB=1) and named who may
	// start them. AllowedRunUsers is the set of Slack user IDs.
	RunEnabled      bool
	AllowedRunUsers map[string]bool

	// PollEvery and MaxWait bound the background wait for a verdict.
	PollEvery time.Duration
	MaxWait   time.Duration

	Logf func(format string, args ...any)
}

// Handler serves POST /slack/command.
type Handler struct {
	cfg Config
}

const maxBody = 64 << 10

// NewHandler validates the config.
func NewHandler(cfg Config) (*Handler, error) {
	if cfg.Verifier == nil || cfg.Tools == nil || cfg.Status == nil {
		return nil, errors.New("slack: Verifier, Tools and Status are required")
	}
	if cfg.Poster == nil {
		cfg.Poster = HTTPPoster{}
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = time.Second
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 90 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Handler{cfg: cfg}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Verify the raw bytes first; nothing in the body is looked at until
	// the signature has passed. The error is logged but not echoed, so a
	// caller learns nothing about why it was refused.
	if err := h.cfg.Verifier.Verify(r.Header, body); err != nil {
		h.cfg.Logf("slack: rejected request: %v", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	conv := Conversation{
		ChannelID: form.Get("channel_id"),
		UserID:    form.Get("user_id"),
	}
	conv.ID = "slack:" + form.Get("team_id") + ":" + conv.ChannelID + ":" + conv.UserID

	// Slack wants an answer within three seconds, with HTTP 200 even for
	// a command the user got wrong (anything else renders as a generic
	// failure). So every path below returns 200 with a message.
	msg := h.dispatch(r.Context(), conv, form)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(msg)
}

const usage = "Usage:\n" +
	"• `/flowgate status`\n" +
	"• `/flowgate verdict [experiment_id]` (no id = the experiment you last started here)\n" +
	"• `/flowgate run <fault> <blast_radius> <duration_s> [latency_ms=N] [capacity_to=N] [recovery_s=N]`\n" +
	"Faults: latency, error, timeout, cpuburn, blackhole, capacity."

func (h *Handler) dispatch(ctx context.Context, conv Conversation, form url.Values) Message {
	fields := strings.Fields(form.Get("text"))
	if len(fields) == 0 {
		return ephemeral(usage)
	}
	switch strings.ToLower(fields[0]) {
	case "status":
		return ephemeral(formatStatus(h.cfg.Status()))
	case "verdict":
		return h.verdict(ctx, conv, fields[1:])
	case "run":
		return h.run(ctx, conv, form.Get("response_url"), fields[1:])
	default:
		return ephemeral("Unknown command `" + fields[0] + "`.\n" + usage)
	}
}

func formatStatus(s Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*flowgate status*\n")
	fmt.Fprintf(&b, "• circuit breaker: %s\n", s.Breaker)
	fmt.Fprintf(&b, "• in flight: %d / %d\n", s.InFlight, s.MaxInFlight)
	fmt.Fprintf(&b, "• rate limiter: %s", s.LimiterMode)
	if s.LimiterMode == "redis" {
		fmt.Fprintf(&b, " (fail-open so far: %d)", s.FailOpen)
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "• tracing: %v", map[bool]string{true: "on", false: "off"}[s.Tracing])
	return b.String()
}

// runView is the part of a run the formatter needs. Decoding through JSON
// accepts whatever shape the tool returned (struct or map).
type runView struct {
	ID          string  `json:"experiment_id"`
	Fault       string  `json:"fault"`
	BlastRadius float64 `json:"blast_radius"`
	Status      string  `json:"status"`
	Verdict     *struct {
		Held       bool   `json:"held"`
		BaselineOK bool   `json:"baseline_ok"`
		Recovered  bool   `json:"recovered"`
		Detail     string `json:"detail"`
	} `json:"verdict"`
}

func decodeRun(content any) (runView, error) {
	var v runView
	raw, err := json.Marshal(content)
	if err != nil {
		return v, err
	}
	return v, json.Unmarshal(raw, &v)
}

func errText(content any) string {
	raw, _ := json.Marshal(content)
	var m struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &m) == nil && m.Error != "" {
		return m.Error
	}
	return string(raw)
}

func formatVerdict(v runView) string {
	head := fmt.Sprintf("*%s* — %s at blast radius %.2f", v.ID, v.Fault, v.BlastRadius)
	if v.Status != "done" || v.Verdict == nil {
		return head + "\nstatus: " + v.Status + " (no verdict yet)"
	}
	yn := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	var b strings.Builder
	b.WriteString(head + "\n")
	if !v.Verdict.BaselineOK {
		b.WriteString("• baseline held before injection: no. The system was already unhealthy, nothing was injected, so ignore the fields below.\n")
	} else {
		fmt.Fprintf(&b, "• baseline held before injection: yes\n")
	}
	fmt.Fprintf(&b, "• steady state held during the fault: %s\n", yn(v.Verdict.Held))
	fmt.Fprintf(&b, "• recovered after rollback: %s", yn(v.Verdict.Recovered))
	if v.Verdict.BaselineOK && !v.Verdict.Recovered {
		b.WriteString(" — the system did NOT return to steady state after the fault was removed")
	}
	if v.Verdict.Detail != "" {
		b.WriteString("\n• " + v.Verdict.Detail)
	}
	return b.String()
}

func (h *Handler) verdict(ctx context.Context, conv Conversation, args []string) Message {
	payload := json.RawMessage(`{}`)
	if len(args) > 0 {
		payload, _ = json.Marshal(map[string]string{"experiment_id": args[0]})
	}
	// With no id the runner fills in this conversation's active
	// experiment, which is what makes "run" then "verdict" work as a pair.
	content, isErr, err := h.cfg.Tools.Call(ctx, conv, "get_verdict", payload)
	if err != nil {
		h.cfg.Logf("slack: get_verdict: %v", err)
		return ephemeral("Could not fetch the verdict (internal error).")
	}
	if isErr {
		msg := errText(content)
		if len(args) == 0 && strings.Contains(msg, "experiment_id is required") {
			return ephemeral("No experiment id given, and you haven't started one in this channel. Use `/flowgate verdict <id>`.")
		}
		return ephemeral("Refused: " + msg)
	}
	v, err := decodeRun(content)
	if err != nil {
		return ephemeral("Could not read the verdict.")
	}
	return ephemeral(formatVerdict(v))
}

func (h *Handler) run(ctx context.Context, conv Conversation, responseURL string, args []string) Message {
	if !h.cfg.RunEnabled {
		return ephemeral("`run` is disabled on this deployment. It needs FLOWGATE_LAB=1 and SLACK_ALLOWED_USER_IDS.")
	}
	if !h.cfg.AllowedRunUsers[conv.UserID] {
		return ephemeral("You are not on the list of users allowed to start experiments.")
	}
	payload, err := parseRunArgs(args)
	if err != nil {
		return ephemeral(err.Error() + "\n" + usage)
	}
	content, isErr, err := h.cfg.Tools.Call(ctx, conv, "run_experiment", payload)
	if err != nil {
		h.cfg.Logf("slack: run_experiment: %v", err)
		return ephemeral("Could not start the experiment (internal error).")
	}
	if isErr {
		// Policy refusals (blast radius over the cap, another run in
		// progress) come back here with the limit named.
		return ephemeral("Refused: " + errText(content))
	}
	run, err := decodeRun(content)
	if err != nil || run.ID == "" {
		return ephemeral("The experiment started but I could not read its id; try `/flowgate verdict`.")
	}

	// The experiment takes seconds and Slack gives us three, so answer now
	// and post the verdict to the response_url when it lands.
	go h.followUp(conv, responseURL, run.ID)
	return inChannel(fmt.Sprintf("<@%s> started experiment `%s` (%s, blast radius %.2f). I'll post the verdict here when it finishes.",
		conv.UserID, run.ID, run.Fault, run.BlastRadius))
}

func (h *Handler) followUp(conv Conversation, responseURL, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.MaxWait)
	defer cancel()
	args, _ := json.Marshal(map[string]string{"experiment_id": id})

	t := time.NewTicker(h.cfg.PollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			h.post(responseURL, inChannel("Still no verdict for `"+id+"` after "+h.cfg.MaxWait.String()+". Check later with `/flowgate verdict "+id+"`."))
			return
		case <-t.C:
			content, isErr, err := h.cfg.Tools.Call(ctx, conv, "get_verdict", args)
			if err != nil || isErr {
				continue
			}
			v, err := decodeRun(content)
			if err != nil || v.Status != "done" {
				continue
			}
			h.post(responseURL, inChannel(formatVerdict(v)))
			return
		}
	}
}

func (h *Handler) post(responseURL string, msg Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.cfg.Poster.Post(ctx, responseURL, msg); err != nil {
		h.cfg.Logf("slack: posting to response_url: %v", err)
	}
}

// parseRunArgs turns `latency 0.2 5 latency_ms=300` into the JSON
// run_experiment takes. Validation of ranges and caps is the server's
// job and is not duplicated here: this only makes sure the text is
// well-formed, so the one place that decides policy stays the one place.
func parseRunArgs(args []string) (json.RawMessage, error) {
	if len(args) < 3 {
		return nil, errors.New("`run` needs <fault> <blast_radius> <duration_s>.")
	}
	num := func(name, s string) (float64, error) {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("%s must be a number, got %q.", name, s)
		}
		return f, nil
	}
	blast, err := num("blast_radius", args[1])
	if err != nil {
		return nil, err
	}
	dur, err := num("duration_s", args[2])
	if err != nil {
		return nil, err
	}
	m := map[string]any{"fault": strings.ToLower(args[0]), "blast_radius": blast, "duration_s": dur}

	optional := map[string]string{"latency_ms": "latency_ms", "capacity_to": "capacity_to", "recovery_s": "recovery_timeout_s"}
	ints := map[string]bool{"latency_ms": true, "capacity_to": true}
	for _, kv := range args[3:] {
		k, v, ok := strings.Cut(kv, "=")
		field, known := optional[k]
		if !ok || !known {
			keys := make([]string, 0, len(optional))
			for k := range optional {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			return nil, fmt.Errorf("unknown option %q; allowed: %s.", kv, strings.Join(keys, ", "))
		}
		f, err := num(k, v)
		if err != nil {
			return nil, err
		}
		if ints[k] {
			m[field] = int(f)
		} else {
			m[field] = f
		}
	}
	return json.Marshal(m)
}
