package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prime5/flowgate/internal/exp"
	"github.com/prime5/flowgate/internal/fault"
	"github.com/prime5/flowgate/internal/pool"
)

// Limits are the policy an agent cannot argue with.
//
// This is the point of the package. A model driving fault injection
// will ask for whatever its prompt suggests, and prompts are editable,
// forgettable and jailbreakable. So the blast-radius cap, the duration
// cap and the one-at-a-time rule live here, in the server, between the
// model and the fault library. An agent that asks for a blast radius of
// 1.0 against a 0.5 cap is refused and told why — it is not silently
// clamped, because a verdict labelled 1.0 that actually ran at 0.5 is
// worse than no verdict at all.
type Limits struct {
	MaxBlastRadius float64
	MaxDuration    time.Duration
	MaxRecovery    time.Duration
	// HistorySize bounds the run registry so a long-lived agent
	// session cannot grow it without end.
	HistorySize int
}

// DefaultLimits are deliberately conservative: an agent gets half the
// traffic at most, for at most fifteen seconds.
func DefaultLimits() Limits {
	return Limits{
		MaxBlastRadius: 0.5,
		MaxDuration:    15 * time.Second,
		MaxRecovery:    15 * time.Second,
		HistorySize:    50,
	}
}

// Status values for a run.
const (
	StatusRunning = "running"
	StatusDone    = "done"
)

// Run is one experiment the agent started.
type Run struct {
	ID          string       `json:"experiment_id"`
	Fault       string       `json:"fault"`
	BlastRadius float64      `json:"blast_radius"`
	Status      string       `json:"status"`
	StartedAt   time.Time    `json:"started_at"`
	FinishedAt  *time.Time   `json:"finished_at,omitempty"`
	Verdict     *exp.Verdict `json:"verdict,omitempty"`
}

// Lab is the system under experiment: a self-contained worker pool and
// a fault toggle, the same shapes cmd/flowgate wires into HTTP.
//
// It fault-injects itself on purpose. The alternative — driving a
// separate running gateway over HTTP — is more realistic and is the
// natural next step, but it couples the MCP server to a second process
// and a network, which is a lot of failure surface for a first
// version. Everything here is honest about being a lab.
type Lab struct {
	limits Limits

	syncPool     *pool.Pool
	backendFault *fault.Toggle

	mu      sync.Mutex
	runs    map[string]*Run
	order   []string // newest last
	seq     int
	running bool // one experiment at a time
}

// NewLab builds the lab with its own pool and fault toggle.
func NewLab(limits Limits) *Lab {
	p := pool.New(25*time.Millisecond, 1000)
	p.SetSize(6)
	return &Lab{
		limits:       limits,
		syncPool:     p,
		backendFault: fault.NewToggle(fault.Latency(250*time.Millisecond, fault.Always())),
		runs:         map[string]*Run{},
	}
}

// runSpec is the decoded form of run_experiment's arguments.
type runSpec struct {
	Fault           string  `json:"fault"`
	BlastRadius     float64 `json:"blast_radius"`
	DurationS       float64 `json:"duration_s"`
	RecoveryTimeout float64 `json:"recovery_timeout_s"`
	LatencyMS       int     `json:"latency_ms"`
	CapacityTo      int     `json:"capacity_to"`
}

// supportedFaults are the fault names run_experiment accepts, in the
// order they appear in the schema.
var supportedFaults = []string{"latency", "error", "timeout", "cpuburn", "blackhole", "capacity"}

func isSupportedFault(name string) bool {
	for _, f := range supportedFaults {
		if f == name {
			return true
		}
	}
	return false
}

// validate applies policy. Every rejection names the limit it hit, so
// the model gets a usable correction rather than a bare refusal.
func (l *Lab) validate(s *runSpec) error {
	if !isSupportedFault(s.Fault) {
		return fmt.Errorf("unknown fault %q; supported: %s", s.Fault, strings.Join(supportedFaults, ", "))
	}
	if s.BlastRadius <= 0 {
		return fmt.Errorf("blast_radius must be > 0; a radius of 0 injects nothing")
	}
	if s.BlastRadius > l.limits.MaxBlastRadius {
		return fmt.Errorf("blast_radius %.3f exceeds the policy cap of %.3f; this limit is enforced by the server and cannot be raised by a caller",
			s.BlastRadius, l.limits.MaxBlastRadius)
	}
	if s.DurationS <= 0 {
		return fmt.Errorf("duration_s must be > 0")
	}
	if d := time.Duration(s.DurationS * float64(time.Second)); d > l.limits.MaxDuration {
		return fmt.Errorf("duration_s %.1f exceeds the policy cap of %.1fs", s.DurationS, l.limits.MaxDuration.Seconds())
	}
	if r := time.Duration(s.RecoveryTimeout * float64(time.Second)); r > l.limits.MaxRecovery {
		return fmt.Errorf("recovery_timeout_s %.1f exceeds the policy cap of %.1fs", s.RecoveryTimeout, l.limits.MaxRecovery.Seconds())
	}
	// capacity is a resource fault: blast radius for it means the
	// fraction of capacity removed, so it needs a target size.
	if s.Fault == "capacity" && s.CapacityTo < 1 {
		return fmt.Errorf("capacity_to must be >= 1 worker")
	}
	return nil
}

// RunExperiment validates, starts the experiment in the background and
// returns its handle immediately.
//
// Starting rather than blocking is the whole reason get_verdict
// exists: an experiment runs for seconds, and a tool call that blocks
// that long stalls the agent's loop and risks the client's own request
// timeout. Start, poll, decide is the shape a long-running tool should
// have.
func (l *Lab) RunExperiment(args json.RawMessage) (any, error) {
	var s runSpec
	if len(args) > 0 {
		if err := json.Unmarshal(args, &s); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
	}
	if s.RecoveryTimeout == 0 {
		s.RecoveryTimeout = 5
	}
	if err := l.validate(&s); err != nil {
		return nil, err
	}

	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return nil, fmt.Errorf("an experiment is already running; poll get_verdict until it reports %q before starting another", StatusDone)
	}
	l.seq++
	id := fmt.Sprintf("exp-%03d", l.seq)
	run := &Run{
		ID:          id,
		Fault:       s.Fault,
		BlastRadius: s.BlastRadius,
		Status:      StatusRunning,
		StartedAt:   time.Now().UTC(),
	}
	l.runs[id] = run
	l.order = append(l.order, id)
	l.trimLocked()
	l.running = true
	l.mu.Unlock()

	go l.execute(run, s)

	return map[string]any{
		"experiment_id": id,
		"status":        StatusRunning,
		"fault":         s.Fault,
		"blast_radius":  s.BlastRadius,
		"note":          "poll get_verdict with this experiment_id; the fault is rolled back automatically whatever the outcome",
	}, nil
}

// trimLocked drops the oldest runs past HistorySize. Caller holds mu.
func (l *Lab) trimLocked() {
	for len(l.order) > l.limits.HistorySize {
		oldest := l.order[0]
		l.order = l.order[1:]
		delete(l.runs, oldest)
	}
}

// execute builds the experiment from primitives and runs it.
func (l *Lab) execute(run *Run, s runSpec) {
	defer func() {
		l.mu.Lock()
		l.running = false
		l.mu.Unlock()
	}()

	e := exp.Experiment{
		Name:            fmt.Sprintf("%s via MCP (blast radius %.2f)", s.Fault, s.BlastRadius),
		BlastRadius:     s.BlastRadius,
		Duration:        time.Duration(s.DurationS * float64(time.Second)),
		RecoveryTimeout: time.Duration(s.RecoveryTimeout * float64(time.Second)),
	}

	if s.Fault == "capacity" {
		// Resource fault: vary the pool's size and watch the backlog.
		const full = 6
		l.syncPool.SetSize(full)
		go func() {
			for i := 0; i < 300; i++ {
				l.syncPool.Submit()
				time.Sleep(5 * time.Millisecond)
			}
		}()
		capFault := fault.Capacity("capacity", func(n int) int { return l.syncPool.SetSize(n) }, s.CapacityTo)
		e.SteadyState = func() bool { return l.syncPool.Stats().Queued < 50 }
		e.Inject = capFault.Apply
	} else {
		// Call-level fault: swap the toggle's primitive for the one
		// asked for, then the experiment is just flipping it on.
		l.backendFault = fault.NewToggle(l.primitive(s))
		const budget = 100 * time.Millisecond
		probe := func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 2*budget)
			defer cancel()
			start := time.Now()
			err := l.backendFault.Inject(ctx, func(context.Context) error { return nil })
			return err == nil && time.Since(start) < budget
		}
		e.SteadyState = probe
		e.Inject = l.backendFault.On
	}

	v := exp.Run(e, 200*time.Millisecond)

	now := time.Now().UTC()
	l.mu.Lock()
	run.Verdict = &v
	run.Status = StatusDone
	run.FinishedAt = &now
	l.mu.Unlock()
}

// primitive builds the call-level fault the spec asks for, with the
// requested blast radius as its sampler.
func (l *Lab) primitive(s runSpec) fault.Primitive {
	sampler := fault.Rate(s.BlastRadius)
	switch s.Fault {
	case "latency":
		ms := s.LatencyMS
		if ms <= 0 {
			ms = 250
		}
		return fault.Latency(time.Duration(ms)*time.Millisecond, sampler)
	case "error":
		return fault.Error(nil, sampler)
	case "timeout":
		return fault.Timeout(2*time.Second, sampler)
	case "cpuburn":
		return fault.CPUBurn(150*time.Millisecond, sampler)
	default: // blackhole
		return fault.Blackhole(sampler)
	}
}

// GetVerdict returns one run's current state.
func (l *Lab) GetVerdict(args json.RawMessage) (any, error) {
	var p struct {
		ID string `json:"experiment_id"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &p); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
	}
	if p.ID == "" {
		return nil, fmt.Errorf("experiment_id is required")
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	run, ok := l.runs[p.ID]
	if !ok {
		return nil, fmt.Errorf("unknown experiment_id %q; it may have aged out of the %d-run history", p.ID, l.limits.HistorySize)
	}
	// Copy under the lock: the executing goroutine mutates the run.
	out := *run
	return out, nil
}

// ListExperiments returns the run history, newest first.
func (l *Lab) ListExperiments(args json.RawMessage) (any, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]Run, 0, len(l.order))
	for _, id := range l.order {
		out = append(out, *l.runs[id])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return map[string]any{
		"experiments": out,
		"count":       len(out),
		"limits": map[string]any{
			"max_blast_radius":       l.limits.MaxBlastRadius,
			"max_duration_s":         l.limits.MaxDuration.Seconds(),
			"max_recovery_s":         l.limits.MaxRecovery.Seconds(),
			"history_size":           l.limits.HistorySize,
			"concurrent_experiments": 1,
		},
	}, nil
}

// Register adds the lab's three tools to a server.
func (l *Lab) Register(s *Server) {
	s.Register(Tool{
		Name:  "run_experiment",
		Title: "Run a chaos experiment",
		Description: "Start one fault-injection experiment against the flowgate lab and return its id immediately. " +
			"The experiment declares a steady state, injects exactly one fault, samples the steady state for the " +
			"duration, then always rolls the fault back and checks recovery. Experiments run one at a time and take " +
			"several seconds; poll get_verdict for the result. Server-enforced limits on blast radius and duration " +
			"apply and cannot be raised by the caller — a request that exceeds them is refused, not clamped.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"fault": map[string]any{
					"type":        "string",
					"enum":        supportedFaults,
					"description": "Which fault to inject. latency, error, timeout, cpuburn and blackhole are call-level faults applied to a fraction of calls; capacity is a resource fault that removes worker capacity and perturbs no individual call.",
				},
				"blast_radius": map[string]any{
					"type":        "number",
					"description": "Fraction of calls the fault applies to, in (0,1]. Smaller is safer but noisier: at 0.01 you need roughly 100x the samples of 0.5 for the same confidence.",
				},
				"duration_s":         map[string]any{"type": "number", "description": "How long the fault stays injected, in seconds."},
				"recovery_timeout_s": map[string]any{"type": "number", "description": "How long after rollback the steady state may take to return. Defaults to 5."},
				"latency_ms":         map[string]any{"type": "integer", "description": "Delay for the latency fault, in milliseconds. Defaults to 250."},
				"capacity_to":        map[string]any{"type": "integer", "description": "Worker count to drop to, for the capacity fault. Required when fault is capacity."},
			},
			"required": []string{"fault", "blast_radius", "duration_s"},
		},
		OutputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"experiment_id": map[string]any{"type": "string"},
				"status":        map[string]any{"type": "string"},
				"fault":         map[string]any{"type": "string"},
				"blast_radius":  map[string]any{"type": "number"},
				"note":          map[string]any{"type": "string"},
			},
			"required": []string{"experiment_id", "status"},
		},
		Handler: func(_ context.Context, args json.RawMessage) (any, error) { return l.RunExperiment(args) },
	})

	s.Register(Tool{
		Name:  "get_verdict",
		Title: "Get an experiment's verdict",
		Description: "Fetch one experiment's current state by id. While status is \"running\" there is no verdict yet; " +
			"poll again. When status is \"done\" the verdict reports whether the baseline held before injection " +
			"(baseline_ok), whether the steady state survived the fault (held), and whether it returned within the " +
			"recovery window after rollback (recovered). A held=false verdict is a successful experiment that found " +
			"something, not a failed run.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"experiment_id": map[string]any{"type": "string", "description": "Id returned by run_experiment."}},
			"required":   []string{"experiment_id"},
		},
		Handler: func(_ context.Context, args json.RawMessage) (any, error) { return l.GetVerdict(args) },
	})

	s.Register(Tool{
		Name:        "list_experiments",
		Title:       "List recent experiments",
		Description: "Return the recent experiment history, newest first, along with the server's enforced limits. Useful for seeing what has already been tried before planning the next experiment.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		Handler:     func(_ context.Context, args json.RawMessage) (any, error) { return l.ListExperiments(args) },
	})
}
