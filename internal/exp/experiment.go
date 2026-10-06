// Package exp is flowgate's fault-injection experiment framework. It
// implements the vocabulary the Load and Fault Environments role
// asks for — steady-state hypotheses, blast-radius controls, fault
// primitives — in one small file, so every experiment in this repo
// follows the same discipline: define steady state, inject one
// fault, observe, always roll back.
package exp

import "time"

// Probe samples one boolean: "is the system in its steady state
// right now?" Returning false means the steady state is violated,
// not that the experiment harness is broken.
type Probe func() bool

// Fault injects a fault and returns its rollback. Faults must be
// reversible: an experiment without a rollback is not an
// experiment, it's an outage with a write-up.
type Fault func() (rollback func())

// Experiment is one chaos experiment: a hypothesis about steady
// state, exactly one fault, and a declared cap on how much of the
// system the fault may touch.
type Experiment struct {
	Name string
	// SteadyState is sampled before, during, and after the fault.
	SteadyState Probe
	// Inject applies the fault; its rollback always runs.
	Inject Fault
	// BlastRadius is the declared cap on the fault's reach as a
	// fraction in (0,1], e.g. 0.5 means at most half the replicas
	// may be affected. The fault honors it; the runner records it
	// as evidence.
	BlastRadius float64
	// Duration is how long the fault stays injected.
	Duration time.Duration
	// RecoveryTimeout is how long after rollback the steady state
	// may take to return (a backlog has to drain). Zero means it
	// must hold immediately.
	RecoveryTimeout time.Duration
}

// Verdict is the outcome of one experiment run.
type Verdict struct {
	Name        string  `json:"name"`
	Held        bool    `json:"held"`        // steady state held for the whole fault window
	BaselineOK  bool    `json:"baseline_ok"` // steady state held before injection
	Recovered   bool    `json:"recovered"`   // steady state returned within RecoveryTimeout of rollback
	BlastRadius float64 `json:"blast_radius"`
	Detail      string  `json:"detail"`
}

// Run executes the experiment: verify the baseline, inject, sample
// through the fault window, roll back, verify recovery. The
// rollback runs even if sampling panics.
func Run(e Experiment, sampleInterval time.Duration) (v Verdict) {
	v = Verdict{Name: e.Name, BlastRadius: e.BlastRadius}

	v.BaselineOK = e.SteadyState()
	if !v.BaselineOK {
		v.Detail = "baseline steady state not met; aborting before injection"
		return v
	}

	rollback := e.Inject()
	// v is a named result, so the recovery check below lands in the
	// verdict the caller sees.
	defer func() {
		rollback()
		v.Recovered = waitFor(e.SteadyState, e.RecoveryTimeout, sampleInterval)
	}()

	held := true
	deadline := time.Now().Add(e.Duration)
	for time.Now().Before(deadline) {
		if !e.SteadyState() {
			held = false
			break
		}
		time.Sleep(sampleInterval)
	}

	v.Held = held
	if held {
		v.Detail = "steady state held for the full fault window"
	} else {
		v.Detail = "steady state violated during fault window"
	}
	return v
}

// waitFor polls p until it holds or timeout elapses; it always
// samples at least once.
func waitFor(p Probe, timeout, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if p() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}
