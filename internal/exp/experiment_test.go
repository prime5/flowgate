package exp

import (
	"testing"
	"time"
)

func TestRunHeldWhenFaultIsBenign(t *testing.T) {
	healthy := true
	e := Experiment{
		Name:        "benign fault",
		SteadyState: func() bool { return healthy },
		Inject: func() func() {
			return func() {} // no-op fault, no-op rollback
		},
		BlastRadius: 0.5,
		Duration:    50 * time.Millisecond,
	}
	v := Run(e, 5*time.Millisecond)
	if !v.BaselineOK || !v.Held || !v.Recovered {
		t.Fatalf("verdict = %+v, want all true", v)
	}
	if v.BlastRadius != 0.5 {
		t.Fatalf("blast radius = %v, want 0.5", v.BlastRadius)
	}
}

func TestRunDetectsViolationAndRollsBack(t *testing.T) {
	rolledBack := false
	violated := false
	e := Experiment{
		Name:        "breaking fault",
		SteadyState: func() bool { return !violated },
		Inject: func() func() {
			violated = true
			return func() { violated = false; rolledBack = true }
		},
		BlastRadius: 0.25,
		Duration:    time.Second,
	}
	v := Run(e, 5*time.Millisecond)
	if v.Held {
		t.Fatalf("verdict = %+v, want Held=false", v)
	}
	if !rolledBack {
		t.Fatalf("rollback did not run")
	}
	if !v.Recovered {
		t.Fatalf("verdict = %+v, want Recovered=true after rollback", v)
	}
}

func TestRunWaitsForRecovery(t *testing.T) {
	// The steady state only returns some time after rollback, like a
	// backlog draining once replicas are restored.
	var healthyAt time.Time
	healthy := true
	e := Experiment{
		Name: "slow recovery",
		SteadyState: func() bool {
			return healthy || (!healthyAt.IsZero() && time.Now().After(healthyAt))
		},
		Inject: func() func() {
			healthy = false
			return func() { healthyAt = time.Now().Add(30 * time.Millisecond) }
		},
		BlastRadius:     0.5,
		Duration:        time.Second,
		RecoveryTimeout: time.Second,
	}
	v := Run(e, 5*time.Millisecond)
	if v.Held || !v.Recovered {
		t.Fatalf("verdict = %+v, want Held=false Recovered=true", v)
	}

	// Without a recovery window the same system reads as not recovered.
	healthy, healthyAt = true, time.Time{}
	e.RecoveryTimeout = 0
	if v := Run(e, 5*time.Millisecond); v.Recovered {
		t.Fatalf("verdict = %+v, want Recovered=false with no recovery window", v)
	}
}

func TestRunAbortsOnBadBaseline(t *testing.T) {
	injected := false
	e := Experiment{
		Name:        "bad baseline",
		SteadyState: func() bool { return false },
		Inject: func() func() {
			injected = true
			return func() {}
		},
		BlastRadius: 1,
		Duration:    time.Millisecond,
	}
	v := Run(e, time.Millisecond)
	if injected {
		t.Fatalf("fault was injected despite a failed baseline")
	}
	if v.BaselineOK {
		t.Fatalf("verdict = %+v, want BaselineOK=false", v)
	}
}
