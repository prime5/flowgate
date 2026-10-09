package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prime5/flowgate/internal/breaker"
	"github.com/prime5/flowgate/internal/mcp"
	"github.com/prime5/flowgate/internal/runner"
	"github.com/prime5/flowgate/internal/shedder"
	"github.com/prime5/flowgate/internal/slack"
)

// slackDeps is what the Slack integration reads from the running gateway.
type slackDeps struct {
	Breaker     *breaker.Breaker
	Shedder     *shedder.Shedder
	LimiterMode string // "memory" or "redis"
	// FailOpenCount is nil unless the limiter is Redis-backed.
	FailOpenCount func() uint64
	// LabOn is FLOWGATE_LAB=1: the same switch that gates /scale and
	// /experiments/*. "/flowgate run" starts fault injection, so it
	// follows the same rule: off unless the operator turned the lab on.
	LabOn     bool
	TracingOn bool
}

// setupSlack mounts the slash-command endpoint and starts the alert
// watcher, each only if its environment is present:
//
//	SLACK_SIGNING_SECRET     mounts POST /slack/command (status, verdict)
//	SLACK_ALLOWED_USER_IDS   comma-separated Slack user IDs allowed to "run"
//	SLACK_WEBHOOK_URL        enables breaker / limiter-fail-open alerts
//
// With none of them set this is a no-op and the gateway is unchanged.
func setupSlack(ctx context.Context, mux *http.ServeMux, d slackDeps) {
	if secret := os.Getenv("SLACK_SIGNING_SECRET"); secret != "" {
		verifier, err := slack.NewVerifier(secret)
		if err != nil {
			log.Fatalf("slack: %v", err)
		}

		// The commands run against an MCP lab inside this process, the
		// same one cmd/flowgate-mcp exposes over stdio. It injects faults
		// into itself, not into the live /work path, so a Slack user
		// cannot degrade the gateway they are asking about.
		server := mcp.NewServer("flowgate-slack", "0.1.0", "Slack front end for the flowgate chaos lab.")
		mcp.NewLab(mcp.DefaultLimits()).Register(server)
		rn := runner.New(runner.NewMemoryStore(), server)

		allowed := map[string]bool{}
		for _, id := range strings.Split(os.Getenv("SLACK_ALLOWED_USER_IDS"), ",") {
			if id = strings.TrimSpace(id); id != "" {
				allowed[id] = true
			}
		}
		runEnabled := d.LabOn && len(allowed) > 0

		h, err := slack.NewHandler(slack.Config{
			Verifier: verifier,
			Tools:    slack.RunnerCaller{R: rn},
			Status: func() slack.Status {
				s := slack.Status{
					Breaker:     d.Breaker.State().String(),
					InFlight:    d.Shedder.InFlight(),
					MaxInFlight: d.Shedder.Capacity(),
					LimiterMode: d.LimiterMode,
					Tracing:     d.TracingOn,
				}
				if d.FailOpenCount != nil {
					s.FailOpen = d.FailOpenCount()
				}
				return s
			},
			RunEnabled:      runEnabled,
			AllowedRunUsers: allowed,
			Logf:            log.Printf,
		})
		if err != nil {
			log.Fatalf("slack: %v", err)
		}
		mux.Handle("/slack/command", h)
		log.Printf("slack: POST /slack/command mounted (run enabled: %v, %d allowed user(s))", runEnabled, len(allowed))
		if len(allowed) > 0 && !d.LabOn {
			log.Printf("slack: SLACK_ALLOWED_USER_IDS is set but FLOWGATE_LAB is not 1, so \"run\" stays disabled")
		}
	}

	if webhook := os.Getenv("SLACK_WEBHOOK_URL"); webhook != "" {
		if err := slack.ValidateWebhookURL(webhook); err != nil {
			log.Fatalf("slack: SLACK_WEBHOOK_URL: %v", err)
		}
		w := slack.NewWatcher(slack.NewAlerter(webhook, log.Printf), slack.Probe{
			BreakerState:  func() string { return d.Breaker.State().String() },
			FailOpenCount: d.FailOpenCount,
		})
		go w.Run(ctx, time.Second)
		log.Printf("slack: alerts enabled (breaker open/recovered, limiter fail-open)")
	}
}
