// Command flowgate-mcp exposes flowgate's chaos-experiment framework
// as MCP tools over stdio, so an agent can run fault-injection
// experiments and read their verdicts.
//
// Run it directly to speak the protocol by hand:
//
//	go run ./cmd/flowgate-mcp
//
// then paste JSON-RPC messages on stdin, one per line. More usually it
// is launched by an MCP client as a subprocess.
//
// Three tools: run_experiment, get_verdict, list_experiments. The
// policy limits an agent cannot exceed are set here and enforced in
// internal/mcp, not in any prompt.
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/prime5/flowgate/internal/mcp"
)

const instructions = `flowgate is a reliability gateway with a chaos-experiment framework.

Every experiment has the same four parts: a steady-state hypothesis, exactly one
injected fault, a declared blast radius, and a guaranteed rollback. Start one with
run_experiment, then poll get_verdict until status is "done".

Faults come in two shapes. latency, error, timeout, cpuburn and blackhole are
call-level: they wrap a unit of work and apply to the fraction of calls given by
blast_radius. capacity is resource-level: it removes worker capacity and perturbs no
individual call, which models the most common real outage there is -- service rate
falling below arrival rate until the backlog grows without bound.

Reading a verdict: baseline_ok false means the system was already unhealthy and the
experiment aborted before injecting anything; trust nothing else in that verdict.
held false means the fault broke the steady state, which is usually the point of
running it rather than a failure. recovered false is the result that should worry
you: the system did not return to its steady state within the recovery window after
the fault was removed.

Blast radius is a safety control and a statistical one. A small radius is safer but
noisier: relative error scales as 1/sqrt(n*p), so an experiment at 0.01 needs roughly
100 times the samples of one at 0.5 to reach the same confidence. Size the experiment
for the radius rather than only capping the radius.

The server enforces its own limits on blast radius and duration. A request that
exceeds them is refused with the limit named, not silently reduced.`

func main() {
	// Diagnostics go to stderr. On stdio transport stdout carries
	// protocol messages and nothing else; one stray write there
	// corrupts the stream.
	log.SetOutput(os.Stderr)
	log.SetPrefix("flowgate-mcp: ")
	log.SetFlags(0)

	limits := mcp.DefaultLimits()
	// The caps are operator policy, so they are set from the
	// environment at launch — by whoever runs the server, never by
	// anything the model can reach.
	if v := os.Getenv("MCP_MAX_BLAST_RADIUS"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 || f > 1 {
			log.Fatalf("MCP_MAX_BLAST_RADIUS must be a number in (0,1], got %q", v)
		}
		limits.MaxBlastRadius = f
	}
	if v := os.Getenv("MCP_MAX_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			log.Fatalf("MCP_MAX_DURATION must be a positive Go duration (e.g. 30s), got %q", v)
		}
		limits.MaxDuration = d
	}

	srv := mcp.NewServer("flowgate", "0.1.0", instructions)
	mcp.NewLab(limits).Register(srv)

	log.Printf("ready on stdio (protocol %s, max blast radius %.2f, max duration %s)",
		mcp.ProtocolVersion, limits.MaxBlastRadius, limits.MaxDuration)

	if err := srv.Serve(context.Background(), os.Stdin, os.Stdout); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
