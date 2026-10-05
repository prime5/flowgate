package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// session drives a server the way a client does: write JSON-RPC lines
// in, read JSON-RPC lines out. Tests exercise the real transport
// rather than calling handlers directly, because the transport is
// where most protocol bugs live.
type session struct {
	t   *testing.T
	srv *Server
}

func newSession(t *testing.T) *session {
	t.Helper()
	s := NewServer("flowgate-test", "0.0.1", "test instructions")
	// Silence diagnostics; the real server logs to stderr.
	s.logf = func(string, ...any) {}
	NewLab(testLimits()).Register(s)
	return &session{t: t, srv: s}
}

func testLimits() Limits {
	return Limits{
		MaxBlastRadius: 0.5,
		MaxDuration:    5 * time.Second,
		MaxRecovery:    5 * time.Second,
		HistorySize:    3,
	}
}

// exchange feeds the given messages through Serve and returns the
// decoded replies.
func (s *session) exchange(msgs ...string) []map[string]any {
	s.t.Helper()
	in := strings.NewReader(strings.Join(msgs, "\n") + "\n")
	var out strings.Builder
	if err := s.srv.Serve(context.Background(), in, &out); err != nil {
		s.t.Fatalf("Serve: %v", err)
	}
	var replies []map[string]any
	dec := json.NewDecoder(strings.NewReader(out.String()))
	for dec.More() {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			s.t.Fatalf("decoding reply: %v (stream: %s)", err, out.String())
		}
		replies = append(replies, m)
	}
	return replies
}

const initMsg = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`

// --- lifecycle ----------------------------------------------------------

func TestInitializeNegotiatesVersionAndDeclaresTools(t *testing.T) {
	s := newSession(t)
	replies := s.exchange(initMsg)
	if len(replies) != 1 {
		t.Fatalf("got %d replies, want 1", len(replies))
	}
	r := replies[0]["result"].(map[string]any)

	if got := r["protocolVersion"]; got != ProtocolVersion {
		t.Fatalf("protocolVersion = %v, want %v", got, ProtocolVersion)
	}
	caps := r["capabilities"].(map[string]any)
	if _, ok := caps["tools"]; !ok {
		t.Fatal("server must declare the tools capability")
	}
	info := r["serverInfo"].(map[string]any)
	if info["name"] != "flowgate-test" {
		t.Fatalf("serverInfo.name = %v", info["name"])
	}
	if r["instructions"] == "" {
		t.Fatal("instructions should be sent; it is the only place to tell a model how to use the server")
	}
}

func TestUnknownClientVersionStillGetsOurs(t *testing.T) {
	s := newSession(t)
	replies := s.exchange(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"1999-01-01","capabilities":{}}}`)
	r := replies[0]["result"].(map[string]any)
	if got := r["protocolVersion"]; got != ProtocolVersion {
		t.Fatalf("protocolVersion = %v, want the server's own %v", got, ProtocolVersion)
	}
}

func TestNotificationsGetNoReply(t *testing.T) {
	s := newSession(t)
	// Three notifications: one known, one unknown, one that would be
	// an error if it were a request. None may produce output.
	replies := s.exchange(
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","method":"notifications/something_unknown"}`,
		`{"jsonrpc":"2.0","method":"totally_made_up"}`,
	)
	if len(replies) != 0 {
		t.Fatalf("got %d replies to notifications, want 0: %+v", len(replies), replies)
	}
}

func TestPingAndUnknownMethod(t *testing.T) {
	s := newSession(t)
	replies := s.exchange(
		`{"jsonrpc":"2.0","id":5,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":6,"method":"does/not/exist"}`,
	)
	if len(replies) != 2 {
		t.Fatalf("got %d replies, want 2", len(replies))
	}
	if _, ok := replies[0]["result"]; !ok {
		t.Fatalf("ping should succeed, got %+v", replies[0])
	}
	e := replies[1]["error"].(map[string]any)
	if int(e["code"].(float64)) != CodeMethodNotFound {
		t.Fatalf("unknown method code = %v, want %d", e["code"], CodeMethodNotFound)
	}
}

func TestBadJSONRPCVersionRejected(t *testing.T) {
	s := newSession(t)
	replies := s.exchange(`{"jsonrpc":"1.0","id":9,"method":"ping"}`)
	e := replies[0]["error"].(map[string]any)
	if int(e["code"].(float64)) != CodeInvalidRequest {
		t.Fatalf("code = %v, want %d", e["code"], CodeInvalidRequest)
	}
}

// --- tools/list ---------------------------------------------------------

func TestToolsListShapeAndOrder(t *testing.T) {
	s := newSession(t)
	replies := s.exchange(initMsg, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	tools := replies[1]["result"].(map[string]any)["tools"].([]any)

	want := []string{"run_experiment", "get_verdict", "list_experiments"}
	if len(tools) != len(want) {
		t.Fatalf("got %d tools, want %d", len(tools), len(want))
	}
	for i, name := range want {
		tool := tools[i].(map[string]any)
		if tool["name"] != name {
			t.Fatalf("tool %d = %v, want %v (registration order must be stable)", i, tool["name"], name)
		}
		schema, ok := tool["inputSchema"].(map[string]any)
		if !ok {
			t.Fatalf("%s has no inputSchema", name)
		}
		if schema["type"] != "object" {
			t.Fatalf("%s inputSchema.type = %v, want object", name, schema["type"])
		}
		if tool["description"] == "" {
			t.Fatalf("%s has no description; the model has nothing to go on", name)
		}
	}
}

// --- guardrails ---------------------------------------------------------

// callTool runs one tools/call and returns its structuredContent and
// isError flag.
func (s *session) callTool(name string, args map[string]any) (map[string]any, bool) {
	s.t.Helper()
	argBytes, _ := json.Marshal(args)
	msg := `{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"` + name + `","arguments":` + string(argBytes) + `}}`
	replies := s.exchange(initMsg, msg)
	res, ok := replies[1]["result"].(map[string]any)
	if !ok {
		s.t.Fatalf("expected a result, got %+v", replies[1])
	}
	sc, _ := res["structuredContent"].(map[string]any)
	isErr, _ := res["isError"].(bool)
	return sc, isErr
}

func TestGuardrailsRefuseRatherThanClamp(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		wantWord string
	}{
		{
			name:     "blast radius over cap",
			args:     map[string]any{"fault": "latency", "blast_radius": 1.0, "duration_s": 1},
			wantWord: "exceeds the policy cap",
		},
		{
			name:     "duration over cap",
			args:     map[string]any{"fault": "latency", "blast_radius": 0.2, "duration_s": 600},
			wantWord: "exceeds the policy cap",
		},
		{
			name:     "recovery over cap",
			args:     map[string]any{"fault": "latency", "blast_radius": 0.2, "duration_s": 1, "recovery_timeout_s": 600},
			wantWord: "exceeds the policy cap",
		},
		{
			name:     "unknown fault",
			args:     map[string]any{"fault": "delete_prod", "blast_radius": 0.1, "duration_s": 1},
			wantWord: "unknown fault",
		},
		{
			name:     "zero blast radius injects nothing",
			args:     map[string]any{"fault": "latency", "blast_radius": 0, "duration_s": 1},
			wantWord: "must be > 0",
		},
		{
			name:     "capacity needs a target",
			args:     map[string]any{"fault": "capacity", "blast_radius": 0.5, "duration_s": 1},
			wantWord: "capacity_to",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSession(t)
			sc, isErr := s.callTool("run_experiment", tc.args)
			if !isErr {
				t.Fatalf("expected refusal, got success: %+v", sc)
			}
			msg, _ := sc["error"].(string)
			if !strings.Contains(msg, tc.wantWord) {
				t.Fatalf("error %q should mention %q so the model can correct itself", msg, tc.wantWord)
			}
			// The refusal must not have started anything.
			if _, started := sc["experiment_id"]; started {
				t.Fatal("a refused request must not create an experiment")
			}
		})
	}
}

func TestBlastRadiusAtTheCapIsAccepted(t *testing.T) {
	// The cap is inclusive: 0.5 against a 0.5 limit must run.
	s := newSession(t)
	sc, isErr := s.callTool("run_experiment", map[string]any{
		"fault": "blackhole", "blast_radius": 0.5, "duration_s": 1, "recovery_timeout_s": 1,
	})
	if isErr {
		t.Fatalf("blast radius exactly at the cap should be accepted, got %v", sc["error"])
	}
	if sc["experiment_id"] == nil {
		t.Fatalf("no experiment_id in %+v", sc)
	}
}

func TestUnknownToolIsProtocolErrorNotToolError(t *testing.T) {
	// The spec draws this line: a tool that does not exist is a
	// protocol fault, while a tool that refuses is a result.
	s := newSession(t)
	replies := s.exchange(initMsg, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"rm_rf","arguments":{}}}`)
	e, ok := replies[1]["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected a protocol error, got %+v", replies[1])
	}
	if int(e["code"].(float64)) != CodeMethodNotFound {
		t.Fatalf("code = %v, want %d", e["code"], CodeMethodNotFound)
	}
}

// --- lifecycle of a run -------------------------------------------------

func TestExperimentRunsToVerdict(t *testing.T) {
	lab := NewLab(testLimits())

	out, err := lab.RunExperiment(json.RawMessage(`{"fault":"latency","blast_radius":0.5,"duration_s":1,"recovery_timeout_s":2,"latency_ms":300}`))
	if err != nil {
		t.Fatalf("RunExperiment: %v", err)
	}
	id := out.(map[string]any)["experiment_id"].(string)

	// A second experiment while the first runs must be refused.
	if _, err := lab.RunExperiment(json.RawMessage(`{"fault":"error","blast_radius":0.1,"duration_s":1}`)); err == nil {
		t.Fatal("expected the concurrency guard to refuse a second experiment")
	}

	deadline := time.Now().Add(20 * time.Second)
	var run Run
	for time.Now().Before(deadline) {
		v, err := lab.GetVerdict(json.RawMessage(`{"experiment_id":"` + id + `"}`))
		if err != nil {
			t.Fatalf("GetVerdict: %v", err)
		}
		run = v.(Run)
		if run.Status == StatusDone {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if run.Status != StatusDone {
		t.Fatalf("experiment never finished; status = %q", run.Status)
	}
	if run.Verdict == nil {
		t.Fatal("a done experiment must carry a verdict")
	}
	if !run.Verdict.BaselineOK {
		t.Fatal("baseline should hold before injection in a quiet lab")
	}
	// A 300ms delay against a 100ms budget must break the hypothesis,
	// and removing it must restore the steady state.
	if run.Verdict.Held {
		t.Fatal("a 300ms injected delay against a 100ms budget should violate the steady state")
	}
	if !run.Verdict.Recovered {
		t.Fatal("the steady state must return once the fault is rolled back")
	}
	if run.FinishedAt == nil {
		t.Fatal("a done experiment must record FinishedAt")
	}
}

func TestGetVerdictUnknownID(t *testing.T) {
	lab := NewLab(testLimits())
	if _, err := lab.GetVerdict(json.RawMessage(`{"experiment_id":"exp-999"}`)); err == nil {
		t.Fatal("expected an error for an unknown id")
	}
	if _, err := lab.GetVerdict(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error when experiment_id is missing")
	}
}

func TestHistoryIsBounded(t *testing.T) {
	// HistorySize is 3 in testLimits; a long agent session must not
	// grow the registry without end.
	lab := NewLab(testLimits())
	for i := 0; i < 6; i++ {
		out, err := lab.RunExperiment(json.RawMessage(`{"fault":"blackhole","blast_radius":0.5,"duration_s":1,"recovery_timeout_s":1}`))
		if err != nil {
			// Concurrency guard: wait for the previous one.
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				lab.mu.Lock()
				busy := lab.running
				lab.mu.Unlock()
				if !busy {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			out, err = lab.RunExperiment(json.RawMessage(`{"fault":"blackhole","blast_radius":0.5,"duration_s":1,"recovery_timeout_s":1}`))
			if err != nil {
				t.Fatalf("run %d: %v", i, err)
			}
		}
		_ = out
	}

	listed, err := lab.ListExperiments(nil)
	if err != nil {
		t.Fatalf("ListExperiments: %v", err)
	}
	m := listed.(map[string]any)
	if n := m["count"].(int); n > testLimits().HistorySize {
		t.Fatalf("history holds %d runs, want at most %d", n, testLimits().HistorySize)
	}
}

func TestListExperimentsReportsLimits(t *testing.T) {
	// The model needs to see the policy to plan within it, rather
	// than discovering each cap by being refused.
	lab := NewLab(testLimits())
	listed, err := lab.ListExperiments(nil)
	if err != nil {
		t.Fatalf("ListExperiments: %v", err)
	}
	limits := listed.(map[string]any)["limits"].(map[string]any)
	if limits["max_blast_radius"] != testLimits().MaxBlastRadius {
		t.Fatalf("max_blast_radius = %v", limits["max_blast_radius"])
	}
	if limits["concurrent_experiments"] != 1 {
		t.Fatalf("concurrent_experiments = %v, want 1", limits["concurrent_experiments"])
	}
}

// --- transport edge cases -----------------------------------------------

func TestMalformedJSONReportsParseError(t *testing.T) {
	s := NewServer("t", "1", "")
	s.logf = func(string, ...any) {}
	in := strings.NewReader("{not json at all}\n")
	var out strings.Builder
	if err := s.Serve(context.Background(), in, &out); err == nil {
		t.Fatal("expected Serve to return the parse error")
	}
	if !strings.Contains(out.String(), "-32700") {
		t.Fatalf("reply should carry the parse-error code: %s", out.String())
	}
}

func TestCleanEOFIsNotAnError(t *testing.T) {
	// Closing the server's stdin is how an MCP client shuts it down.
	s := newSession(t)
	if err := s.srv.Serve(context.Background(), strings.NewReader(""), &strings.Builder{}); err != nil {
		t.Fatalf("clean EOF should return nil, got %v", err)
	}
}

func TestToolResultCarriesBothForms(t *testing.T) {
	// Structured content for programs, the same JSON in a text block
	// for clients that do not read it.
	r := toolResult(map[string]any{"k": "v"}, false)
	if _, ok := r["structuredContent"]; !ok {
		t.Fatal("missing structuredContent")
	}
	content := r["content"].([]map[string]any)
	if content[0]["type"] != "text" {
		t.Fatalf("content[0].type = %v, want text", content[0]["type"])
	}
	if !strings.Contains(content[0]["text"].(string), `"k"`) {
		t.Fatalf("text block should carry the serialized result, got %v", content[0]["text"])
	}
	if r["isError"] != false {
		t.Fatal("isError should be present and false on success")
	}
}
