# agent — the other half of the agentic story

`internal/mcp` exposes flowgate's experiment framework as agent-callable
tools. This directory is what calls them.

Python rather than Go, deliberately: the MCP client here speaks the
protocol, not flowgate, so it works against any MCP server over stdio.
A Go client welded to our own server would not have been reusable.

## What's here

```
flowgate_agent/mcp_client.py    MCP over stdio. No third-party dependencies.
tests/fake_server.py            A fake MCP server, so tests need no Go build.
tests/test_mcp_client.py        15 unit tests + 1 integration test.
```

Runtime dependencies: none. `pip install -r requirements-dev.txt` gets
pytest, and that is only for the tests.

## The distinction the client is built around

flowgate's server says "no" in two different ways, and they mean
opposite things:

| | Wire form | Meaning | Client |
|---|---|---|---|
| Unknown tool | JSON-RPC `error` `-32601` | the caller is confused — no result exists | raises `MCPProtocolError` |
| Refused experiment | result with `isError: true` | the tool ran and policy said no, with a reason | returns `ToolResult(ok=False)` |

An agent should change its *approach* in the first case and its
*request* in the second. Collapsing both into one exception throws that
away, so the client keeps them apart. `FAQ.md` has the server-side half
of this argument.

## Running it

```bash
go build -o /tmp/flowgate-mcp ./cmd/flowgate-mcp     # from the repo root
cd agent
pip install -r requirements-dev.txt

python3 -m pytest tests/ -q                          # unit tests, no Go needed
FLOWGATE_MCP_BIN=/tmp/flowgate-mcp python3 -m pytest tests/ -q   # + integration
```

Minimal use:

```python
from flowgate_agent.mcp_client import MCPClient

with MCPClient(["/tmp/flowgate-mcp"]) as c:
    c.initialize()
    result = c.call_tool("run_experiment", {
        "fault": "latency", "blast_radius": 0.2, "duration_s": 2,
    })
    print(result.ok, result.data.get("experiment_id") or result.error)
```

## Two design notes

**Both pipes are read on threads.** stderr because an unread pipe fills
its buffer and the server blocks on its next log line — which looks
exactly like a hang mid-experiment. stdout because a blocking
`readline()` cannot be given a deadline, so a server that accepts a
request and never answers would pin the caller forever. The caller polls
`get_verdict` in a loop, which is precisely where that would bite. A
test asserts the timeout fires; it caught this bug rather than
documenting it after the fact.

**Replies are matched by id, not by arrival order.** This server answers
in order, so the matching is redundant against it today. It is here
because a client that assumes ordering breaks silently against one that
does not, and silent is the expensive kind of breakage.

## Not done yet

The executor, planner, judge and gate. The planner is the interesting
one: the design is that it reads what changed and what the last load
test showed, then picks the single experiment that most reduces
uncertainty about that change — a release gate rather than a fault
sweep. A deterministic round-robin planner comes first, as the control
arm the model-driven one has to beat.
