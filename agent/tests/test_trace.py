"""Tests for trace context propagation, agent -> server.

The client generates a W3C traceparent per request and sends it in
``params._meta``. The fake server echoes each received traceparent to
stderr, so these tests assert on what the client actually put on the
wire — no Go toolchain, no real server.
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from flowgate_agent.mcp_client import MCPClient  # noqa: E402

FAKE = [sys.executable, str(Path(__file__).parent / "fake_server.py")]

TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-01$")


def fake() -> MCPClient:
    return MCPClient(FAKE, timeout=10.0)


def traceparents(c: MCPClient, want: int, deadline_s: float = 5.0) -> list[str]:
    """Traceparents the fake server has logged so far, oldest first.

    stderr is drained on a thread, so a line flushed by the server just
    before its reply may not be visible the instant the reply arrives.
    Poll briefly rather than assuming it is there.
    """
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        found = [
            line.removeprefix("fake: traceparent=")
            for line in c.stderr_tail(50)
            if line.startswith("fake: traceparent=")
        ]
        if len(found) >= want:
            return found
        time.sleep(0.05)
    raise AssertionError(f"want {want} traceparents, saw {len(found)}: {found}")


def test_traceparent_is_valid_w3c():
    with fake() as c:
        c.ping()
        (tp,) = traceparents(c, 1)
    assert TRACEPARENT.match(tp), f"not a W3C traceparent: {tp!r}"


def test_one_trace_per_session_distinct_spans_per_request():
    """A run_experiment and its get_verdict polls must land in one trace,
    each call as its own span — that is the whole point of the session
    trace id."""
    with fake() as c:
        c.ping()
        c.list_tools()
        c.call_tool("get_verdict", {"experiment_id": "exp-1"})
        tps = traceparents(c, 3)
    parsed = [TRACEPARENT.match(tp) for tp in tps]
    assert all(parsed), f"malformed traceparent in {tps!r}"
    trace_ids = {m.group(1) for m in parsed if m}
    span_ids = [m.group(1 + 1) for m in parsed if m]
    assert trace_ids and len(trace_ids) == 1, f"one session, one trace id: {tps!r}"
    assert len(set(span_ids)) == 3, f"each request its own span: {tps!r}"


def test_new_session_new_trace():
    with fake() as c1, fake() as c2:
        c1.ping()
        c2.ping()
        (tp1,) = traceparents(c1, 1)
        (tp2,) = traceparents(c2, 1)
    id1 = TRACEPARENT.match(tp1).group(1)  # type: ignore[union-attr]
    id2 = TRACEPARENT.match(tp2).group(1)  # type: ignore[union-attr]
    assert id1 != id2, "sessions must not share a trace id"


def test_initialize_carries_trace_meta():
    """initialize sends dict params; the traceparent must survive that
    path too, and must not disturb the handshake fields."""
    with fake() as c:
        info = c.initialize()
        (tp,) = traceparents(c, 1)
    assert TRACEPARENT.match(tp)
    assert info.protocol_version  # handshake still negotiated normally
