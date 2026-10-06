"""Tests for the MCP client.

Most of these run against a fake server written in Python (see
``fake_server.py``) so the suite needs no Go toolchain and no built
binary — a test that only passes when someone remembered to run
``go build`` is a test that silently stops running.

The last test is the integration one: it drives the real
``flowgate-mcp`` and is skipped when the binary isn't there.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from flowgate_agent.mcp_client import (  # noqa: E402
    CODE_METHOD_NOT_FOUND,
    MCPClient,
    MCPProtocolError,
    MCPTransportError,
    PROTOCOL_VERSION,
)

FAKE = [sys.executable, str(Path(__file__).parent / "fake_server.py")]


def fake(*modes: str) -> MCPClient:
    """A client wired to the fake server, with behaviour flags."""
    env_modes = ",".join(modes)
    return MCPClient(FAKE + ([env_modes] if env_modes else []), timeout=10.0)


# ---- handshake ---------------------------------------------------------


def test_initialize_negotiates_and_reports_server_info():
    with fake() as c:
        info = c.initialize()
    assert info.protocol_version == PROTOCOL_VERSION
    assert info.name == "fake-flowgate"
    assert info.capabilities == {"tools": {"listChanged": False}}


def test_initialized_notification_expects_no_reply():
    """The notification carries no id. If the client waited for a reply
    to it, the next request would read the notification's non-existent
    response and every id would be off by one from there on."""
    with fake() as c:
        c.initialize()
        tools = c.list_tools()  # would mismatch ids if the handshake desynced
    assert [t.name for t in tools] == ["run_experiment", "get_verdict", "list_experiments"]


def test_ping_before_initialize_is_allowed():
    with fake() as c:
        c.ping()


# ---- tools -------------------------------------------------------------


def test_list_tools_exposes_schema_parameters():
    with fake() as c:
        c.initialize()
        tools = {t.name: t for t in c.list_tools()}
    assert tools["run_experiment"].parameters == [
        "blast_radius",
        "duration_s",
        "fault",
    ]
    assert tools["list_experiments"].parameters == []


def test_successful_call_returns_ok_result():
    with fake() as c:
        c.initialize()
        r = c.call_tool("run_experiment", {"fault": "latency", "blast_radius": 0.2})
    assert r.ok is True
    assert r.error is None
    assert r.data["experiment_id"] == "exp-001"


# ---- the distinction this client exists to preserve --------------------


def test_refusal_is_a_result_not_an_exception():
    """A refused experiment is information: the tool exists, the request
    was understood, and policy said no with a reason."""
    with fake() as c:
        c.initialize()
        r = c.call_tool("run_experiment", {"fault": "latency", "blast_radius": 0.9})
    assert r.ok is False
    assert "exceeds the policy cap" in (r.error or "")
    assert r.tool == "run_experiment"


def test_unknown_tool_is_a_protocol_error():
    """An unknown tool means the caller is confused. There is no result
    to read, so this raises rather than returning one."""
    with fake() as c:
        c.initialize()
        with pytest.raises(MCPProtocolError) as e:
            c.call_tool("no_such_tool", {})
    assert e.value.code == CODE_METHOD_NOT_FOUND


# ---- transport ---------------------------------------------------------


def test_replies_are_matched_by_id_not_by_arrival_order():
    """A message carrying an id the client never sent must be skipped,
    not mistaken for the reply being awaited."""
    with fake("stray_message") as c:
        c.initialize()
        tools = c.list_tools()
        r = c.call_tool("run_experiment", {"fault": "latency", "blast_radius": 0.2})
    assert [t.name for t in tools] == ["run_experiment", "get_verdict", "list_experiments"]
    assert r.ok is True


def test_non_json_on_stdout_names_the_stdio_contract():
    """On stdio transport stdout carries protocol messages only. A stray
    print in the server corrupts the stream, and the error should say so
    rather than surfacing as a bare JSON decode failure."""
    with fake("garbage_stdout") as c:
        with pytest.raises(MCPTransportError) as e:
            c.initialize()
    assert "stdout carries protocol messages only" in str(e.value)


def test_server_exit_mid_request_is_reported_with_stderr():
    with fake("exit_early") as c:
        with pytest.raises(MCPTransportError) as e:
            c.initialize()
    assert "closed stdout" in str(e.value) or "exited" in str(e.value)


def test_timeout_does_not_hang_forever():
    c = MCPClient(FAKE + ["hang"], timeout=1.0)
    try:
        with pytest.raises(MCPTransportError) as e:
            c.initialize()
        assert "timed out" in str(e.value)
    finally:
        c.close()


def test_stderr_is_drained_and_available():
    """An unread stderr pipe fills and the server blocks on its next log
    line, which looks exactly like a hang mid-experiment."""
    with fake("chatty_stderr") as c:
        c.initialize()
        c.list_tools()
        tail = c.stderr_tail(500)
    assert len(tail) > 100
    assert any("diagnostic" in line for line in tail)


def test_close_reaps_the_subprocess():
    c = fake()
    c.initialize()
    c.close()
    assert c._proc.poll() is not None


def test_missing_binary_raises_transport_error():
    with pytest.raises(MCPTransportError):
        MCPClient(["/nonexistent/flowgate-mcp-does-not-exist"])


def test_empty_command_is_rejected():
    with pytest.raises(ValueError):
        MCPClient([])


# ---- integration against the real server -------------------------------

REAL = os.environ.get("FLOWGATE_MCP_BIN", "")


@pytest.mark.skipif(
    not (REAL and Path(REAL).exists()),
    reason="set FLOWGATE_MCP_BIN to the built flowgate-mcp binary",
)
def test_real_server_refuses_over_cap_and_runs_within_it():
    import time

    with MCPClient([REAL]) as c:
        info = c.initialize()
        assert info.name == "flowgate"

        names = {t.name for t in c.list_tools()}
        assert {"run_experiment", "get_verdict", "list_experiments"} <= names

        over = c.call_tool(
            "run_experiment",
            {"fault": "latency", "blast_radius": 0.9, "duration_s": 1},
        )
        assert over.ok is False
        assert "policy cap" in (over.error or "")

        started = c.call_tool(
            "run_experiment",
            {
                "fault": "latency",
                "blast_radius": 0.2,
                "duration_s": 1,
                "recovery_timeout_s": 2,
                "latency_ms": 100,
            },
        )
        assert started.ok is True
        experiment_id = started.data["experiment_id"]

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            got = c.call_tool("get_verdict", {"experiment_id": experiment_id})
            if got.data.get("status") == "done":
                verdict = got.data["verdict"]
                # snake_case throughout, matching the enclosing payload
                assert "held" in verdict and "recovered" in verdict
                assert verdict["blast_radius"] == 0.2
                return
            time.sleep(0.25)
        pytest.fail("experiment never reported done")
