"""A minimal MCP client over stdio, with no third-party dependencies.

Hand-rolled for the same reason internal/mcp hand-rolls the server: the
wire format is documented plain JSON, and writing it here means every
byte on the wire is something this project does rather than something a
library does silently.

# The distinction this client exists to preserve

flowgate's MCP server separates two kinds of "no", and they mean
opposite things to a caller:

  * A **protocol error** (JSON-RPC ``error``, e.g. -32601 unknown tool)
    means the caller is confused — it asked for something that does not
    exist. There is no result. This client raises ``MCPProtocolError``.

  * A **refusal** (a normal result carrying ``isError: true``) means the
    tool exists, the request was understood, and the server deliberately
    said no, with a reason. That is information, not a failure. This
    client returns it as a ``ToolResult`` with ``ok == False``.

Flattening both into one exception would destroy the difference, and the
difference is the whole point: an agent should change its approach in
the first case and adjust its request in the second.

# The stdio contract

On stdio transport stdout carries protocol messages and nothing else,
so the server writes every diagnostic to stderr. This client reads
stderr on a separate thread and keeps the last lines available via
``stderr_tail()`` — draining it matters, because a server that fills the
stderr pipe buffer while nobody reads it will block forever.

Messages are newline-delimited JSON: Go's ``json.Encoder`` terminates
each value with a newline, and this client writes the same way.

# Tracing

Every request carries the client's trace context in ``params._meta``,
the slot MCP reserves for request metadata. The trace id is fixed for
the client's session — a ``run_experiment`` and its ``get_verdict``
polls land in one trace — while the span id is fresh per request, so
each call is its own span. The Go server continues the trace from
there. Trace ids come from ``secrets``, and there are no third-party
dependencies: this file stays stdlib-only.
"""

from __future__ import annotations

import json
import queue
import secrets
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

PROTOCOL_VERSION = "2025-06-18"

# JSON-RPC 2.0 standard error codes, mirrored from internal/mcp/jsonrpc.go.
CODE_PARSE_ERROR = -32700
CODE_INVALID_REQUEST = -32600
CODE_METHOD_NOT_FOUND = -32601
CODE_INVALID_PARAMS = -32602
CODE_INTERNAL_ERROR = -32603


class MCPError(Exception):
    """Base for every failure this client raises."""


class MCPProtocolError(MCPError):
    """The server returned a JSON-RPC error: the caller asked wrongly.

    This is *not* how a refused experiment arrives — see ``ToolResult``.
    """

    def __init__(self, code: int, message: str, data: Any = None):
        super().__init__(f"jsonrpc {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class MCPTransportError(MCPError):
    """The server died, timed out, or wrote something unparseable."""


@dataclass(frozen=True)
class Tool:
    """One entry from ``tools/list``."""

    name: str
    description: str
    input_schema: dict[str, Any]

    @property
    def parameters(self) -> list[str]:
        return sorted(self.input_schema.get("properties", {}))


@dataclass(frozen=True)
class ToolResult:
    """The outcome of ``tools/call``.

    ``ok`` is False for a refusal — the tool ran and said no, with a
    reason in ``error``. That is a result worth reading, not an
    exception worth catching.
    """

    tool: str
    ok: bool
    data: dict[str, Any]
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def error(self) -> str | None:
        """The refusal reason, when the server refused."""
        if self.ok:
            return None
        return str(self.data.get("error", "")) or None


@dataclass(frozen=True)
class ServerInfo:
    """What ``initialize`` negotiated."""

    protocol_version: str
    name: str
    version: str
    capabilities: dict[str, Any]
    instructions: str = ""


class MCPClient:
    """Speaks MCP to a server started as a subprocess over stdio.

    Use it as a context manager so the subprocess is always reaped::

        with MCPClient(["./flowgate-mcp"]) as c:
            info = c.initialize()
            for tool in c.list_tools():
                print(tool.name, tool.parameters)
    """

    def __init__(
        self,
        command: list[str],
        *,
        client_name: str = "flowgate-agent",
        client_version: str = "0.1.0",
        timeout: float = 30.0,
        stderr_lines: int = 200,
    ):
        if not command:
            raise ValueError("command must not be empty")
        self.command = command
        self.client_name = client_name
        self.client_version = client_version
        self.timeout = timeout

        self._next_id = 1
        self._pending: dict[int, dict[str, Any]] = {}
        self._stderr: deque[str] = deque(maxlen=stderr_lines)
        self._initialized = False

        # One trace per session: every request this client sends joins
        # it, each as its own span. Generated here rather than per call
        # so a run_experiment and the polls that follow it are one
        # trace on the server's side too.
        self._trace_id = secrets.token_hex(16)

        try:
            self._proc = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # line buffered: messages are newline-delimited
            )
        except OSError as e:
            raise MCPTransportError(f"could not start {command[0]!r}: {e}") from e

        # stdout is read on its own thread into a queue rather than
        # inline. A blocking readline() cannot be given a deadline, so a
        # server that accepts a request and never answers would hang the
        # caller forever — and the caller here polls get_verdict in a
        # loop, which is exactly where that would bite.
        self._lines: queue.Queue[str | None] = queue.Queue()
        self._stdout_thread = threading.Thread(
            target=self._pump_stdout, daemon=True, name="mcp-stdout"
        )
        self._stdout_thread.start()

        # Drain stderr continuously. An unread pipe fills its buffer and
        # the server blocks on its next log line, which looks exactly
        # like a hang in the middle of an experiment.
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, daemon=True, name="mcp-stderr"
        )
        self._stderr_thread.start()

    # ---- lifecycle -----------------------------------------------------

    def __enter__(self) -> MCPClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Shut the server down, politely first."""
        if self._proc.poll() is None:
            try:
                if self._proc.stdin:
                    self._proc.stdin.close()
                self._proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                self._proc.kill()
                self._proc.wait(timeout=5)
        for stream in (self._proc.stdout, self._proc.stderr):
            try:
                if stream:
                    stream.close()
            except OSError:
                pass

    def stderr_tail(self, n: int = 20) -> list[str]:
        """The server's most recent diagnostics, newest last."""
        return list(self._stderr)[-n:]

    def _pump_stdout(self) -> None:
        """Feed every stdout line into the queue; None marks EOF."""
        stream = self._proc.stdout
        if stream is None:
            self._lines.put(None)
            return
        try:
            for line in stream:
                self._lines.put(line)
        except (ValueError, OSError):
            pass
        finally:
            self._lines.put(None)

    def _drain_stderr(self) -> None:
        stream = self._proc.stderr
        if stream is None:
            return
        try:
            for line in stream:
                self._stderr.append(line.rstrip("\n"))
        except (ValueError, OSError):
            pass  # stream closed during shutdown

    # ---- protocol ------------------------------------------------------

    def initialize(self) -> ServerInfo:
        """Run the handshake: ``initialize`` then ``notifications/initialized``."""
        result = self._request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": self.client_name,
                    "version": self.client_version,
                },
            },
        )
        # The initialized notification carries no id and gets no reply,
        # ever. Waiting for one here deadlocks the client.
        self._notify("notifications/initialized")
        self._initialized = True

        server = result.get("serverInfo", {})
        return ServerInfo(
            protocol_version=result.get("protocolVersion", ""),
            name=server.get("name", ""),
            version=server.get("version", ""),
            capabilities=result.get("capabilities", {}),
            instructions=result.get("instructions", "") or "",
        )

    def ping(self) -> None:
        """Round-trip the server. Permitted before initialization."""
        self._request("ping", None)

    def list_tools(self) -> list[Tool]:
        result = self._request("tools/list", None)
        return [
            Tool(
                name=t.get("name", ""),
                description=t.get("description", ""),
                input_schema=t.get("inputSchema", {}) or {},
            )
            for t in result.get("tools", [])
        ]

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> ToolResult:
        """Call a tool.

        Returns a ``ToolResult`` whether the server accepted or refused.
        Raises ``MCPProtocolError`` only when the tool does not exist or
        the request was malformed — that is the caller being confused,
        not the server declining.
        """
        result = self._request(
            "tools/call", {"name": name, "arguments": arguments or {}}
        )
        is_error = bool(result.get("isError", False))
        return ToolResult(
            tool=name,
            ok=not is_error,
            data=_decode_content(result),
            raw=result,
        )

    # ---- transport -----------------------------------------------------

    def _request(self, method: str, params: Any) -> dict[str, Any]:
        msg_id = self._next_id
        self._next_id += 1

        message: dict[str, Any] = {"jsonrpc": "2.0", "id": msg_id, "method": method}
        params = self._with_trace_meta(params)
        if params is not None:
            message["params"] = params
        self._write(message)

        reply = self._read_reply(msg_id)
        if "error" in reply:
            err = reply["error"] or {}
            raise MCPProtocolError(
                code=int(err.get("code", CODE_INTERNAL_ERROR)),
                message=str(err.get("message", "")),
                data=err.get("data"),
            )
        return reply.get("result", {}) or {}

    def _with_trace_meta(self, params: Any) -> Any:
        """Attach this session's trace context to an outgoing request.

        The W3C traceparent goes in ``params._meta``. The span id is
        fresh per request; the trace id is the session's. A
        caller-supplied ``_meta`` is kept and only gains a traceparent,
        and non-dict params are left alone — tracing is metadata, never
        a reason to reshape the caller's payload.
        """
        traceparent = f"00-{self._trace_id}-{secrets.token_hex(8)}-01"
        if params is None:
            return {"_meta": {"traceparent": traceparent}}
        if isinstance(params, dict):
            out = dict(params)
            meta = out.get("_meta")
            merged = dict(meta) if isinstance(meta, dict) else {}
            merged.setdefault("traceparent", traceparent)
            out["_meta"] = merged
            return out
        return params

    def _notify(self, method: str, params: Any = None) -> None:
        """Send a message with no id. The server must not reply."""
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._write(message)

    def _write(self, message: dict[str, Any]) -> None:
        if self._proc.poll() is not None:
            raise MCPTransportError(
                f"server exited with code {self._proc.returncode}; "
                f"stderr: {' | '.join(self.stderr_tail(5))}"
            )
        try:
            assert self._proc.stdin is not None
            self._proc.stdin.write(json.dumps(message) + "\n")
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            raise MCPTransportError(f"writing {message.get('method')!r}: {e}") from e

    def _read_reply(self, want_id: int) -> dict[str, Any]:
        """Read until the reply with ``want_id`` arrives.

        Replies are matched by id rather than assumed in order. This
        server answers in order, but a client that assumes ordering
        breaks silently against one that does not.
        """
        if want_id in self._pending:
            return self._pending.pop(want_id)

        deadline = time.monotonic() + self.timeout
        while True:
            if time.monotonic() > deadline:
                raise MCPTransportError(
                    f"timed out after {self.timeout}s waiting for reply to id {want_id}"
                )

            line = self._readline(deadline - time.monotonic())
            if line is None:
                raise MCPTransportError(
                    f"server closed stdout while awaiting id {want_id}; "
                    f"stderr: {' | '.join(self.stderr_tail(5))}"
                )
            line = line.strip()
            if not line:
                continue

            try:
                message = json.loads(line)
            except json.JSONDecodeError as e:
                raise MCPTransportError(
                    f"server wrote a non-JSON line on stdout: {line[:200]!r} ({e}). "
                    "On stdio transport stdout carries protocol messages only."
                ) from e

            got = message.get("id")
            if got == want_id:
                return message
            if isinstance(got, int):
                self._pending[got] = message  # out of order: keep for later

    def _readline(self, remaining: float) -> str | None:
        """Next protocol line, or None at EOF.

        Raises ``queue.Empty`` upward as a timeout via the caller's
        deadline check — the wait itself is bounded, so a silent server
        cannot pin the caller.
        """
        if remaining <= 0:
            raise MCPTransportError(f"timed out after {self.timeout}s waiting for the server")
        try:
            return self._lines.get(timeout=remaining)
        except queue.Empty:
            raise MCPTransportError(
                f"timed out after {self.timeout}s waiting for the server; "
                f"stderr: {' | '.join(self.stderr_tail(5))}"
            ) from None


def _decode_content(result: dict[str, Any]) -> dict[str, Any]:
    """Pull the payload out of an MCP tool result.

    flowgate returns its payload as JSON inside ``content[0].text``, and
    also as ``structuredContent`` where the client understands it. Prefer
    the structured form and fall back to parsing the text, so this client
    keeps working against servers that send only one of them.
    """
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured

    for block in result.get("content", []) or []:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text", "")
            try:
                parsed = json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return {"text": text}
            return parsed if isinstance(parsed, dict) else {"value": parsed}
    return {}
