"""A fake MCP server, so the client's tests need no Go build.

It speaks the same newline-delimited JSON-RPC as internal/mcp and
reproduces the one distinction that matters: a refused call comes back
as a result with isError, an unknown tool comes back as a JSON-RPC
error.

Behaviour flags arrive as a comma-separated argv[1], so a test can ask
for a misbehaving server:

    stray_message    emit an unsolicited message with an id nobody awaits,
                     just before the real reply
    garbage_stdout   print a non-protocol line on stdout
    exit_early       close stdout mid-request
    hang             accept requests and never answer
    chatty_stderr    write far more stderr than a pipe buffer holds
"""

from __future__ import annotations

import json
import sys

PROTOCOL_VERSION = "2025-06-18"
CODE_METHOD_NOT_FOUND = -32601

TOOLS = [
    {
        "name": "run_experiment",
        "description": "Start one fault-injection experiment.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "fault": {"type": "string"},
                "blast_radius": {"type": "number"},
                "duration_s": {"type": "number"},
            },
            "required": ["fault", "blast_radius"],
        },
    },
    {
        "name": "get_verdict",
        "description": "Poll one experiment.",
        "inputSchema": {
            "type": "object",
            "properties": {"experiment_id": {"type": "string"}},
            "required": ["experiment_id"],
        },
    },
    {
        "name": "list_experiments",
        "description": "List runs.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]

MAX_BLAST_RADIUS = 0.5


def tool_result(payload: dict, is_error: bool) -> dict:
    return {
        "content": [{"type": "text", "text": json.dumps(payload)}],
        "structuredContent": payload,
        "isError": is_error,
    }


def send(message: dict) -> None:
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def main() -> None:
    modes = set(sys.argv[1].split(",")) if len(sys.argv) > 1 and sys.argv[1] else set()

    if "chatty_stderr" in modes:
        # Far more than a 64KiB pipe buffer: a client that does not drain
        # stderr will deadlock here rather than finishing the handshake.
        for i in range(4000):
            print(f"fake: diagnostic line {i} " + "x" * 40, file=sys.stderr)
        sys.stderr.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        req = json.loads(line)
        method, req_id = req.get("method"), req.get("id")

        # Echo the caller's trace context to stderr so tests can assert
        # on it without the protocol growing a test-only reply field.
        params = req.get("params")
        meta = params.get("_meta") if isinstance(params, dict) else None
        traceparent = meta.get("traceparent") if isinstance(meta, dict) else None
        if traceparent:
            print(f"fake: traceparent={traceparent}", file=sys.stderr)
            sys.stderr.flush()

        if "hang" in modes:
            continue  # accept everything, answer nothing

        if "garbage_stdout" in modes:
            sys.stdout.write("Listening on :8080\n")  # a stray print in the server
            sys.stdout.flush()
            continue

        if "exit_early" in modes:
            sys.stdout.close()
            return

        if req_id is None:
            continue  # notification: never answer, including notifications/initialized

        if method == "initialize":
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "fake-flowgate", "version": "0.0.1"},
                    "instructions": "fake",
                },
            })

        elif method == "ping":
            send({"jsonrpc": "2.0", "id": req_id, "result": {}})

        elif method == "tools/list":
            if "stray_message" in modes:
                # A reply to an id the client never sent. It must be
                # skipped rather than mistaken for the awaited one.
                send({"jsonrpc": "2.0", "id": 9999, "result": {"tools": []}})
            send({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}})

        elif method == "tools/call":
            params = req.get("params", {})
            name, args = params.get("name"), params.get("arguments", {})

            if name not in {t["name"] for t in TOOLS}:
                send({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": CODE_METHOD_NOT_FOUND,
                        "message": f"unknown tool: {name}",
                    },
                })
            elif name == "run_experiment" and args.get("blast_radius", 0) > MAX_BLAST_RADIUS:
                send({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": tool_result(
                        {
                            "error": f"blast_radius {args['blast_radius']:.3f} exceeds "
                                     f"the policy cap of {MAX_BLAST_RADIUS:.3f}; this "
                                     "limit is enforced by the server and cannot be "
                                     "raised by a caller"
                        },
                        True,
                    ),
                })
            else:
                send({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": tool_result(
                        {
                            "experiment_id": "exp-001",
                            "fault": args.get("fault"),
                            "blast_radius": args.get("blast_radius"),
                            "status": "running",
                        },
                        False,
                    ),
                })

        else:
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": CODE_METHOD_NOT_FOUND, "message": f"unknown method: {method}"},
            })


if __name__ == "__main__":
    main()
