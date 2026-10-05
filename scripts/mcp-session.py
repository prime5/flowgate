#!/usr/bin/env python3
"""Drive the flowgate MCP server over stdio and narrate the session.

MCP on stdio is a subprocess protocol, not a network service: the client
spawns the server and owns its stdin/stdout. That is what this script is --
a minimal client. There is no port to connect to, which is why you cannot
poke at it from a second terminal.

Usage:
    python3 scripts/mcp-session.py            # builds and runs ./cmd/flowgate-mcp
    python3 scripts/mcp-session.py ./flowgate-mcp   # or point at a built binary
"""
import json
import subprocess
import sys
import time

BOLD, DIM, RESET = "\033[1m", "\033[2m", "\033[0m"


def head(title):
    print(f"\n{BOLD}{'=' * 70}\n{title}\n{'=' * 70}{RESET}")


class Client:
    """A minimal MCP client: spawn the server, speak JSON-RPC on its pipes."""

    def __init__(self, cmd):
        self.p = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self._id = 0

    def _send(self, msg):
        self.p.stdin.write(json.dumps(msg) + "\n")
        self.p.stdin.flush()

    def rpc(self, method, params=None):
        self._id += 1
        msg = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params:
            msg["params"] = params
        self._send(msg)
        line = self.p.stdout.readline()
        if not line:
            err = self.p.stderr.read()
            raise SystemExit(f"server closed the stream. stderr:\n{err}")
        return json.loads(line)

    def notify(self, method):
        self._send({"jsonrpc": "2.0", "method": method})

    def call(self, name, args):
        """Returns (structuredContent, isError) or raises on a protocol error."""
        r = self.rpc("tools/call", {"name": name, "arguments": args})
        if "error" in r:
            return {"_protocol_error": r["error"]}, True
        res = r["result"]
        return res.get("structuredContent", {}), res.get("isError", False)

    def close(self):
        self.p.stdin.close()
        self.p.wait(timeout=10)
        return self.p.returncode


def main():
    cmd = sys.argv[1:] or ["go", "run", "./cmd/flowgate-mcp"]
    c = Client(cmd)

    head("1. Handshake")
    r = c.rpc("initialize", {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "mcp-session.py", "version": "1.0"},
    })["result"]
    c.notify("notifications/initialized")
    print(f"  protocol   {r['protocolVersion']}")
    print(f"  server     {r['serverInfo']['name']} {r['serverInfo']['version']}")
    print(f"  capabilities {json.dumps(r['capabilities'])}")
    print(f"{DIM}  instructions: {len(r.get('instructions',''))} chars sent to the model{RESET}")

    head("2. tools/list -- what the model can see")
    for t in c.rpc("tools/list")["result"]["tools"]:
        req = ",".join(t["inputSchema"].get("required", [])) or "-"
        print(f"  {BOLD}{t['name']:<18}{RESET} required: {req}")
        print(f"{DIM}    {t['description'][:110]}...{RESET}")

    head("3. Guardrails -- the server refuses, it does not clamp")
    for label, args in [
        ("blast radius 1.0 (cap is 0.5)", {"fault": "latency", "blast_radius": 1.0, "duration_s": 3}),
        ("duration 120s (cap is 15s)", {"fault": "latency", "blast_radius": 0.4, "duration_s": 120}),
        ("a fault that does not exist", {"fault": "delete_prod", "blast_radius": 0.1, "duration_s": 3}),
        ("capacity with no target size", {"fault": "capacity", "blast_radius": 0.5, "duration_s": 3}),
    ]:
        sc, is_err = c.call("run_experiment", args)
        mark = "REFUSED " if is_err else "ACCEPTED"
        print(f"  {BOLD}{mark}{RESET} {label}")
        print(f"{DIM}           {sc.get('error', sc)}{RESET}")

    print(f"\n  {DIM}An unknown TOOL is different -- that is a protocol error, not a result:{RESET}")
    sc, _ = c.call("no_such_tool", {})
    print(f"{DIM}           {sc.get('_protocol_error')}{RESET}")

    head("4. A real experiment, start to verdict")
    sc, is_err = c.call("run_experiment", {
        "fault": "latency", "blast_radius": 0.4,
        "duration_s": 3, "recovery_timeout_s": 3, "latency_ms": 400,
    })
    if is_err:
        raise SystemExit(f"unexpected refusal: {sc}")
    eid = sc["experiment_id"]
    print(f"  started {BOLD}{eid}{RESET} -- returned immediately, nothing blocked")

    sc2, err2 = c.call("run_experiment", {"fault": "blackhole", "blast_radius": 0.2, "duration_s": 2})
    print(f"  second experiment mid-run: {BOLD}{'REFUSED' if err2 else 'ACCEPTED'}{RESET}")
    print(f"{DIM}    {sc2.get('error','')}{RESET}")

    print("\n  polling get_verdict:")
    for n in range(1, 40):
        sc3, _ = c.call("get_verdict", {"experiment_id": eid})
        print(f"    poll {n}: {sc3['status']}")
        if sc3["status"] == "done":
            v = sc3["verdict"]
            print(f"\n  {BOLD}VERDICT{RESET}")
            for k in ("Name", "BaselineOK", "Held", "Recovered", "BlastRadius", "Detail"):
                print(f"    {k:<12} {v.get(k)}")
            print(f"\n{DIM}    BaselineOK true  = the system was healthy before injection{RESET}")
            print(f"{DIM}    Held       false = the fault broke the hypothesis -- usually the point{RESET}")
            print(f"{DIM}    Recovered  true  = it came back after rollback; false is what should worry you{RESET}")
            break
        time.sleep(1)

    head("5. list_experiments -- history and the policy in force")
    sc4, _ = c.call("list_experiments", {})
    print(f"  {sc4['count']} experiment(s)")
    for e in sc4["experiments"]:
        print(f"    {e['experiment_id']}  {e['fault']:<10} radius={e['blast_radius']:<5} {e['status']}")
    print(f"\n  limits the model is told about up front:")
    for k, v in sc4["limits"].items():
        print(f"    {k:<24} {v}")

    code = c.close()
    print(f"\n{DIM}server exited {code} on stdin close -- that is how MCP shuts down on stdio{RESET}")


if __name__ == "__main__":
    main()
