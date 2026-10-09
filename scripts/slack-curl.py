#!/usr/bin/env python3
"""Send a correctly signed slash-command request to a local flowgate.

Lets you exercise POST /slack/command without Slack or a tunnel:

    export SLACK_SIGNING_SECRET=...      # same value flowgate was started with
    python3 scripts/slack-curl.py status
    python3 scripts/slack-curl.py "run latency 0.2 5"
    python3 scripts/slack-curl.py --user U123 "verdict"

Stdlib only. The signature is v0=HMAC-SHA256(secret, "v0:<ts>:<body>"),
the same recipe Slack uses and internal/slack/verify.go checks.
"""
import argparse
import hashlib
import hmac
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("text", help='command text, e.g. "status" or "run latency 0.2 5"')
    ap.add_argument("--url", default="http://localhost:8080/slack/command")
    ap.add_argument("--user", default="UTEST", help="Slack user id to send as")
    ap.add_argument("--channel", default="CTEST")
    ap.add_argument("--age", type=int, default=0, help="make the timestamp N seconds old (replay test)")
    ap.add_argument("--bad-signature", action="store_true", help="sign with the wrong secret")
    args = ap.parse_args()

    secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if not secret:
        print("set SLACK_SIGNING_SECRET to the value flowgate was started with", file=sys.stderr)
        return 2

    body = urllib.parse.urlencode({
        "command": "/flowgate",
        "text": args.text,
        "user_id": args.user,
        "channel_id": args.channel,
        "team_id": "TTEST",
        # Only matters for "run": the verdict is POSTed here, and flowgate
        # refuses anything that is not https on slack.com.
        "response_url": "https://hooks.slack.com/commands/TTEST/0/local",
    }).encode()
    ts = int(time.time()) - args.age
    key = (secret + "x" if args.bad_signature else secret).encode()
    sig = "v0=" + hmac.new(key, b"v0:%d:" % ts + body, hashlib.sha256).hexdigest()

    req = urllib.request.Request(args.url, data=body, method="POST", headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Slack-Request-Timestamp": str(ts),
        "X-Slack-Signature": sig,
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(resp.status, resp.read().decode())
    except urllib.error.HTTPError as e:
        print(e.code, e.read().decode().strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
