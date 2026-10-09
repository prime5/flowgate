#!/usr/bin/env python3
"""Count how many requests ONE client key gets admitted through flowgate.

Use it for the shared-limiter before/after in README.md. A single key is
sent to a URL (the nginx front of deploy/shared-limiter, normally
http://localhost:8000/work) as fast as possible, and the script reports how
many came back 200 vs 429, against the most one bucket could have admitted.

    # in-process buckets: each of 3 replicas has its own, so expect ~3x
    REDIS_ADDR= docker compose -f deploy/shared-limiter/docker-compose.yml up --build
    python3 scripts/limiter-compare.py

    # Redis-backed: one bucket shared by all replicas
    docker compose -f deploy/shared-limiter/docker-compose.yml up --build
    python3 scripts/limiter-compare.py

Standard library only. The header is only honoured when the server runs with
TRUST_FLY_CLIENT_IP=1 (the compose file sets it); against a server that does
not trust it, every request shares your real address, which is the safe case
and still gives one key.
"""
import argparse
import collections
import concurrent.futures
import time
import urllib.error
import urllib.request


def hit(url, header, ip, timeout):
    req = urllib.request.Request(url, headers={header: ip})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0  # transport failure, reported separately


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--url", default="http://localhost:8000/work")
    p.add_argument("--n", type=int, default=300, help="total requests")
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--header", default="Fly-Client-IP")
    p.add_argument("--ip", default="10.9.9.9", help="the one client key")
    p.add_argument("--burst", type=float, default=20)
    p.add_argument("--rate", type=float, default=5, help="refill per second")
    p.add_argument("--timeout", type=float, default=10)
    a = p.parse_args()

    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(a.concurrency) as ex:
        codes = list(ex.map(lambda _: hit(a.url, a.header, a.ip, a.timeout), range(a.n)))
    elapsed = time.monotonic() - start

    c = collections.Counter(codes)
    ok = c.get(200, 0)
    one_bucket = a.burst + a.rate * elapsed
    print(f"url={a.url} key={a.ip} requests={a.n} elapsed={elapsed:.2f}s")
    print("status counts:", dict(sorted(c.items())))
    if c.get(0):
        print(f"WARNING: {c[0]} transport failures; the counts below are not trustworthy")
    print(f"admitted (200): {ok}")
    print(f"most ONE bucket could admit in {elapsed:.2f}s "
          f"(burst {a.burst:g} + {a.rate:g}/s): {one_bucket:.0f}")
    print(f"ratio admitted / one-bucket ceiling: {ok / one_bucket:.2f}")
    print("A ratio near 1 means one shared bucket; near 3 with three "
          "replicas means each replica kept its own.")


if __name__ == "__main__":
    main()
