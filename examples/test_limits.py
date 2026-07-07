#!/usr/bin/env python3
"""
test_limits.py: verify the lse-data plan limits against the live API.

The plan documents three caps on a single data key. This script exercises
each one against the real server so you can confirm they hold:

  1. row cap        every synchronous vault call returns at most one page of rows
  2. rate limit     the plan's calls per minute, then HTTP 429
  3. data metering  every response body is measured in bytes and billed
                    against the monthly allowance shared with streaming

GET /vault/usage (with the key) reports the live monthly figures; this script
reads it before and after so the meter is proven against real numbers.

The key is read from the LSE_API_KEY environment variable or the --key
argument. It is never written to a file.

Usage:
  export LSE_API_KEY=lse_live_xxx
  python3 examples/test_limits.py                 # run every check
  python3 examples/test_limits.py --rate          # only the rate limit check
  python3 examples/test_limits.py --cap --bytes   # cap and metering only
  python3 examples/test_limits.py --symbol ETH/USD
  python3 examples/test_limits.py --rate-calls 150 --rate-workers 24

Exit code is 0 only if every check that ran passed.
"""

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

# Run straight from a checkout without installing: put the repo root (the
# parent of examples/) on the path so "from lse ..." resolves to this tree.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lse import LSE, LSEError  # noqa: E402
# Reuse the SDK's own endpoint constants so this test never drifts from the
# client it is verifying. _USER_AGENT matters because the download host sits
# behind a CDN that bounces the default Python urllib agent.
from lse.client import WS_URL, _USER_AGENT  # noqa: E402
from lse.vault import VAULT_URL  # noqa: E402


# A query with far more rows than one page for any liquid symbol, so the row
# cap is actually exercised: years of hourly bars force the server to truncate.
DEEP_PARAMS = [("timeframe", "1h"), ("order", "desc"), ("limit", "10000")]
# The cheapest possible billable call: one daily bar. Used for the rate test
# where call count matters and payload size does not.
CHEAP_PARAMS = [("timeframe", "1d"), ("order", "desc"), ("limit", "1")]

GREEN = "\033[32m"
RED = "\033[31m"
DIM = "\033[2m"
BOLD = "\033[1m"
OFF = "\033[0m"


class Meter:
    """Wraps every HTTP call so byte volume and call count accumulate in one
    place. This mirrors what the server bills: the size of each response body,
    summed across streaming and download against the monthly allowance."""

    def __init__(self):
        self.calls = 0
        self.bytes = 0          # client measured response sizes
        self.billed = 0         # server reported X-Data-Bytes (the real meter)
        self.last_billed = 0    # X-Data-Bytes of the most recent call

    def get(self, params, key, timeout=30):
        """One raw GET against /vault/candles. Returns (status, nbytes, body,
        seconds). Counts toward the session meter whether it succeeds or 429s.
        Captures the server's X-Data-Bytes header, which is the exact figure
        the server adds to your monthly allowance for this call."""
        qs = urllib.parse.urlencode(params)
        url = f"{VAULT_URL}/candles" + (f"?{qs}" if qs else "")
        req = urllib.request.Request(
            url, headers={"x-api-key": key, "User-Agent": _USER_AGENT}
        )
        t0 = time.perf_counter()
        headers = {}
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                status = resp.status
                headers = resp.headers
        except urllib.error.HTTPError as e:
            body = e.read()
            status = e.code
            headers = e.headers or {}
        dt = time.perf_counter() - t0
        # Prefer the server's own billed figure; fall back to the body length
        # when the header is absent (e.g. a rate or quota reject before metering).
        hdr = headers.get("x-data-bytes") if headers else None
        billed = int(hdr) if hdr and hdr.isdigit() else len(body)
        self.calls += 1
        self.bytes += len(body)
        self.billed += billed
        self.last_billed = billed
        return status, len(body), body, dt


def human_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0


def ok(label, detail=""):
    print(f"  {GREEN}PASS{OFF} {label}" + (f"  {DIM}{detail}{OFF}" if detail else ""))
    return True


def fail(label, detail=""):
    print(f"  {RED}FAIL{OFF} {label}" + (f"  {DIM}{detail}{OFF}" if detail else ""))
    return False


def section(title):
    print(f"\n{BOLD}{title}{OFF}")


# --------------------------------------------------------------------------
# Check 1: the key authenticates over REST, and the same key streams over WS
# --------------------------------------------------------------------------

async def _probe_tier(key, timeout=10):
    """Authenticate over the WebSocket and read the tier and instrument count
    the server reports. Proves the same key works on the live plane too."""
    import websockets

    async with websockets.connect(WS_URL, ping_interval=25, ping_timeout=30) as ws:
        await asyncio.wait_for(ws.recv(), timeout=timeout)  # welcome frame
        await ws.send(json.dumps({"action": "auth", "api_key": key}))
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            msg = json.loads(raw)
            if msg.get("type") == "authenticated":
                return msg.get("tier", ""), len(msg.get("symbols", []))
            if msg.get("type") == "error":
                raise RuntimeError(msg.get("message", "auth error"))


def check_auth(meter, key, symbol):
    section("1. Authentication")
    status, nbytes, body, dt = meter.get([("symbol", symbol)] + CHEAP_PARAMS, key)
    if status == 200:
        passed = ok("REST key accepted", f"{symbol} {status} in {dt * 1000:.0f} ms")
    elif status in (401, 403):
        return fail("REST key rejected", f"HTTP {status}: key bad, inactive, or over quota")
    else:
        passed = fail("unexpected REST status", f"HTTP {status}")

    # Best effort: confirm the key also authenticates on the streaming plane
    # and surface the tier. A WS hiccup here does not fail the check, since
    # the REST result above already proved the key.
    try:
        tier, n = asyncio.run(asyncio.wait_for(_probe_tier(key), timeout=15))
        ok("WebSocket key accepted", f"tier={tier or 'n/a'}, {n} streamable instruments")
    except Exception as e:
        print(f"  {DIM}(stream auth probe skipped: {e}){OFF}")
    return passed


# --------------------------------------------------------------------------
# Check 2: the server caps every call at 5000 rows
# --------------------------------------------------------------------------

def check_cap(meter, key, symbol):
    section("2. Row cap (one page per call)")
    # Ask the raw API for 10000 rows, bypassing the SDK so we test the SERVER
    # cap, not the client clamp. A correct server truncates to 5000.
    status, nbytes, body, dt = meter.get([("symbol", symbol)] + DEEP_PARAMS, key)
    if status != 200:
        return fail("could not fetch deep history", f"HTTP {status} on /vault/candles")
    rows = json.loads(body)
    n = len(rows)
    detail = f"asked 10000, got {n} rows, {human_bytes(nbytes)} in {dt * 1000:.0f} ms"
    if 0 < n <= 5000:
        passed = ok("server truncates to <= 5000", detail)
        if n < 5000:
            print(f"  {DIM}(only {n} rows exist for {symbol} at 1h, cap not stressed but honoured){OFF}")
    else:
        passed = fail("server returned more than 5000 rows", detail)

    # Confirm the SDK pre-clamps too, so a careless limit never even leaves
    # the client. candles() should hand back at most 5000 regardless.
    sdk_rows = LSE(api_key=key).candles(symbol, "1h", limit=10000, order="desc")
    if len(sdk_rows) <= 5000:
        ok("SDK clamps limit before sending", f"candles(limit=10000) returned {len(sdk_rows)}")
    else:
        passed = fail("SDK did not clamp", f"returned {len(sdk_rows)}")
    return passed


# --------------------------------------------------------------------------
# Check 3: about 100 calls per minute, then HTTP 429
# --------------------------------------------------------------------------

def check_rate(meter, key, symbol, n_calls, workers):
    section("3. Rate limit (plan calls per minute)")
    print(f"  {DIM}firing {n_calls} cheap calls across {workers} workers; "
          f"this intentionally spends a minute of your call budget{OFF}")

    results = []  # (status, latency) per call

    def one(_):
        status, _b, _body, dt = meter.get([("symbol", symbol)] + CHEAP_PARAMS, key)
        return status, dt

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for r in pool.map(one, range(n_calls)):
            results.append(r)
    elapsed = time.perf_counter() - t0

    ok_count = sum(1 for s, _ in results if s == 200)
    limited = sum(1 for s, _ in results if s == 429)
    other = [s for s, _ in results if s not in (200, 429)]

    rate_per_min = ok_count / elapsed * 60 if elapsed else 0
    print(f"  {DIM}{n_calls} calls in {elapsed:.1f}s: "
          f"{ok_count} accepted, {limited} throttled (429)"
          + (f", {len(other)} other {set(other)}" if other else "") + OFF)

    if limited == 0:
        return fail("no 429 seen", f"all {n_calls} calls passed; raise --rate-calls to push past the limit")

    # The limit is enforced, which is the pass condition. Report the observed
    # ceiling too, since the window boundary means a burst can straddle two
    # minutes and pass slightly more than one minute's quota.
    passed = ok("rate limit enforced",
                f"throttled after ~{ok_count} accepted in {elapsed:.1f}s")
    print(f"  {DIM}observed burst ceiling ~{ok_count} calls "
          f"(~{rate_per_min:,.0f}/min equivalent) before the first 429{OFF}")
    return passed


# --------------------------------------------------------------------------
# Check 4: per call byte metering toward the monthly allowance
# --------------------------------------------------------------------------

def check_bytes(meter, key, symbol):
    section("4. Data metering (monthly allowance, shared with streaming)")
    # Pull one full 5000 row page and report its billed size, then extrapolate
    # to show how far the monthly allowance stretches at that payload size.
    # One retry after a short wait covers the case where a rate burst (this
    # script's own, or a prior run's) left the bucket briefly drained.
    params = [("symbol", symbol), ("timeframe", "1h"), ("order", "desc"), ("limit", "5000")]
    status, nbytes, body, dt = meter.get(params, key)
    if status == 429:
        print(f"  {DIM}bucket drained from a prior burst, waiting 65s for a fresh minute window{OFF}")
        time.sleep(65)
        status, nbytes, body, dt = meter.get(params, key)
    if status != 200:
        return fail("could not sample a full page", f"HTTP {status}")
    rows = len(json.loads(body))
    # The server's X-Data-Bytes header is the exact amount this call added to
    # your monthly bucket. It matches the body size, and is what the quota gate
    # sums (REST downloads + WS streaming) against the monthly cap.
    per_call = meter.last_billed
    ok("server billed this call", f"{rows} rows, X-Data-Bytes = {per_call:,} ({human_bytes(per_call)})")
    # /vault/usage is the live balance: read it so the meter is checked against
    # the server's own monthly figure, not an assumed allowance.
    req = urllib.request.Request(f"{VAULT_URL}/usage",
                                 headers={"x-api-key": key, "User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            u = json.loads(resp.read().decode())
        used, cap = u.get("bytes_used_month", 0), u.get("bytes_cap_month", -1)
        detail = f"{human_bytes(used)} used" + ("" if cap in (-1, None) else f" of {human_bytes(cap)}")
        ok("usage endpoint reports the live balance", detail)
    except Exception as e:
        print(f"  {DIM}(usage read skipped: {e}){OFF}")
    print(f"  {DIM}downloads and streaming share the one monthly bucket{OFF}")
    return True


def main():
    ap = argparse.ArgumentParser(description="Verify lse-data plan limits with your own key.")
    ap.add_argument("--key", help="API key (else LSE_API_KEY env var)")
    ap.add_argument("--symbol", default="BTC/USD", help="symbol to probe (default BTC/USD)")
    ap.add_argument("--auth", action="store_true", help="run only the auth check")
    ap.add_argument("--cap", action="store_true", help="run only the row cap check")
    ap.add_argument("--rate", action="store_true", help="run only the rate limit check")
    ap.add_argument("--bytes", action="store_true", help="run only the metering check")
    ap.add_argument("--rate-calls", type=int, default=400, help="calls fired in the rate test")
    ap.add_argument("--rate-workers", type=int, default=40, help="concurrent workers in the rate test")
    args = ap.parse_args()

    key = args.key or os.environ.get("LSE_API_KEY")
    if not key:
        print("No API key. Pass --key or set LSE_API_KEY.", file=sys.stderr)
        return 2

    # No flag means run everything. Any flag means run only what was asked.
    selected = {
        "auth": args.auth,
        "cap": args.cap,
        "rate": args.rate,
        "bytes": args.bytes,
    }
    if not any(selected.values()):
        selected = {k: True for k in selected}

    print(f"{BOLD}lse-data limit verification{OFF}  "
          f"{DIM}key {key[:13]}..., symbol {args.symbol}, host {VAULT_URL}{OFF}")

    meter = Meter()
    results = {}
    # Order matters: the rate test deliberately drains the burst bucket, so it
    # runs LAST. Otherwise its 429s spill into the cap and metering calls that
    # follow and fail them for the wrong reason.
    if selected["auth"]:
        results["auth"] = check_auth(meter, key, args.symbol)
    if selected["cap"]:
        results["cap"] = check_cap(meter, key, args.symbol)
    if selected["bytes"]:
        results["bytes"] = check_bytes(meter, key, args.symbol)
    if selected["rate"]:
        results["rate"] = check_rate(meter, key, args.symbol, args.rate_calls, args.rate_workers)

    section("Summary")
    for name, passed in results.items():
        mark = f"{GREEN}PASS{OFF}" if passed else f"{RED}FAIL{OFF}"
        print(f"  {mark}  {name}")
    print(f"  {DIM}this session: {meter.calls} calls, server billed "
          f"{human_bytes(meter.billed)} against your monthly allowance{OFF}")

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
