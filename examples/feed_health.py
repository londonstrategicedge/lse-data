#!/usr/bin/env python3
"""
feed_health.py: check live websocket and historical candles across every asset
class, accounting for which markets are open right now.

Builds a representative basket from the public catalog (no hardcoded symbols),
streams it for a fixed window to see live ticks, then pulls the most recent 1m
and 1d candle for each symbol to gauge historical freshness. Verdict per class
is market hours aware: a closed market is judged by how stale its last bar is,
not by live ticks.

Run from the lse-data dir with LSE_API_KEY set.
"""

import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lse import LSE, LSEError  # noqa: E402

KEY = os.environ.get("LSE_API_KEY")
if not KEY:
    print("set LSE_API_KEY", file=sys.stderr)
    sys.exit(2)

STREAM_SECONDS = int(os.environ.get("STREAM_SECONDS", "25"))
NOW = datetime.now(timezone.utc)
WD = NOW.weekday()  # 0=Mon .. 6=Sun
H = NOW.hour + NOW.minute / 60.0


def _us_equity_open():
    return WD < 5 and 13.5 <= H < 20.0           # 13:30-20:00 UTC Mon-Fri (EDT)


def _uk_eu_open():
    return WD < 5 and 7.0 <= H < 15.5            # ~07:00-15:30 UTC (London BST)


def _fx_open():
    # Sun 22:00 UTC -> Fri 22:00 UTC
    if WD == 5:
        return False
    if WD == 6:
        return H >= 22.0
    if WD == 4:
        return H < 22.0
    return True


# Expected-open predicate per class (best effort; CFD indices/commodities trade
# nearly around the clock on weekdays so we treat them as open on a weekday).
EXPECTED_OPEN = {
    "US stock":   _us_equity_open(),
    "US ETF":     _us_equity_open(),
    "UK/EU stock": _uk_eu_open(),
    "Intl stock": False,                          # Asia/HK/KS: overnight UTC, assume closed now
    "FX":         _fx_open(),
    "Crypto":     True,                           # 24/7
    "Index":      WD < 5,
    "Commodity":  WD < 5,
}


def build_basket():
    """Pick representative symbols per class straight from the catalog."""
    cat = LSE(api_key=KEY).catalog()
    by = defaultdict(list)
    for x in cat:
        by[x.get("category")].append(x["symbol"])

    def pick(cands, pool, n=3):
        out = [s for s in cands if s in set(pool)]
        for s in pool:
            if len(out) >= n:
                break
            if s not in out:
                out.append(s)
        return out[:n]

    stocks = by.get("Stocks", [])
    us = [s for s in stocks if s.isalpha() and s.isupper() and 1 <= len(s) <= 5]
    intl = [s for s in stocks if "." in s]
    uk = [s for s in stocks if s.endswith(".L") or s.endswith(".LON")]

    basket = {
        "US stock":    pick(["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"], us, 5),
        "US ETF":      pick(["SPY", "QQQ", "DIA"], by.get("ETFs", []), 3),
        "UK/EU stock": pick([], uk or intl, 2),
        "Intl stock":  pick(["0005.HK", "000270.KS"], intl, 2),
        "FX":          pick(["EUR/USD", "GBP/USD", "USD/JPY"], by.get("Forex", []), 3),
        "Crypto":      pick(["BTC/USD", "ETH/USD", "SOL/USD"], by.get("Crypto", []), 3),
        "Index":       pick([], by.get("Indices", []), 3),
        "Commodity":   pick(["XAU/USD", "BCO/USD"], by.get("Commodities", []), 3),
    }
    return {k: v for k, v in basket.items() if v}


def stream_window(symbols):
    """Subscribe to all symbols, collect ticks for STREAM_SECONDS, return
    {symbol: (count, last_price)}."""
    got = defaultdict(lambda: [0, None])
    client = LSE(api_key=KEY)

    def on_tick(t):
        got[t.symbol][0] += 1
        got[t.symbol][1] = t.price

    client.on("tick", on_tick)
    th = threading.Thread(target=lambda: client.connect(symbols), daemon=True)
    th.start()
    time.sleep(STREAM_SECONDS)
    client.disconnect()
    th.join(timeout=5)
    return got


def last_candle_age(client, symbol, tf):
    """Age in minutes of the most recent `tf` candle, or None on error/empty."""
    try:
        rows = client.candles(symbol, tf, limit=1, order="desc")
    except LSEError:
        return None
    if not rows:
        return None
    ts = rows[0].get("timestamp") or rows[0].get("time")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    return (NOW - dt).total_seconds() / 60.0


def fmt_age(mins):
    if mins is None:
        return "   n/a"
    if mins < 90:
        return f"{mins:4.0f}m"
    if mins < 60 * 48:
        return f"{mins/60:4.1f}h"
    return f"{mins/1440:4.1f}d"


def main():
    print(f"feed health  {NOW:%Y-%m-%d %H:%M UTC} ({NOW:%A})  stream window {STREAM_SECONDS}s\n")
    basket = build_basket()
    all_syms = [s for v in basket.values() for s in v]
    print(f"streaming {len(all_syms)} symbols across {len(basket)} classes ...")
    ticks = stream_window(all_syms)

    client = LSE(api_key=KEY)
    print(f"\n{'class':12} {'open?':6} {'symbol':12} {'ticks':>6} {'1m age':>7} {'1d age':>7}  verdict")
    print("-" * 78)
    problems = []
    for cls, syms in basket.items():
        exp_open = EXPECTED_OPEN.get(cls, False)
        for s in syms:
            n = ticks.get(s, [0, None])[0]
            a1 = last_candle_age(client, s, "1m")
            ad = last_candle_age(client, s, "1d")
            # Verdict
            if exp_open:
                if n > 0:
                    v = "LIVE ok"
                elif a1 is not None and a1 < 15:
                    v = "fresh (no tick yet)"
                else:
                    v = "OPEN but SILENT + stale"
                    problems.append((cls, s, v))
            else:
                # closed: judge by historical freshness only
                if ad is not None and ad < 1.5 * 1440 + 24 * 60:  # last daily within ~2d
                    v = "closed, hist ok"
                else:
                    v = "closed, hist STALE"
                    problems.append((cls, s, v))
            print(f"{cls:12} {('open' if exp_open else 'closed'):6} {s:12} "
                  f"{n:6d} {fmt_age(a1):>7} {fmt_age(ad):>7}  {v}")

    print("\n" + "=" * 78)
    if problems:
        print(f"FLAGGED {len(problems)} item(s) to investigate:")
        for cls, s, v in problems:
            print(f"  - {cls} {s}: {v}")
    else:
        print("No anomalies for the markets that are open; closed markets have fresh history.")


if __name__ == "__main__":
    main()
