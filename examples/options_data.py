"""
Options: the chain, the flow, and one contract's candles.

Start from a ticker or a plain company name and get the chain, then drill
into a single contract. The chain gives you each contract's ticker, and the
client builds one from its parts when you address a contract directly.

Usage:
    pip install lse-data
    LSE_API_KEY=lse_live_... python options_data.py
"""

from datetime import datetime, timedelta, timezone

from lse import LSE, LSEError

client = LSE()  # reads LSE_API_KEY from the environment

# The chain: one row per listed contract with its last trade, today's volume
# and premium, and the newest implied volatility and Greeks. Names resolve, so
# "apple" works as well as "AAPL".
chain = client.options("apple", type="call", max_dte=30)
print(f"{len(chain)} AAPL calls inside 30 days")
for c in chain[:5]:
    print(f"  {c['ticker']}  strike {c['strike']:g}  last {c['last_price']}  iv {c['iv']}  vol {c['volume_today']}")

# The tape: prints with their premium, venue and the Greeks of their minute.
# Without an underlying the call sweeps every name: today by default, or up to
# seven days with start and end.
week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
big = client.options_flow(min_premium=250_000, start=week_ago, limit=10)
for p in big:
    print(f"  {p['ts']}  {p['ticker']}  venue {p['venue']}  ${p['premium']:,.0f}")

# One contract's candles: the one that traded last. Address a contract by its
# parts, or paste a ticker from the chain. A contract with no trade in the last
# 30 days has no candles, and the call says so.
traded = [c for c in chain if c["last_trade_at"]]
if traded:
    top = max(traded, key=lambda c: c["last_trade_at"])
    try:
        bars = client.option_candles(top["ticker"], limit=20, order="desc")
    except LSEError as e:
        bars = []
        print(f"{top['ticker']}: no candles in the last 30 days ({e.code})")
    print(f"{top['ticker']}: {len(bars)} recent 1m bars")
    for b in bars[:3]:
        print(f"  {b['minute']}  o {b['open']} h {b['high']} l {b['low']} c {b['close']}  vol {b['volume']}")

# Discovery: every underlying that has listed options.
names = client.options_underlyings()
print(f"{len(names)} option underlyings available")
