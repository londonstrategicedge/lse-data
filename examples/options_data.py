"""
Options over REST: chain, flow, and per contract history.

Start from a ticker or a plain company name and get the chain, then
drill into a single contract. The chain gives you each contract's
ticker, and the SDK builds one from its parts when you address a
contract directly.

Usage:
    pip install lse-data
    python options_data.py
"""

from lse import LSE

client = LSE(api_key="YOUR_API_KEY")

# The live chain: one row per contract with last price, IV, greeks, and
# today's volume/premium. Names resolve, so "apple" works as well as "AAPL".
chain = client.options("apple", type="call", max_dte=30)
print(f"{len(chain)} AAPL calls inside 30 days")
for c in chain[:5]:
    print(f"  {c['ticker']}  strike {c['strike']:g}  last {c['last_price']}"
          f"  iv {c['iv']}  vol {c['volume_today']}")

# The tape: recent prints with premium and greeks. Omit underlying to sweep
# every name, e.g. all prints above $250k premium.
big = client.options_flow(min_premium=250_000, limit=10)
for p in big:
    print(f"  {p['ts']}  {p['ticker']}  ${p['premium']:,.0f}")

# One contract's 1 minute history. Address it by parts (the SDK builds the
# OSI ticker) or paste a ticker straight from the chain above.
if chain:
    top = max(chain, key=lambda c: c["volume_today"])
    bars = client.option_candles(top["ticker"], limit=20, order="desc")
    print(f"{top['ticker']}: {len(bars)} recent 1m bars")
    for b in bars[:3]:
        print(f"  {b['minute']}  o {b['open']} h {b['high']} l {b['low']} c {b['close']}"
              f"  vol {b['volume']}")

# Discovery: every underlying that has listed options.
names = client.options_underlyings()
print(f"{len(names)} option underlyings available")
