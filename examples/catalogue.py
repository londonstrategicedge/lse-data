"""Find instruments: markets, the catalogue, contracts by underlying.

Usage:
    LSE_API_KEY=lse_live_... python catalogue.py
"""

from lse import LSE

client = LSE()

for m in client.markets():
    print(f"  {m['folder']:<14} {m['label']:<22} {m['pairs']:>12,} instruments  levels {m['levels']}  open to this key: {m['allowed']}")

# Search a market's instruments by name, or list them best-known first.
print([p["symbol"] for p in client.pairs("us_stocks", q="apple")])
print([p["symbol"] for p in client.pair_page("us_stocks", sort="popular", limit=5)["pairs"]])

# Contracts of one underlying: a CME family, or an option root.
print([p["symbol"] for p in client.pairs("cme", underlying="SP500", expiry="2026-12-18", instrument_type="future")])
print(client.pair_page("us_options", underlying="AAPL", option_type="call", expiry="2026-11-20", limit=3)["total"], "AAPL calls expiring 20 Nov")

# Splits and dividends; the key's own plan and allowance.
print(client.factors("uk_stocks", "ULVR")["dividends"]["rows"][0])
print(client.usage())
