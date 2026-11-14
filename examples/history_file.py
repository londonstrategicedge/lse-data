"""Pull history as a file: CSV or Parquet, checked by SHA-256.

The server sends every export whole, with its size and SHA-256. If the
connection drops the client asks for it again, and the file is placed only
after its checksum matches what the server states. One export reads at most
2,500,000 rows; estimate() says how many a selection holds.

Usage:
    LSE_API_KEY=lse_live_... python history_file.py
"""

from lse import LSE, LSEError

client = LSE()

# What would it weigh? Estimates answer any key for any dates.
est = client.estimate("us_stocks", "AAPL", "5m", "2026-10-08", "2026-10-14", format="csv")
print(est["rows"], "rows,", est["estimated_bytes"], "bytes,", est["mode"], est["filename"])

# One week of five-minute candles as CSV, split adjusted.
path = client.export("us_stocks", "AAPL", "5m", "2026-10-08", "2026-10-14", format="csv",
                     progress=lambda p: print("  ", p.get("phase"), p.get("bytes", "")))
print("saved", path)

# The same through history(): a DataFrame with the frames extra installed,
# or the file's path with dataframe=False.
daily = client.history("AAPL", timeframe="1d", start="2024-01-01", end="2026-10-01", dataframe=False)
print("saved", daily)

# A paid feed on a free key is refused with the plan that opens it.
try:
    client.export("cme", "ESZ6", "tick", "2026-10-13", "2026-10-13", data="trades", format="parquet")
except LSEError as e:
    print(e.status, e.code, e.message, "->", (e.required_plan or {}).get("name"))
