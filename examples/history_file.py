"""Pull history as a file: CSV or Parquet, resumable, checked by SHA-256.

A small export streams at once; a large one is prepared on the server and
polled. If the connection drops, run the same call again and it resumes from
the bytes already received. The file is placed only after its size and
checksum match what the server states.

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
