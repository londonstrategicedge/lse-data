"""Pull deep history from the LSE vault: ticks, candles, economics, reference data.

Every history()/dataset() pull runs one export job in the vault, lands as a
Parquet file and, with the frames extra installed (pip install 'lse-data[frames]'),
also returns a pandas DataFrame. Plans include an hourly export-job budget
(GET /vault/usage shows it), so this example runs two exports and reads the
rest through the instant row endpoints.
"""

from lse import LSE

client = LSE()  # reads LSE_API_KEY from the environment

# What does the vault hold? One row per instrument with its full recorded span.
for row in client.datasets("stocks")[:5]:
    print(row["symbol"], row["ticks"], "ticks,", row["first_tick"][:10], "to", row["last_tick"][:10])

# Export 1: a year of daily candles. The dataset resolves from the catalog.
daily = client.history("AAPL", timeframe="1d", start="2024-01-01", end="2025-01-01")
print(daily.tail())

# Export 2: the raw tick tape for one day.
ticks = client.history("BTC/USD", start="2026-06-30", end="2026-07-01")
print(len(ticks), "ticks")

# Macro economics series need no export job: list them, then pull one back to
# 1971 as (date, value) rows in a single call.
print(len(client.economics()), "series available")
fed_funds = client.economics("fdtr")
print(fed_funds[-3:])

# Whole reference datasets also export as single Parquet files when you want
# them, e.g. client.dataset("cot"); that is another export job, so pace it.
cot_rows = client.cot("GC")  # instant rows instead: CFTC gold positioning
print(len(cot_rows), "COT rows for gold")
