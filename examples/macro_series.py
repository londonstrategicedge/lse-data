"""Macro economics: find a series, read it as rows, export it as a file.

Usage:
    LSE_API_KEY=lse_live_... python macro_series.py
"""

from lse import LSE

client = LSE()

# The catalogue: kinds and countries, then a page of US observation series.
summary = client.macro_catalogue()
print([(k["kind"], k["series"]) for k in summary["kinds"]])
page = client.macro_catalogue("observation", country="US", q="inflation", limit=5)
for s in page["series"]:
    print(f"  {s['code']:<36} {s['name']:<40} {s['frequency']:<10} {s['first']} to {s['last']}")

# One series as (date, value) rows: a yield by its tenor, an observation by its code.
print(client.series("US10Y", order="desc", limit=3))
print(client.series("US.core_inflation_rate", start="2026-01-01"))

# Every row of a window as JSON, and the whole series as a CSV file.
rows = client.macro_series("observation", "US.core_inflation_rate", "2020-01-01", "2026-10-01")["rows"]
print(len(rows), "rows since 2020")
path = client.macro_export("observation", "US.core_inflation_rate", "1957-02-01", "2026-07-01", format="csv")
print("saved", path)
