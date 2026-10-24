"""Reference collections: company profiles, insider trades, dividends, COT and more.

Usage:
    LSE_API_KEY=lse_live_... python reference_rows.py
"""

from lse import LSE

client = LSE()

# The eighteen collections, with their rows, series and dates.
for d in client.reference_catalogue()["datasets"]:
    print(f"  {d['dataset']:<22} {d['rows']:>12,} rows  {d['first']} to {d['last']}")

# Rows, filtered in the client: the same names and shapes as before 3.0.
print(client.company_profiles("AAPL")[0]["company_name"])
print(len(client.insider_trades("WRB", type="P-Purchase")), "purchases by WRB insiders")
print(client.dividends("AAPL")[:2])
print(client.cot("GC")[-1])

# A whole collection as one Parquet file.
path = client.dataset("stock_splits", dataframe=False)
print("saved", path)
