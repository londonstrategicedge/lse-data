"""
Save live ticks to a CSV file.

Usage:
    pip install lse-data
    python save_to_csv.py
"""

import csv
import datetime
from lse import LSE

client = LSE(api_key="YOUR_API_KEY")

with open("ticks.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "symbol", "price", "bid", "ask"])

    for tick in client.stream(["BTC/USD", "ETH/USD"]):
        now = datetime.datetime.now().isoformat()
        writer.writerow([now, tick.symbol, tick.price, tick.bid, tick.ask])
        f.flush()  # write immediately so you can tail the file
        print(f"{now} {tick.symbol} {tick.price}")
