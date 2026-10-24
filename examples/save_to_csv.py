"""
Save live ticks to a CSV file, each with the time the server stamped on it.

Usage:
    pip install lse-data
    LSE_API_KEY=lse_live_... python save_to_csv.py
"""

import csv
from lse import LSE

client = LSE()  # reads LSE_API_KEY from the environment

with open("ticks.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "symbol", "price", "bid", "ask"])

    for tick in client.stream(["BTC/USD", "ETH/USD"]):
        writer.writerow([tick.timestamp, tick.symbol, tick.price, tick.bid, tick.ask])
        f.flush()  # write immediately so you can tail the file
        print(f"{tick.timestamp} {tick.symbol} {tick.price}")
