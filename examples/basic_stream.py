"""
Basic streaming example - print live prices to the terminal.

Usage:
    pip install lse-data
    python basic_stream.py
"""

from lse import LSE

client = LSE(api_key="YOUR_API_KEY")

for tick in client.stream(["BTC/USD", "ETH/USD", "AAPL", "EUR/USD"]):
    print(f"{tick.symbol:12s} ${tick.price:>12,.2f}")
