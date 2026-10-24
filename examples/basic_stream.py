"""
Basic streaming example - print live prices to the terminal.

Usage:
    pip install lse-data
    LSE_API_KEY=lse_live_... python basic_stream.py
"""

from lse import LSE

client = LSE()  # reads LSE_API_KEY from the environment

for tick in client.stream(["BTC/USD", "ETH/USD", "AAPL", "EUR/USD"]):
    print(f"{tick.symbol:12s} ${tick.price:>12,.2f}")
