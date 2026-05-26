"""
Options chain streaming - subscribe to all contracts for a stock.

subscribe_options("AAPL") gives you every AAPL option contract
(calls + puts, all strikes, all expiries) as a single subscription.
This is much more efficient than subscribing to each contract name.

Usage:
    pip install lse-data
    python options_stream.py
"""

from lse import LSE

client = LSE(api_key="YOUR_API_KEY")

# Subscribe to all AAPL and TSLA option contracts
client.subscribe_options(["AAPL", "TSLA"])

# Also get the underlying stock price for comparison
client.subscribe(["AAPL", "TSLA"])


def on_tick(tick):
    # Option contract symbols look like "AAPL250620C00200000"
    # Underlying stock symbols are just "AAPL"
    if len(tick.symbol) > 6:
        print(f"  [OPT] {tick.symbol}: ${tick.price:.2f}")
    else:
        print(f"[STOCK] {tick.symbol}: ${tick.price:.2f}")


client.on("tick", on_tick)
client.connect()
