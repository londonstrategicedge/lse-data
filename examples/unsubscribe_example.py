"""
Dynamic subscribe/unsubscribe - change subscriptions at runtime.

Start with three symbols, then swap one for another after 50 ticks. Useful for
dashboards where the user switches between instruments.

Usage:
    pip install lse-data
    LSE_API_KEY=lse_live_... python unsubscribe_example.py
"""

from lse import LSE

client = LSE()  # reads LSE_API_KEY from the environment
tick_count = 0
removed_btc = False


def on_tick(tick):
    global tick_count, removed_btc
    tick_count += 1
    print(f"{tick.symbol}: ${tick.price:.2f}")

    # After 50 ticks, drop BTC and add SOL
    if tick_count == 50 and not removed_btc:
        removed_btc = True
        print("\n--- Switching: unsubscribe BTC/USD, subscribe SOL/USD ---\n")
        client.unsubscribe(["BTC/USD"])
        client.subscribe(["SOL/USD"])


client.on("tick", on_tick)
client.connect(symbols=["BTC/USD", "ETH/USD", "AAPL"])
