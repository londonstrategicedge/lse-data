"""
Disconnect example - collect 100 ticks then exit cleanly.

disconnect() tells the client to stop reconnecting and close the
WebSocket. Works from callbacks (on_tick) or from another thread.

Usage:
    pip install lse-data
    python disconnect_example.py
"""

from lse import LSE

client = LSE(api_key="YOUR_API_KEY")
tick_count = 0


def on_tick(tick):
    global tick_count
    tick_count += 1
    print(f"[{tick_count}/100] {tick.symbol}: ${tick.price:.2f}")
    if tick_count >= 100:
        print("Collected 100 ticks, disconnecting...")
        client.disconnect()


client.on("tick", on_tick)
# connect() blocks until disconnect() is called, then returns
client.connect(symbols=["BTC/USD", "ETH/USD"])
print("Done!")
