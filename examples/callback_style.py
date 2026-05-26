"""
Callback-style example - event-driven tick handling.

Usage:
    pip install lse-data
    python callback_style.py
"""

from lse import LSE


def on_tick(tick):
    print(f"[TICK] {tick.symbol}: {tick.price}")


def on_connected():
    print("Connected to LSE")


def on_authenticated():
    print("Authenticated successfully")


def on_error(msg):
    print(f"[ERROR] {msg}")


client = LSE(api_key="YOUR_API_KEY")
client.on("tick", on_tick)
client.on("connected", on_connected)
client.on("authenticated", on_authenticated)
client.on("error", on_error)

# connect() blocks forever, ticks arrive via on_tick callback
client.connect(symbols=["BTC/USD", "ETH/USD", "SOL/USD"])
