"""
Options chain streaming - subscribe to all contracts for a stock.

subscribe_options("AAPL") gives you every AAPL option contract
(calls + puts, all strikes, all expiries) as a single subscription.
This is much more efficient than subscribing to each contract name.

Option ticks arrive as OptionTick objects with the contract already parsed:
underlying, right ("call"/"put"), strike, expiry, dte, premium, notional.
For a ready-made, human-readable feed, pass tape() as the tick callback.

Usage:
    pip install lse-data
    LSE_API_KEY=lse_live_... python options_stream.py
"""

from lse import LSE, OptionTick, tape

client = LSE()  # reads LSE_API_KEY from the environment

# Subscribe to all AAPL and TSLA option contracts
client.subscribe_options(["AAPL", "TSLA"])

# Also get the underlying stock price for comparison
client.subscribe(["AAPL", "TSLA"])

# Option A: the aligned column table, one line per tick, header printed once.
client.on("tick", tape())



# Option B: your own callback on the parsed fields. OptionTick is a Tick
# subclass, so plain stock ticks and option ticks share one callback.
# To use it, register it in place of tape(): client.on("tick", short_dated_puts)
def short_dated_puts(tick):
    if isinstance(tick, OptionTick) and tick.right == "put" and tick.dte <= 7 and tick.volume:
        print(f"{tick.underlying} {tick.strike:g} put, {tick.dte}d left: "
              f"${tick.premium:.2f} x{tick.volume} (${tick.notional:,.0f})")


client.connect()
