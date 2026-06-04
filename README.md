# lse-data

A Python client for London Strategic Edge market data. Stream live prices and download history with the same key.

[![PyPI](https://img.shields.io/pypi/v/lse-data)](https://pypi.org/project/lse-data/)
[![Python](https://img.shields.io/pypi/pyversions/lse-data)](https://pypi.org/project/lse-data/)
[![Licence](https://img.shields.io/pypi/l/lse-data)](LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/lse-data)](https://pypi.org/project/lse-data/)

```bash
pip install lse-data
```

```python
from lse import LSE

client = LSE(api_key="your_key")
for tick in client.stream(["BTC/USD", "AAPL"]):
    print(tick.symbol, tick.price)
```

It covers stocks, forex, crypto, commodities, indices and ETFs, a little over 4,000 instruments. Live ticks come over a websocket and history comes over plain HTTP, both on one key. Get a key at [londonstrategicedge.com/websockets](https://londonstrategicedge.com/websockets).

## How it compares

|  | lse-data | yfinance | Alpha Vantage | Finnhub |
|---|:---:|:---:|:---:|:---:|
| Live websocket | yes | no | no | yes |
| Historical candles | yes | yes | yes | yes |
| Asset classes | stocks, FX, crypto, commodities, indices, ETFs | equities focus | stocks, FX, crypto | stocks, FX, crypto |
| Official API | yes | no, scrapes Yahoo | yes | yes |
| Free key | yes | none needed | yes | yes |

The free key allows 100 calls a minute and 50 GB of data a month, shared between streaming and download.

## Download history

The same key pulls history over REST. Candles for any instrument, plus the economic calendar, insider trades, dividends and splits.

```python
from lse import LSE

client = LSE(api_key="your_key")

# OHLCV candles. timeframe: 1m, 5m, 15m, 1h, 4h, 1d
candles  = client.candles("BTC/USD", "1d", start="2026-01-01")
intraday = client.candles("AAPL", "1h", limit=200, order="desc")

# Reference and event feeds
events   = client.economic_calendar(region="US", start="2026-04-01")
insiders = client.insider_trades("AAPL", type="P-Purchase")
divs     = client.dividends("AAPL")
splits   = client.splits("NVDA")

# Anything else, with raw filters
rows = client.get("z_insider_trades", symbol="eq.NVDA", limit="50")
```

Each call returns a list of dicts. A call that fails raises `LSEError`:

```python
from lse import LSEError

try:
    client.candles("BTC/USD", "1m")
except LSEError as e:
    print(e.status, e.message)
```

A call returns at most 5,000 rows. Page through more with `start` and `end`.

## Find instruments

`catalog()` lists everything you can stream or download. It needs no key and no connection.

```python
client.catalog()              # every instrument
client.catalog("crypto")      # [{"symbol": "BTC/USD", "name": "Bitcoin", "category": "Crypto"}, ...]
[x["symbol"] for x in client.catalog("forex")]
```

Categories are stock, forex, crypto, etf, commodity and index. Use a symbol straight in `stream` or `candles`.

## Stream live data

```python
from lse import LSE

client = LSE(api_key="your_key")
for tick in client.stream(["BTC/USD", "ETH/USD", "AAPL"]):
    print(tick.symbol, tick.price)
```

Use callbacks instead of a loop:

```python
client = LSE(api_key="your_key")
client.on("tick", lambda t: print(t.symbol, t.price))
client.connect(["BTC/USD"])
```

Events are `tick`, `connected`, `authenticated`, `disconnected` and `error`.

Change subscriptions while connected:

```python
client.subscribe(["SOL/USD"])
client.unsubscribe(["BTC/USD"])
client.subscribe_options(["AAPL"])   # every AAPL contract at once
```

### Replay then live

Pass `start` and the server sends history from that point, then carries on with live ticks on the same connection. History goes back up to 24 hours.

```python
for tick in client.stream(["BTC/USD"], start="2026-06-01T09:00:00"):
    print("replay" if tick.replay else "live", tick.symbol, tick.price)
```

### Async

```python
import asyncio
from lse import LSE

async def main():
    client = LSE(api_key="your_key")
    async for tick in client.stream_async(["BTC/USD"]):
        print(tick)

asyncio.run(main())
```

## The key

Pass it directly, or set it in the environment:

```python
client = LSE(api_key="your_key")

import os
os.environ["LSE_API_KEY"] = "your_key"
client = LSE()
```

`LSE` also works as a context manager, which disconnects on exit:

```python
with LSE() as client:
    for tick in client.stream(["BTC/USD"]):
        ...
```

A tick carries `symbol`, `price`, `bid`, `ask`, `volume`, `timestamp` (an ISO 8601 string), `name` and `replay`. Use `tick.datetime` for the timestamp as a parsed datetime.

## Command line

```bash
lse auth lse_live_xxxxxxxxxxxx
lse stream BTC/USD AAPL
```

## Licence

MIT. See [LICENSE](LICENSE).
