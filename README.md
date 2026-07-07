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

It covers stocks, forex, crypto, commodities, indices, ETFs, futures and options, plus macro economics series and government bond yields. Live ticks come over a websocket. Every historical read is served from the LSE vault, the ClickHouse store behind the platform, which holds the full recorded tape: US stocks back to 2003, FX to 2009, crypto to 2017, options prints to 2014, and 14,000+ economics series, some reaching back over a century. Get a key at [londonstrategicedge.com/data](https://londonstrategicedge.com/data).

## How it compares

|  | lse-data | yfinance | Alpha Vantage | Finnhub |
|---|:---:|:---:|:---:|:---:|
| Live websocket | yes | no | no | yes |
| Historical candles | yes | yes | yes | yes |
| Tick history | yes | no | no | paid |
| Asset classes | stocks, FX, crypto, commodities, indices, ETFs, futures, options | equities focus | stocks, FX, crypto | stocks, FX, crypto |
| Official API | yes | no, scrapes Yahoo | yes | yes |
| Cost | free | free | free + paid | free + paid |

Streaming and download share one allowance. `GET /vault/usage` (with your key) reports where you stand.

## Download history

The same key reads the vault over REST: candles at fourteen resolutions for any instrument, plus the reference sets (economic calendar, insider trades, dividends, splits, COT positioning, financial statements, company profiles, fundamentals, bond yields).

```python
from lse import LSE

client = LSE(api_key="your_key")

# OHLCV candles. timeframe: 1s, 5s, 15s, 30s, 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1mo
candles  = client.candles("BTC/USD", "1d", start="2026-01-01")
intraday = client.candles("AAPL", "1h", limit=200, order="desc")
fine     = client.candles("EUR/USD", "5s", start="2026-07-01", end="2026-07-02")

# Macro series and yields: (date, value) rows, any series in one call
cpi      = client.economics("cpi_yoy")            # US inflation back to 1914
ffr      = client.series("fdtr", start="1980-01-01")
bund     = client.series("DE10Y")

# Reference and event feeds
events   = client.economic_calendar(region="US", start="2026-04-01")
insiders = client.insider_trades("WRB", type="P-Purchase")
divs     = client.dividends("AAPL")
splits   = client.splits("NVDA")
cot      = client.cot("GC")        # COT uses futures codes: GC gold, CL crude, ES S&P
reports  = client.financial_reports("AAPL", report_type="income", period="FY")
profile  = client.company_profiles("NVDA")
funda    = client.fundamentals("MSFT")
yields   = client.bond_yields("US10Y", start="2000-01-01")
```

Each call returns a list of dicts. A call that fails raises `LSEError`:

```python
from lse import LSEError

try:
    client.candles("BTC/USD", "1m")
except LSEError as e:
    print(e.status, e.message)
```

A call returns one page of rows. Page through more with `start` and `end`, or pull the whole range at once with `history()` below.

## Deep history as Parquet

Interactive calls page; bulk pulls do not have to. `history()` runs an export job in the vault, waits, and downloads the finished Parquet file with resume support. With `pip install 'lse-data[frames]'` it returns a DataFrame directly. Each `history()` or `dataset()` call is one export job, and plans include an hourly export budget (`GET /vault/usage` shows where you stand), so space bulk pulls out rather than firing them in a burst.

```python
df = client.history("AAPL", timeframe="1m", start="2015-01-01")   # candles
df = client.history("EUR/USD")                                    # the raw tick tape
df = client.dataset("insider_trades")                              # a whole reference set
df = client.economics("fdtr")                                      # one macro series, full depth
client.datasets("crypto")                                          # what the vault holds per class
```

## Options

Start from a ticker or a company name and get the chain, then drill into one contract. The chain gives you each contract's ticker, and the SDK builds one from its parts when you address a contract directly.

```python
chain  = client.options("apple", type="call", max_dte=30)
prints = client.options_flow("NVDA", min_premium=100_000)
bars   = client.option_candles("AAPL", strike=300, expiry="2026-06-12", type="call")
names  = client.options_underlyings()
```

`options()` returns the chain: one row per contract with the latest price, implied volatility, greeks, and the volume and premium traded today. `options_flow()` returns individual prints with premium and greeks at print time; omit the underlying to see every name at once, and use `start`/`end` to reach older prints, which the vault keeps. `option_candles()` returns 1 minute bars for a single contract and accepts either an OSI ticker from the chain or the parts, in which case the SDK builds the ticker. Implied volatility and greeks come from our own pricing models.

For live option ticks over the WebSocket, `subscribe_options(["AAPL"])` delivers every AAPL contract on one subscription, parsed into `OptionTick` objects.

## Find instruments

`catalog()` lists everything you can stream or download, live from the vault: one row per dataset and symbol with its name, category, tick count and history span.

```python
client.catalog()              # every instrument, 22,000+ rows
client.catalog("stocks")      # [{"symbol": "AAPL", "name": "Apple Inc.", "category": "Stocks", ...}, ...]
[x["symbol"] for x in client.catalog("forex")]
```

Categories are stock, forex, crypto, etf, commodity, index, options, futures, economics, bonds, volatility, interest rates and currency index. Use a symbol straight in `stream`, `candles` or `history`.

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
