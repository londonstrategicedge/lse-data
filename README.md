# lse-data

The Python client for LSE Data 3.0. One key streams live prices and downloads history.

[![PyPI](https://img.shields.io/pypi/v/lse-data)](https://pypi.org/project/lse-data/)
[![Python](https://img.shields.io/pypi/pyversions/lse-data)](https://pypi.org/project/lse-data/)
[![Licence](https://img.shields.io/pypi/l/lse-data)](https://github.com/londonstrategicedge/lse-data/blob/main/LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/lse-data)](https://pypi.org/project/lse-data/)

## LSE Data 3.0

LSE Data 3.0 is the new version of our market data archive and API, with data integrity as its first priority.

- Level 3 order data: Nasdaq TotalView, CME Globex
- Level 2 book: Nasdaq TotalView
- 24,000 additional pairs
- Over 76,000 instruments across 11 markets
- 67 million US options contracts
- CSV and Parquet exports
- Free plan: 50 GB a month
- Coming soon: London Stock Exchange, Toronto Stock Exchange, Eurex

```bash
pip install lse-data
```

```python
from lse import LSE

client = LSE(api_key="your_key")
for tick in client.stream(["BTC/USD", "AAPL"]):
    print(tick.symbol, tick.price)

path = client.export("us_stocks", "AAPL", "5m", "2026-10-08", "2026-10-14", format="csv")
```

Markets: US, UK and Canadian stocks, forex, crypto, futures, indices, commodities, US options, CME Globex and Nasdaq TotalView, plus macro series, bond yields and 18 reference collections. [docs/api.md](https://github.com/londonstrategicedge/lse-data/blob/main/docs/api.md) lists every call.

One key for streaming and downloads. `client.usage()` shows the plan and its limits.

## Download history

Rows for a quick look, files for everything else.

```python
from lse import LSE

client = LSE(api_key="your_key")

# OHLCV candles as rows. timeframe: 1s, 5s, 15s, 30s, 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1mo
candles  = client.candles("AAPL", "1h", start="2026-10-01", end="2026-10-14")
latest   = client.candles("BTCUSDT", "1d", limit=30, order="desc")

# The last ten rows of anything, and what an export would weigh, before you ask for it
peek     = client.preview("us_stocks", "AAPL", "5m")
size     = client.estimate("us_stocks", "AAPL", "1m", "2021-10-21", "2026-10-21", format="parquet")

# Macro series and yields: (date, value) rows, any series in one call
cpi      = client.series("US.core_inflation_rate", start="2020-01-01")
us10y    = client.series("US10Y")

# Reference collections, as rows
events   = client.economic_calendar(region="US", start="2026-10-01")
insiders = client.insider_trades("WRB", type="P-Purchase")
divs     = client.dividends("AAPL")
splits   = client.splits("NVDA")
cot      = client.cot("GC")
reports  = client.financial_reports("AAPL", report_type="income", period="FY")
profile  = client.company_profiles("NVDA")
```

Each call returns a list of dicts. Errors raise `LSEError`:

```python
from lse import LSEError

try:
    client.export("cme", "ESZ6", "tick", "2026-10-13", "2026-10-13", data="trades")
except LSEError as e:
    print(e.status, e.code, e.message)       # 403 plan-required This feed requires a subscription
    print(e.required_plan)                   # {"market": "cme", "tier": "lite", "name": "CME Group Lite", ...}
```

Rows are held in memory, so keep them to days or weeks. Years of candles, ticks and order books are files.

## Files: CSV and Parquet

`export()` saves a selection as a file and returns its path. Every export is sent whole with its size and SHA-256 and verified when it finishes; a download that drops is requested again from the start. The free plan includes 50 GB a month.

```python
path = client.export("us_stocks", "AAPL", "1m", "2021-10-21", "2026-10-21", format="parquet")                # five years of one-minute candles
path = client.export("us_stocks", "AAPL", "tick", "2026-10-21", "2026-10-21", format="csv")        # the trade slice
path = client.export("nasdaq", "NVDA", "tick", "2026-10-14 14:30:00", "2026-10-14 14:31:00", data="book")   # the Level 2 book, a minute of it
path = client.export("cme", "ESZ6", "tick", "2026-10-13 14:30:00", "2026-10-13 14:31:00", data="orders")    # Level 3 orders, a minute of them
path = client.macro_export("observation", "US.core_inflation_rate", "1957-02-01", "2026-07-01", format="csv")
path = client.reference_export("company_profiles", "AAPL", format="csv")

df = client.history("AAPL", timeframe="1d", start="2020-01-01", end="2026-10-01")   # a DataFrame with the frames extra
```

One export reads at most 2,500,000 rows, on every plan; a wider one is refused with `413 export-too-wide`, which says how many rows it would have read. Five years of AAPL's one-minute candles are about 1.25 million rows. The book and the orders of a busy instrument run to millions of rows a day, so ask for them by time: two UTC timestamps, an hour or less at a time. `estimate()` with the same arguments says how many rows a selection holds before you download it.

The free plan also reads at most 12,500,000 rows a day (`429 rows-exhausted` until 00:00 UTC) and 12.5 GiB a week (`429 allowance-exhausted` until Monday 00:00 UTC), at 50 Mbit/s. A paid plan has neither limit and downloads at 250 Mbit/s. Every plan has one download open at a time; one more answers `429 connections-exhausted` and names the file to wait for. `client.usage()` shows your figures and what is used.

`pip install 'lse-data[frames]'` adds DataFrames to `history()`, `dataset()` and `load(path)`.

CME Globex and Nasdaq TotalView are paid feeds. Their rows need the feed's plan, and a free key gets an error naming the plan. Tick counts, dates, table sizes and estimates are open to every key.

## Options

The whole US options tape, every contract, with implied volatility and the Greeks computed twice a day.

```python
chain  = client.options("apple", type="call", max_dte=30)
prints = client.options_flow("NVDA", min_premium=100_000, start="2026-11-09", end="2026-11-09")
sweep  = client.options_flow(min_premium=250_000, start="2026-11-09", end="2026-11-09", limit=200)
bars   = client.option_candles("AAPL", strike=100, expiry="2026-11-20", type="call")
names  = client.options_underlyings()
```

`options()` returns the chain, one row per contract with the last trade, volume, premium, implied volatility and Greeks. `options_flow()` returns prints with their premium and venue; leave out the underlying to scan every name. `start` and `end` set the window, up to seven days without an underlying; without them it is today (UTC), and a session's prints are stored after it ends, so name the days you want. `option_candles()` returns candles for one contract. `subscribe_options(["AAPL"])` streams every AAPL contract live as `OptionTick` objects, which add `underlying`, `right`, `strike`, `expiry`, `dte`, `premium` and `notional` to a tick. With `from lse import tape`, `client.on("tick", tape())` prints them as a readable table.

## Find instruments

```python
client.markets()                         # every market, with what your key may open
client.catalog("stocks")                 # one dict per symbol: {"symbol": "AAPL", "name": "Apple Inc. - Common Stock", "ticks": 1627759459, ...}
client.pairs("us_stocks", q="apple")     # a market's instruments, searched
client.pairs("cme", underlying="SP500", expiry="2026-12-18")   # the contracts of one underlying
client.factors("uk_stocks", "ULVR")      # an instrument's splits and, for a stock, its dividends
client.macro_catalogue("observation", country="US", q="inflation")
client.reference_catalogue()             # the eighteen reference collections
```

Symbols are spelt as the catalogue lists them, in any case: `AAPL`, `EUR_USD`, `BTCUSDT`, `ESZ6`, `AAPL  261120C00100000`. An option contract works with or without the spaces, so `AAPL261120C00100000` is the same contract. Pair names work too, so `EUR/USD` and `BTC/USD` resolve. A symbol in several markets, such as AAPL, resolves to US stocks unless you pass `market=`.

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

Crypto streams from `wss://london-ws.londonstrategicedge.com`; the default socket carries the other symbols of these examples. The client opens the crypto socket beside the default one, with the same key, for the crypto symbols you ask for, so one loop or callback carries both; a plan's connections are counted on each socket. If the crypto socket refuses the key, the `error` event says so with that address and the other symbols keep streaming. `LSE(api_key, url=...)` uses the one socket you name and nothing else; the [WebSocket documentation](https://londonstrategicedge.com/websocket-documentation) lists the sockets and what each carries.

Change subscriptions while connected:

```python
client.subscribe(["SOL/USD"])
client.unsubscribe(["BTC/USD"])
client.subscribe_options(["AAPL"])   # every AAPL contract at once
```

The sockets carry live ticks only. Replay, `stream(..., start=...)`, was removed on 25 October 2026: the server refuses it and the client raises that refusal as `LSEError` (code `REPLAY_REMOVED`). For history use `candles()`, `rows()` or `export()`.

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

The key never goes in a URL: downloads send it in the `X-API-Key` header, the stream in its first message. `LSE` also works as a context manager:

```python
with LSE() as client:
    for tick in client.stream(["BTC/USD"]):
        ...
```

A tick has `symbol`, `price`, `bid`, `ask`, `volume`, `timestamp` (ISO 8601) and `name`; `tick.datetime` is the parsed timestamp.

## Settings

Every setting of `LSE()` is optional:

| Setting | Default | What it does |
|---|---|---|
| `timeout` | `60` | seconds to wait for each HTTP call |
| `retries` | `3` | retries of a call that got no answer, or a 429, 500, 502, 503 or 504 |
| `poll_seconds` | `0.5` | how often a prepared export is polled |
| `wait_for_limit` | `True` | on a `429`, wait the server's `Retry-After` once in a call, when it is two minutes or less, and ask again without using up a retry; `False` never waits |
| `data_url` | `https://download.londonstrategicedge.com` | the LSE Data 3.0 server |
| `url` | `wss://data-ws.londonstrategicedge.com` | the live WebSocket; left at its default, crypto symbols are streamed from `wss://london-ws.londonstrategicedge.com` beside it |

```python
client = LSE(api_key="your_key", timeout=120, wait_for_limit=False)
```

## Command line

```bash
lse auth lse_live_xxxxxxxxxxxx
lse stream BTC/USD AAPL
lse datasets
lse browse
```

## Upgrading from 0.x

Scripts written for 0.14 keep working: the method names and row shapes are the same, except `get()`. What changed:

- `candles()` holds rows in memory and takes a date window. It still returns at most `limit` bars (5,000 by default), from `start` onwards or the most recent. To page, pass the last bar's `timestamp` as the next `start`; that bar comes back as the first row. For years of bars, use `history()` or `export()`.
- Symbols follow the catalogue. The 0.x spellings still resolve: `EUR/USD` to `EUR_USD`, `BTC/USD` to `BTCUSDT` (crypto is quoted in Tether).
- `series()` takes catalogue codes (`US10Y`, `US.core_inflation_rate`). `cpi_yoy` and `fdtr` still work, as `US.inflation_rate` and `US.interest_rate`. A name matching several series raises `LSEError` with code `series-ambiguous` and lists them.
- `get()` read the retired tables. It now raises `LSEError` (410, `removed`) naming the methods to use.
- `stream(..., start=...)` replayed recent ticks before the live ones. Replay was removed on 25 October 2026; the call raises `LSEError` (410, `REPLAY_REMOVED`).

[CHANGELOG.md](https://github.com/londonstrategicedge/lse-data/blob/main/CHANGELOG.md) lists every change and [docs/api.md](https://github.com/londonstrategicedge/lse-data/blob/main/docs/api.md) every call.

## Licence

This client is MIT licensed. See [LICENSE](https://github.com/londonstrategicedge/lse-data/blob/main/LICENSE).

That licence extends to the library alone and confers no rights in the data
obtained through it. Data retrieved with an LSE key may be used for your own
research, trading and model training, including for commercial purposes. It may
not be redistributed, resold, or otherwise made available to third parties,
whether in bulk or by way of any competing feed, download service or interface
sourced from LSE. Full terms: https://londonstrategicedge.com/terms
