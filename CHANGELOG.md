# Changelog

## LSE Data 3.0.2

Fixed

- London (`HIK.L`, `VOD.L`) and European (`ASML.EU`) share subscriptions now use the London socket automatically with the default client. Previously they were sent to the default socket, where authentication and subscription succeeded but no share ticks followed. Crypto and other markets can still stream in the same loop or callbacks, including after a reconnect.
- A bare share name such as `HIK` resolves to `HIK.L` when that is its only match in the authenticated catalogue and no exact bare symbol is listed. An exact symbol takes precedence. An ambiguous bare name raises `LSEError` with code `symbol-ambiguous` and lists the explicit spellings; it is never guessed. Use `.L` or `.EU` when the name is absent from the default catalogue. Subscribing and unsubscribing accept these same spellings.

`pip install -U lse-data` installs the fix. A client given an explicit `url=` continues to use that socket alone and requires its symbol spellings. Historical downloads are unchanged.

## LSE Data 3.0.1

Fixes to the 3.0 client; the calls and their names are the same. `pip install -U lse-data`.

Fixed

- Direct `rows()` downloads, including `candles()`, verify the response SHA-256 and declared size before parsing. A damaged or shortened CSV is refused instead of becoming partial history.
- Long weekly and monthly `candles()` reads keep each candle whole. Internal date chunks previously split a week or month into two partial rows. Timestamp-bounded daily reads also stay whole across exchange session boundaries, and intraday chunks join a candle that crosses a session boundary. Requested start/end bounds and the row limit are preserved.
- A `429` could come back as nothing. With `retries=0`, a call that waited out a `429` returned `None` (`usage()` and every JSON call), or raised `no-response` (`rows()`, `candles()`, the reference readers) or `download-missing` (`export()`), although the server would have answered the next ask; a client that had waited once could do the same on a later call with the default retries. Now every call waits a `Retry-After` of two minutes or less once, the wait does not use up a retry, nothing is carried from one call to the next, and a refusal that stands is raised as `LSEError` with the server's own code and sentence (`calls-exhausted`, `connections-exhausted`, `rows-exhausted`, `allowance-exhausted`).
- Crypto streams again from the default client. Crypto moved to `wss://london-ws.londonstrategicedge.com` on 8 November 2026, and `stream(["BTC/USD", "AAPL"])` on the default address then gave one old price for BTC/USD and nothing after it. The client now opens the crypto socket beside the default one, with the same key, for the symbols the catalogue lists as crypto, and their ticks arrive in the same loop and callbacks; `subscribe()` and `unsubscribe()` while connected go to the right socket. A client made with `url=` uses that socket alone, as before. If the crypto socket refuses the key, the `error` event carries its sentence with that address and the other symbols keep streaming.
- A missing-data answer is recognised by its code, whatever its sentence: the server's sentence now begins "No stored data for this selection" and says why.

Changed

- `stream(..., start=...)` and `stream_async(..., start=...)`: replay was removed from the sockets on 25 October 2026, and a `start` left the loop waiting for ticks that never came. The server's refusal is now raised as `LSEError` (410, `REPLAY_REMOVED`) with its sentence. For history use `candles()`, `rows()` or `export()`.
- The README's examples run as written: the Level 2 and Level 3 examples ask for a minute by time, the years-of-candles example asks for five years (about half of the 2,500,000 rows one export may read), the options flow examples name their day.
- The guide (`docs/api.md`) describes the server as it answers now: files are sent at once and not kept (the prepared-job routes answer `404 export-unknown`), paid plans download at 250 Mbit/s, one export reads at most 2,500,000 rows (`413 export-too-wide`), the free plan reads 12,500,000 rows a day (`429 rows-exhausted`), and `/v3/usage` reports both under `rows`.

## LSE Data 3.0

Every read of history now comes from `https://download.londonstrategicedge.com/v3`: eleven markets, over 76,000 instruments and 67 million US options contracts, resumable CSV and Parquet exports checked by SHA-256, macro and reference data, options chains and flow. The websocket, the key and the package are the same. [docs/api.md](https://github.com/londonstrategicedge/lse-data/blob/main/docs/api.md) describes every call.

Kept, with the same names and row shapes

- Streaming: `stream()`, `connect()`, `subscribe()`, `unsubscribe()`, `subscribe_options()`, `unsubscribe_options()`, `on()`, `disconnect()`, `stream_async()`, the `Tick` and `OptionTick` objects.
- `candles()`, `series()`, `economics()`, `bond_yields()`, `economic_calendar()`, `insider_trades()`, `dividends()`, `splits()`, `cot()`, `financial_reports()`, `company_profiles()`, `fundamentals()`, `catalog()`, `datasets()`, `reference()`, `history()`, `dataset()`, `options()`, `options_flow()`, `option_candles()`, `options_underlyings()`, answered by LSE Data 3.0 underneath.
- The command line: `lse auth`, `lse stream`, `lse browse`, `lse datasets`.

Added

- `markets()`, `health()`, `usage()`, `tables()`, `overview()`, `catalogue()`, `pair_page()`, `iter_pairs()`, `pairs()`, `factors()`: the catalogue of every market, with what the key may open and each instrument's stored rows.
- `preview()`, `estimate()`, `rows()`, `export()`, `job()`: the last ten rows of any selection, what an export would weigh, a selection as rows, and the file itself: CSV or Parquet, streamed at once or prepared as a job, resumed with `Range` after a cut, placed only after its size and SHA-256 match. `data=` selects candles, the trade slice (`timeframe="tick"`), trades, quotes, the Level 2 book, Level 3 orders or the Greeks.
- `dataset_export()`: candles with indicators, a companion instrument and local-time filters, computed on the server.
- `options_chain()`, `options_flow_page()`: the chain and the flow as pages with their totals and window.
- `macro_catalogue()`, `iter_macro_series()`, `macro_preview()`, `macro_series()`, `macro_estimate()`, `macro_export()`, `macro_chart_export()`: economic observations, releases and bond yields by country, as rows or files.
- `reference_catalogue()`, `iter_reference_series()`, `reference_preview()`, `reference_estimate()`, `reference_export()`, `reference_rows()`: eighteen reference collections with every native column.
- `load(path)`: a saved CSV or Parquet as a pandas DataFrame (the `frames` extra).
- `LSEError.code` and `LSEError.required_plan`: the server's refusal code (`plan-required`, `history-limit`, `level-required`, ...) and the plan that opens a paid feed.
- `LSE(..., data_url=, retries=, poll_seconds=, wait_for_limit=)`: the data server, how often a failed call is retried, how often a prepared export is polled, and whether a call past the plan's calls a minute waits once and tries again.

Changed

- Symbols are spelt as the catalogue lists them: `EUR_USD` for forex (the name `EUR/USD` is accepted too), `BTCUSDT` for crypto (`BTC/USD` is accepted too), `ESZ6` for a CME contract. A symbol listed in several markets resolves to US stocks unless `market=` says otherwise.
- `candles()` holds its rows in memory and reads a window of dates: pass `start` and `end`, or `limit` for the most recent bars. As in 0.x it returns at most `limit` bars, the first ones from `start` (the newest with `order="desc"`), and reads a long window a span of days at a time until it has them. `start` may be a timestamp when `end` is a date. The row's time key is still `timestamp`; rows also carry `trades`. For years of candles use `history()` or `export()`.
- `series()` takes the catalogue's codes: yields keep their names (`US10Y`); observations use the country-prefixed codes (`US.core_inflation_rate`). The 0.x names `cpi_yoy` and `fdtr` answer as `US.inflation_rate` and `US.interest_rate`. A name that is not a code is searched; a search that matches different series raises `series-ambiguous` with the codes, and `macro_catalogue()` lists them all.
- The reference readers read a collection and filter it: `insider_trades(type=)`, `financial_reports(report_type=, period=)`, `economic_calendar(region=, event=, released_only=)` apply their filters on the rows.
- `catalog()` lists every instrument of the nine listed markets, Nasdaq TotalView included; the two contract markets, US options and CME Globex, are listed by underlying, and `pairs(market, underlying=...)` pages through their contracts. Rows gain `pair_id`; `ticks` is the instrument's stored rows.
- `history()` takes `market=` beside the old `dataset=`, defaults to one-minute candles, needs `start` and `end` for a bounded pull (without them the whole stored history is asked for), and returns the file's path with `dataframe=False`.
- `options_flow()` carries each print's `venue`; `option_candles()` takes `timeframe=`.
- An option contract is accepted with or without the spaces of its OCC symbol: `AAPL  261120C00100000` or `AAPL261120C00100000`.
- `LSEError(status, message)` keeps its two positional arguments; `str(e)` is unchanged.
- The package needs no extra dependency for files; `frames` stays optional.

Removed

- `get()` over the retired `/iso` tables raises `LSEError` (410, `removed`) naming the methods to use.
- `vault_meta()`: `markets()`, `catalogue()` and `reference_catalogue()` answer what it answered.
- Arrow exports: files are CSV or Parquet.

## 0.14.0

Changed

- Every REST read is now served from the LSE vault
  (`api.londonstrategicedge.com/vault/...`) instead of the legacy `/iso`
  endpoints. Method signatures and row shapes are unchanged; what changed is
  what is behind them:
  - `candles()` accepts every vault resolution ("1s", "5s", "15s", "30s",
    "1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mo") and reaches
    the vault's full depth (US stocks back to 2003, FX to 2009, crypto to
    2017). Stock and ETF candles come back split adjusted.
  - `catalog()` reads the live vault catalog (one row per dataset and symbol,
    22,000+ instruments including economics series, bond yield tenors and
    option underlyings) instead of a static file, so it now needs the API key.
    Rows keep `symbol`/`name`/`category` and gain `dataset`, `ticks`, `first`,
    `last` and `country`.
  - `options_underlyings()` also reads the catalog and needs the key.
  - `option_candles()` merges archive bars and live folded prints on the
    server in one call.
  - `get()` still accepts the previously documented `/iso` table names and
    their filters, translating them onto the vault; anything else
    raises an error naming the method to use instead.
  - `economic_calendar()` no longer sends a staleness filter; the vault
    holds only current events.

Added

- `series()`: any (date, value) observation stream in one synchronous call,
  with the class resolved from the catalog: `series("cpi_yoy")` for a macro
  series, `series("US10Y")` for a bond yield tenor. `economics(symbol)` now
  returns those rows instantly instead of running an export job (use
  `history(symbol, dataset="economics")` when you want Parquet).
- New reference readers, same row-dict shape as the rest: `cot()`,
  `financial_reports()` (statements parsed from JSON), `company_profiles()`,
  `fundamentals()` and `bond_yields()`.
- `candles(..., dataset=...)` to pin the asset class when a symbol exists in
  more than one.
- Options history keeps accumulating in the vault: prints older than the old
  one-week window stay queryable through `options_flow()` with `start`/`end`.
- `LSE(..., timeout=)` sets the REST timeout per client. The default rose from
  30s to 60s because the longest queries (1s candles over long spans) can
  take tens of seconds.

Fixed

- Timeouts and connection failures on REST calls and downloads now raise
  `LSEError` like HTTP errors always did, instead of leaking a raw
  `TimeoutError`/`URLError` traceback. One `except LSEError` covers every
  failed call.
- Bare `lse` (and `lse browse`) without an interactive terminal exits with a
  plain one-line message pointing at `lse datasets`, instead of a curses
  traceback. Ctrl+C inside the browser exits quietly.

## 0.13.0

Added

- The vault: `history()` pulls deep history (raw ticks or any of 14 candle
  resolutions) as Parquet through resumable async export jobs; `dataset()`
  downloads whole reference datasets; `economics()` lists and pulls macro
  series; `datasets()` and `reference()` list what the vault holds;
  `vault_meta()` describes its shape. DataFrames come back directly with
  `pip install 'lse-data[frames]'`.
- `lse browse`: a curses TUI over the vault catalog (also the bare `lse`
  default), plus `lse datasets` for a plain listing.

Changed

- `vault_usage` was dropped in favour of the server's usage endpoint.

## 0.12.0

Added

- Options over REST. `options("apple")` returns the live chain (one row per
  contract: last price, IV, greeks, today's volume and premium), filterable
  by `type`, `expiry`, `strike` window and DTE window. Underlyings resolve
  from tickers or company names in any case.
- `options_flow()`: recent option prints (time and sales) with premium, IV
  and greeks, across one underlying or the whole tape (`min_premium=250_000`).
- `option_candles()`: 1-minute premium OHLC + greeks history for a single
  contract, addressed by OSI ticker or by parts
  (`option_candles("AAPL", strike=205, expiry="2026-06-12", type="call")`).
  Archive bars and a live trailing week merge into one continuous series.
- `options_underlyings()`: every underlying with listed options, no key
  required.

## 0.11.0

Added

- `OptionTick`: option contract ticks now arrive as a `Tick` subclass with the
  contract parsed into named fields: `underlying`, `right` ("call"/"put"),
  `strike`, `expiry` (a `date`), plus `dte`, `premium` (alias of `price`) and
  `notional` (price x volume x 100) properties. `print(tick)` is now readable.
  Non-option symbols still produce plain `Tick` objects, and `OptionTick` is a
  `Tick`, so existing callbacks keep working unchanged.
- `tape()`: a ready-made tick callback that prints an aligned column table
  (time, underlying, type, strike, expiry, DTE, premium, volume, notional)
  with a one-time header. Usage: `client.on("tick", tape())`.

Fixed

- `subscribe()` called before `connect()` (or `stream()`) now actually takes
  effect. Previously it added the symbol to the local set but only sent the
  subscribe message if the socket was already open, so the common
  `subscribe(); connect()` pattern silently received nothing for those symbols,
  and symbols added with `subscribe()` were also not restored after a reconnect.
  Authentication now replays the full subscription set, matching how option
  subscriptions were already handled. Passing symbols straight to
  `connect()`/`stream()` was unaffected.

## 0.10.0

Added

- Historical download over REST with the same key used for streaming:
  `candles()`, `economic_calendar()`, `insider_trades()`, `dividends()`,
  `splits()`, and a generic `get()`. Each returns a list of row dicts.
- `catalog()` lists every available instrument (`symbol`, `name`, `category`)
  with optional category filtering. No key or connection required.
- `LSEError` raised on REST API errors (bad filter, rate limit, quota, forbidden
  table).
- `LSE()` reads the `LSE_API_KEY` environment variable when no key is passed.
- `LSE` supports the context manager protocol (`with LSE(...) as client:`),
  which disconnects on exit.
- `py.typed` marker so type checkers use the package's annotations.

Changed

- `Tick.timestamp` is typed as a string (ISO 8601), which is what the server
  sends. Use the new `Tick.datetime` property for a parsed `datetime`.

Fixed

- Streaming no longer hangs or reconnects forever on a bad or over-quota key,
  and `disconnect()` now ends a `stream()`/replay loop cleanly.
- REST calls send a User-Agent header.
