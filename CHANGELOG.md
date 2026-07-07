# Changelog

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
    PostgREST-style filters, translating them onto the vault; anything else
    raises an error naming the method to use instead.
  - `economic_calendar()` no longer sends a staleness filter; the vault copy
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
  one week window stay queryable through `options_flow()` with `start`/`end`.
- `LSE(..., timeout=)` sets the REST timeout per client. The default rose from
  30s to 60s because the deepest vault queries (1s candles over long spans)
  can take tens of seconds.

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

- Docstrings no longer state fixed limits or allowances; `vault_usage` was
  dropped in favour of the server-side usage endpoint.

## 0.12.0

Added

- Options over REST. `options("apple")` returns the live chain (one row per
  contract: last price, IV, greeks, today's volume and premium), filterable
  by `type`, `expiry`, `strike` window and DTE window. Underlyings resolve
  from tickers or company names in any case.
- `options_flow()`: recent option prints (time and sales) with premium, IV
  and greeks, across one underlying or the whole tape (`min_premium=250_000`).
- `option_candles()`: 1 minute premium OHLC + greeks history for a single
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
  Non option symbols still produce plain `Tick` objects, and `OptionTick` is a
  `Tick`, so existing callbacks keep working unchanged.
- `tape()`: a ready made tick callback that prints an aligned column table
  (time, underlying, type, strike, expiry, DTE, premium, volume, notional)
  with a one time header. Usage: `client.on("tick", tape())`.

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
- REST calls send a User-Agent so the CDN does not bounce them.
