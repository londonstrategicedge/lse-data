# Changelog

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
