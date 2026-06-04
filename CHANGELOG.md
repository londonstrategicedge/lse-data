# Changelog

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
