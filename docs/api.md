# LSE Data 3.0: the API

LSE Data 3.0 is one HTTPS service at `https://download.londonstrategicedge.com`. Every call is a GET under `/v3`;
the answers are JSON, except the files (CSV or Parquet) and the pictures (WebP). This guide describes every call the
server has, what it takes, what it answers and how it refuses. The examples are real answers from the server, shortened. The
Python client in this repository wraps every call; the last section maps its methods to them.

## How a request works

**The address and the key.** `https://download.londonstrategicedge.com/v3/<call>?<parameters>`, with the key in the
`X-API-Key` header. The server also accepts the key as `?key=` in the query string; use the header, so the key never
sits in a URL. Without a key every call answers `401 key-missing`; with a key the server does not know,
`401 key-unknown`. A key made on the Data page works within a minute, and a key whose subscription ended is kept but
withheld (below).

**Calls a minute.** Every call, successful or not, counts against the key's plan: the free plan allows 60 a minute.
Past it the server answers `429 calls-exhausted` with a `Retry-After` header and `retry_after` in the body, both in
seconds. `GET /v3/usage` shows the figure as `calls_per_minute`.

**What any key may read, and what needs a plan.** The included markets are the nine of the free plan: `us_stocks`,
`uk_stocks`, `ca_stocks`, `forex`, `crypto`, `futures`, `indices`, `commodities` and `us_options`. The paid markets,
CME Globex (`cme`) and Nasdaq TotalView (`nasdaq`), are sold whole from their Lite tier. The catalogue (`/v3/markets`,
`/v3/pairs`, `/v3/catalogue`, `/v3/factors`, `/v3/tables`, `/v3/overview`, `/v3/photo`, the macro and reference lists)
answers any key, and so do previews and estimates of the included markets' candles and Greeks, for any dates. A paid
market's figures answer every key too: counts, dates and sizes, never a row. They are each pair's `tick_count`,
`tick_first` and `tick_last`, each market's `ticks` and `stored_rows` in `/v3/overview` and an instrument's history
there, `/v3/tables`, the largest list, and `/v3/estimate`. For a key without the plan, the estimate answers the
selection's stored rows by month, with `basis` saying what was counted and `download` the refusal a download would
meet. A paid market's rows, its candles included, need the plan. Everything else is checked in this order:

1. **The key must belong to an account.** A key that belongs to no account answers `403 account-required` on every
   download, preview of trades, quotes, orders, book or a paid market's candles, estimate of an included market's
   trade slice, options call, macro or reference read.
2. **The key must not be withheld.** A key whose subscription ended answers `403 plan-ended`; a key beyond the plan's
   key allowance `403 key-withheld`; the message says why, for example "your CME Group subscription has ended; renew
   it to download CME Group data". A withheld key still reads the catalogue and its own usage.
3. **The plan must open the market.** A paid market (CME Group, Nasdaq TotalView) outside the plan answers
   `403 plan-required` with `required_plan` naming the cheapest tier on sale: `{"market": "cme", "tier": "lite",
   "name": "CME Group Lite", "subscribe_url": "/data?tab=subscriptions"}`. An included market the key's plan does not
   open answers `403 market-not-in-plan`.
4. **The market must serve the level and the tier must sell it.** Level 3 orders and the Level 2 book need the level
   on the market, else `404 level-not-served` ("CME Globex does not serve Level 2 yet", with `levels`), and on the
   key's tier, else `403 level-required` naming the cheapest tier that sells it.
5. **The dates must be within the plan's history.** Each tier includes a fixed depth of history per level, counted back
   from today at 00:00 UTC. A request whose `from` is earlier is refused whole with `403 history-limit`: `plan`,
   `level`, `earliest`, `requested_from` and a message naming the next tier that reaches further. The included markets
   have no window.

**Dates and times.** `from` and `to` are either two dates or two UTC timestamps, never one of each (`400 date-mixed`).
A date is `YYYY-MM-DD`, both ends included, and selects exchange trading days. A timestamp is
`YYYY-MM-DD HH:MM:SS` with an optional fraction; a `T` between date and time and a trailing `Z` are accepted; the
upper bound is excluded. A timestamp without seconds is refused `400 time-bad`, a malformed date `400 date-bad`, `from`
after `to` `400 date-order`. For CME Globex and the `futures` market, dates with no session answer `404 date-not-trading`.
Daily, weekly and monthly candles follow exchange sessions.

**Markets and symbols.** A market is named by its folder as `/v3/markets` lists it: `us_stocks`, `uk_stocks`,
`ca_stocks`, `forex`, `crypto`, `futures`, `indices`, `commodities`, `us_options`, `cme` and `nasdaq`.
`opra` is a second name for `us_options`. A market switched off or coming soon (Eurex, London Stock Exchange and
Toronto Stock Exchange are coming soon) answers `404 market-not-enabled` with its label; a folder the server does not know
`404 market-unknown`. A symbol is spelt as `/v3/pairs` lists it, in any case: `aapl` finds AAPL. Every pair also has a
number, `pair_id`, which may be given instead of the symbol; Nasdaq TotalView uses the numbers of `us_stocks`, so
`2000008377` is NVDA in both. US option contracts are named by their OCC symbol as the
catalogue spells it, the root padded to six characters (`SPY   261016C00780000`).

**Answers.** A JSON answer is an object; lists are paged with `limit` and `offset`, and a page says `total`, `count`,
`limit` and `offset`. A refusal is an object with `code` and `message`, plus the fields named above where they apply,
and its HTTP status says the kind: 400 the request is wrong, 401 the key, 403 the plan, 404 nothing there, 410 gone,
413 too wide, 416 range past the end, 429 too many, 502 the data store refused, 503 busy or unavailable, 504 timed out.
The refusal codes are listed at the end. Browsers may call the server only from LSE's own website, so call it from
your own code, not from a web page on another site.

**Files.** An export answers the bytes at once: `200` with `Content-Disposition` naming the file, `Content-Length`,
`x-checksum-sha256` (the file's SHA-256), `x-export-id` (the server's name for this export in its log), `x-export-rows`
(the rows it charged) and `x-lse3-adjusted` 1 or 0. The file is built whole before the first byte goes out, so the
size and checksum are exact, and nothing is kept once it has been sent (since 2026-11-06; before it a large or slow
export became a prepared job kept 24 hours). `prepare=1` is accepted and ignored. `/v3/jobs/<id>` and
`/v3/jobs/<id>/file`, the prepared-file routes of before, answer `404 export-unknown`: a download that was cut is
requested again from the start. A selection with no stored rows answers `404 data-unavailable` instead of an empty
file; its message begins "No stored data for this selection" and says why. Parquet files type `size` as double,
because split-adjusted volumes can be fractional.

**One download at a time.** A key has one download open at a time, on every plan. While an export of this key is
being built or sent, one more `/v3/export`, `/v3/dataset/export`, macro or reference export answers
`429 connections-exhausted` (`429 exports-busy` while the first is still building), with the message "One download at a
time. <file> is still coming." naming the file to wait for, and `Retry-After: 5`; the same request once the first is
done goes through. The bytes are paced per key: 50 Mbit/s on the free plan, 250 Mbit/s on a paid plan.
`GET /v3/usage` shows the pair as `rate_mbit` and `connections`, and `open` says whether a download is running now.

**Rows an export and rows a day.** Every export is measured in rows before it is built, and the plan sets two
figures, which `GET /v3/usage` reports under `rows`:

| Plan | Rows one export may read | Rows a day |
|---|---|---|
| Free | 2,500,000 | 12,500,000, reset at 00:00 UTC |
| Paid tiers | 2,500,000 | no daily limit |

An export past the first answers `413 export-too-wide` before anything is built, with both figures in the sentence;
so does one the server could not measure within 10 seconds. Past the rows a day it answers `429 rows-exhausted`, with
`Retry-After` and `retry_after` giving the seconds to 00:00 UTC. Nothing is charged for a selection with no rows or
for a refused export. Ask for a long history in pieces: `/v3/estimate` says how many rows a selection holds.

```text
GET /v3/export?market=us_stocks&symbol=AAPL&timeframe=1m&from=2003-01-01&to=2026-10-24&format=csv
413  {"code": "export-too-wide", "message": "This export would read 4,132,206 rows; your plan allows at most 2,500,000 rows an export. Choose shorter dates."}

429  {"code": "rows-exhausted", "message": "Your plan allows 12,500,000 rows a day; 11,900,000 are used and this export would read 6,743. It resets at 00:00 UTC, in 5 h 12 min.", "retry_after": 18720}
```

The free plan also has a weekly allowance of 12.5 GiB, counted on the bytes of its exports; past it every export
answers `429 allowance-exhausted` until Monday 00:00 UTC. A paid plan has none.

**What an answer says about you.** No answer carries an email, a user id or a person's name. The one thing a caller
learns about itself is in `/v3/usage`: the key's own name as it was typed on the Data page, its plan and its product.

## The calls

Every call is `GET https://download.londonstrategicedge.com<path>` with the key in `X-API-Key`. The parameters are
query-string fields unless the table says they are in the path.

### Service and keys

#### GET /v3/health

Is the data server up. No parameters. Answers `ok`, `service`, `version` and `markets` (how many are enabled).

```text
GET /v3/health
{"ok": true, "service": "...", "version": "...", "markets": 11, ...}
```

#### GET /v3/usage

What this key may do and has used. No parameters. Answers `key` (the key's name as it was typed on the Data page),
`plan`, `product`, `markets` (the enabled markets the key opens), `rate_mbit`, `connections` (downloads at once),
`open` (downloads running now), `bytes_since_start`, `calls_per_minute`, `levels` and `history` per paid market (`l1`,
`l2`, `l3` months, -1 the whole archive, and the tier's label), `rows` (`per_export`; `per_day`, -1 for a paid plan;
`used_today`; `day`; `resets_in_seconds`), `weekly` (`allowance_bytes`, -1 for a paid plan; `used_bytes`; `week` as
`2026-W46`) and `withheld` (null, or the sentence that explains why every download is refused).

```text
GET /v3/usage                                                   (a free key)
{"key": "Default", "plan": "free", "product": "global",
 "markets": ["us_stocks", "uk_stocks", "ca_stocks", "forex", "crypto", "futures", "indices", "commodities", "us_options"],
 "rate_mbit": 50.0, "connections": 1, "open": 0, "bytes_since_start": 29818, "calls_per_minute": 60, "levels": {}, "history": {},
 "rows": {"day": "2026-11-11", "per_day": 12500000, "per_export": 2500000, "resets_in_seconds": 61553, "used_today": 996029},
 "weekly": {"allowance_bytes": 13421772800, "used_bytes": 14457484, "week": "2026-W46"}, "withheld": null}
```

A key of a paid tier reads `"rate_mbit": 250.0`, `"rows": {"per_day": -1, "per_export": 2500000, ...}` and
`"weekly": {"allowance_bytes": -1, ...}`.

A key whose subscription ended reads `"plan": "cme:ended", "markets": [], "withheld": "your CME Group subscription has
ended; renew it to download CME Group data"`.

### Catalogue

#### GET /v3/markets

Every market the server serves and lists, with what this key may open, and the coming-soon markets apart. No
parameters. Answers `plan` (the key's), `markets[]` and `coming_soon[]`. A market has `folder`, `code`, `name`,
`label`, `time_zone`, `pairs` (how many `/v3/pairs` lists), `underlyings` (options and futures), `allowed` (this key
may download it), `required_plan` (a paid market's cheapest tier, else null), `levels` (`L1`, `L2`, `L3`), `product` and `capabilities`: `candles`, `tick`, `trades`,
`quotes`, `orders` (Level 3), `book` (Level 2), `greeks`. A coming-soon market has `status: "coming_soon"`,
`enabled: false`, its `label`, `levels`, `product`, `required_plan` and a `note`.

```text
{"code": "XLON", "folder": "london_stock_exchange", "label": "London Stock Exchange", "status": "coming_soon", "enabled": false,
 "levels": ["L1"], "product": "lse", "required_plan": {"market": "london_stock_exchange", "tier": "lite", "name": "London Stock Exchange Lite", ...},
 "note": "trades and bars from 2021-10-29; no quotes, no order book", "time_zone": "Europe/London"}
```

#### GET /v3/pairs

A market's instruments, searched and paged; without a market, one search across every enabled market in a fixed
order; for `us_options`, pages by cursor; for `cme` and `us_options`, grouped by underlying.

| Parameter | Meaning |
|---|---|
| `market` | the folder; empty for a search across every market |
| `symbol` | exactly this symbol, any case |
| `q` | a search in symbol and name (for `us_options`, at most 128 characters) |
| `limit` | rows a page, 0 to 10000; 100 when not given |
| `offset` | rows to skip |
| `sort` | `symbol` (default) or `popular`: a short editorial list first (AAPL, NVDA, MSFT on `us_stocks`; the nearest contracts on `cme`), then the rest by symbol; `sort_basis` says `symbol`, `editorial` or `search_relevance` |
| `cursor` | `us_options` only: the page's `next_cursor`, passed back unchanged with the same filters |
| `group_by` | `underlying`, on `cme` or `us_options`: lists underlyings instead of contracts |
| `underlying` | on `cme` or `us_options`: the contracts of one underlying, named by the `id` the grouped listing gives: a CME family such as `SP500` (the ES and MES products) or `CRUDEOIL`, an option root such as `AAPL`. The page adds `expiries`, `products`, `instrument_types` and `has_more` |
| `product`, `expiry`, `instrument_type`, `option_type` | with `underlying`: one product, one expiry day (`YYYY-MM-DD`), `future`/`option`/`spread`/`strategy`/`other`/`all`, `call`/`put`/`all` |

A page answers `market`, `total`, `count`, `limit`, `offset`, `sort`, `sort_basis` and `pairs[]`. A pair has `folder`,
`symbol`, `name`, `pair_id`, `company_id` and `photo` (when it has them: `light` and `dark` paths under `/v3/photo`),
`active`, `listed_date`, `delisted_date`, `splits[]` (`date`, `price_factor`, `volume_factor`, `type`),
`availability` (`candles` and `tick`, each `available` true, false or null with a `reason`), and its stored tick count:
`tick_count`, `tick_count_basis` (`stored_rows`, or `stored_trades` for an option contract), `tick_count_status`
(`counted`, `not-collected`, `unavailable`), `tick_count_as_of`, `tick_first`, `tick_last`, for every key. `us_options` pages add `next_cursor` and `has_more`; the first page of
a contract catalogue is read with `limit` alone, the next with its `next_cursor`.

```text
GET /v3/pairs?market=us_stocks&q=apple&limit=25
{"market": "us_stocks", "total": 6, "count": 6, "limit": 25, "offset": 0, "sort": "symbol", "sort_basis": "search_relevance",
 "pairs": [{"folder": "us_stocks", "symbol": "AAPL", "name": "Apple Inc. - Common Stock", "pair_id": 2000000040, "company_id": 412,
            "photo": {"light": "/v3/photo/company/412/light?v=76aae5e90b7e", "dark": "/v3/photo/company/412/dark?v=76aae5e90b7e"},
            "active": true, "splits": [{"date": "2020-08-31", "price_factor": 0.25, "type": "split", "volume_factor": 4.0}],
            "tick_count": 1627759459, "tick_count_basis": "stored_rows", "tick_count_status": "counted",
            "tick_count_as_of": "2026-10-22T15:00:00Z", ...}, ...]}

GET /v3/pairs?market=us_options&limit=5
{"market": "us_options", "count": 5, "has_more": true, "next_cursor": "[\"1BOX  200228C00016000\",6039808675]", ...}

GET /v3/pairs?market=cme&group_by=underlying&sort=popular&limit=3
{"market": "cme", "group_by": "underlying", "sort": "popular", "sort_basis": "editorial", "total": 7268, "has_more": true,
 "underlyings": [{"id": "SP500", "symbol": "SP500", "name": "S&P 500", "products": ["ES", "MES"], "contract_count": 391, "active_contract_count": 58},
                 {"id": "NASDAQ100", "name": "Nasdaq 100", "products": ["MNQ", "NQ"], ...}, {"id": "DOW", "name": "Dow Jones Industrial Average", ...}]}
```

```text
GET /v3/pairs?market=cme&underlying=SP500&expiry=2026-12-18&instrument_type=future&limit=3
{"market": "cme", "underlying": "SP500", "total": 2, "pairs": [{"symbol": "ESZ6", "pair_id": 4000000001, "name": "ES futures 2026-12", ...}, {"symbol": "MESZ6", "pair_id": 4000000013, ...}],
 "expiries": [...], "products": [...], "instrument_types": [...], "has_more": false, ...}

GET /v3/pairs?market=us_options&group_by=underlying&sort=popular&limit=3
{"market": "us_options", "group_by": "underlying", "total": 14119, "underlyings": [{"id": "AAPL", "symbol": "AAPL", "name": "Apple Inc. - Common Stock", ...}, {"id": "NVDA", ...}, {"id": "MSFT", ...}]}
```

Spellings: forex symbols carry an underscore, `EUR_USD`, while the name is `EUR/USD`; `q=EUR/USD` finds the pair
through its name, `symbol=EUR/USD` finds nothing. Crypto pairs are quoted in Tether, `BTCUSDT`. An `offset` past the
end is an empty page, not a refusal.

#### GET /v3/catalogue

What the archive holds: with `largest=N` (1 to 50) the N symbols with the most stored rows across
the shown markets; without `market` or `largest`, the `/v3/markets` answer
with `count` and `exchanges[]` added; with `market`, the `/v3/pairs` page with `symbols` as a copy of `pairs`.
`q` (at most 64 characters), `limit` (0 to 10000, 500 when not given) and `offset` as `/v3/pairs`.

```text
GET /v3/catalogue?largest=5
{"count": 5, "largest": [{"exchange": "cme", "symbol": "SR3Z6", "name": "SR3 futures 2026-12", "pair_id": 4000000054, "level": "L1",
                          "rows": 2665744576, "first": "2025-09-28", "last": "2026-10-11"}, ...]}
```

Every key gets the same answer.

#### GET /v3/factors

A pair's splits and, for a share or fund in `us_stocks`, `uk_stocks`, `ca_stocks` or `nasdaq`, its dividends.
`market` and `symbol` or `pair_id`. Answers `market`, `pair_id`, `symbol`, `splits[]` and, for those four markets,
`dividends`: `status` (`ok`, `none`, `unlinked`, `unavailable`), `unit` (the unit the amounts are quoted in; `GBX` is
pence), `as_of`, `last_added`, `count`, `truncated` (more than 500 rows exist) and `rows[]` newest first, each with
`ex_date`, `amount`, `payment_date`, `record_date`, `declaration_date`, `frequency` and `special`.

```text
GET /v3/factors?market=uk_stocks&symbol=ULVR
{"market": "uk_stocks", "symbol": "ULVR", "pair_id": 1000007737,
 "splits": [{"date": "2006-05-22", "price_factor": 2.2222222222222223, "volume_factor": 0.45, "type": "consolidation"}],
 "dividends": {"status": "ok", "unit": "GBX", "as_of": "2026-10-24T05:35:34Z", "last_added": "2026-09-07T13:37:29Z", "count": 103,
               "rows": [{"ex_date": "2026-08-06", "amount": 39.82, "payment_date": "2026-09-18", "record_date": "2026-08-07", "frequency": "Quarterly", "special": false}, ...]}}
```

#### GET /v3/tables

The stored tables of one market and their size, for any key: `market`. Answers `market`
and `tables[]` with `table`, `rows`, `bytes`, and `first` and `last` month (`YYYYMM`).

```text
GET /v3/tables?market=us_stocks
{"market": "us_stocks", "tables": [{"table": "bars", "rows": 38599560040, "bytes": 455150770805, "first": "200309", "last": "202610"}]}
```

#### GET /v3/photo/{kind}/{key}/{theme}

A company's or a pair's picture, as the `photo` field of a pair links it: `/v3/photo/company/412/light`,
`/v3/photo/pair/forex-10000000026/dark`. A WebP with `ETag` and `Cache-Control: public, max-age=86400`; the `?v=` the
link carries is a cache key the server ignores. Needs a key like every call; takes no download slot. `theme` other
than `light` or `dark` answers `404 theme-unknown`; a key with no picture `404 photo-unknown`.

#### GET /v3/overview

The country overview and featured archives: `stats`, `markets[]` (with `contracts` for options and futures,
`stored_rows`, and `ticks` with `ticks_day`: every stored row of the market on the day of the newest daily
count, for every market and every key), `countries`, `archives[]` (familiar symbols with
their tick count as `rows`, `first`, `last`, `ticks_day`) and `as_of`. The totals in `stats` and `countries` cover
every market. With `market` and `symbol` or `pair_id`, `history` is added for that instrument: `rows`, `first`, `last`
of its stored 1m candles and `as_of`, the time they were built, for every key. They are rebuilt every hour, so `last`
can trail the newest data by up to an hour.
A pair with no data mapping answers `rows` 0. A market whose figures are not ready yet, or a read that fails, answers
null bounds with `"note": "history bounds are temporarily unavailable"`; bounds older than a day carry
`"note": "history bounds were last refreshed more than a day ago"`. `timeframe` other than `1m` is refused
`400 timeframe-bad`.

### Options

Both calls read `us_options`, one of the included markets, and need your own key.

#### The market: us_options

`us_options` is OPRA's tape: every trade of every listed US option contract as the exchanges print it, with the
venue letter, stored since October 2025. It is one of the included markets. `/v3/markets` reports `levels: ["L1"]` and
`capabilities` with candles, tick and greeks true, and trades, quotes, orders and book false. It lists over 67 million
contracts of some 14,000 underlyings. What it answers:

- **The contracts.** `/v3/pairs?market=us_options`, paged by cursor; `underlying=AAPL` with `option_type`, `expiry`
  and `product`; `group_by=underlying` for the roots. A contract is named by its OCC symbol with the root padded to six
  characters (`AAPL  261120C00100000`) or by its `pair_id`.
- **Candles at every timeframe**, built from the trades.
- **The trade slice.** `timeframe=tick` with the default `data=candles` answers the trades as `time, price, size,
  kind`, for example `2026-10-21 13:34:17.213383276 | 239.25 | 6 | trade`. `data=trades` belongs to the paid feeds and
  here answers `400 data-unavailable`.
- **No quotes.** The tape carries trades only, so `quotes` is false and the book and orders do not exist for it.
- **The Greeks.** `data=greeks`, one row per traded minute, computed twice a day; `greeks_as_of` in the chain says
  when the newest were computed.
- **The chain and the flow**, below. Each print of the flow carries its `venue` letter (`A` is NYSE American) and its
  premium, for example `AAPL  270416P00300000 | 2026-10-22 19:59:58.091147533 | 7.33 x 340 | premium 249,220`.
- **Factors.** `/v3/factors` on a contract answers the underlying company's split rows (AAPL's 2020-08-31 split for an
  AAPL contract listed years later).
- **Another name.** `market=opra` is accepted as another name for `us_options`.

A contract that did not trade on the days asked answers `rows` 0 with a `message` (estimate) or
`404 data-unavailable` (export).

#### GET /v3/options/chain

The listed, unexpired contracts of one underlying. `underlying` (a root, up to six characters) is required; `type`
(`call` or `put`), `expiry` (`YYYY-MM-DD`), `strike`, `strike_min`, `strike_max`, `min_dte`, `max_dte` narrow it;
`limit` 1 to 5000 (1000 when not given) and `offset` page it. Answers `underlying`, `underlying_price` and
`underlying_at` (the underlying's last trade in US stocks, `us_stocks`, today or else within the last week; null when the
root is not a US stock, such as SPX or VIX), `contracts` (matched), `returned`, `limit`, `offset`,
`greeks_as_of` and `rows[]` ordered by expiry, strike and type, each with `ticker`, `symbol`, `pair_id`,
`contract_type`, `strike`, `expiry`, `dte`, `last_price`, `last_trade_at`, `volume_today`, `premium_today`, and the
Greeks of the newest computed day: `iv`, `delta`, `gamma`, `theta`, `vega`, `greeks_underlying_price`, `greeks_at`
(null for a contract that did not trade that day). The Greeks are computed twice a day, so on a trading day they are
at least a session old.

```text
GET /v3/options/chain?underlying=AAPL&type=call&max_dte=30&limit=50
{"underlying": "AAPL", "contracts": 518, "returned": 50, "limit": 50, "offset": 0, "greeks_as_of": "2026-10-21 19:57:52.725026314",
 "rows": [{"ticker": "AAPL261023C00110000", "symbol": "AAPL  261023C00110000", "pair_id": 6008463023, "contract_type": "call", "strike": 110.0,
           "expiry": "2026-10-23", "dte": 0, "last_price": 235.0, "last_trade_at": "2026-10-21 17:42:45.587198596", "volume_today": 0, "premium_today": 0.0,
           "iv": null, "delta": null, ...}, ...]}
```

A long chain can take seconds to answer; page it with `limit` and `offset` rather than asking for thousands of rows
at once.

#### GET /v3/options/flow

Prints of the OPRA tape with their premium (price × size × 100), newest first, each with `ts`, the contract, `price`,
`size`, `premium`, `venue` and the Greeks of its minute where that day's figures are computed. With `underlying`, the
contract filters of the chain apply; without it the call sweeps the whole tape and takes `min_premium`, `from`, `to`,
`order` and `limit` only, over at most seven days. `from` and `to` are dates or UTC timestamps, `to` excluded, a date
as `to` meaning that whole day; the default window is today. `order` is `desc` (default) or `asc`; `limit` 1 to 5000.

```text
GET /v3/options/flow?underlying=AAPL&min_premium=100000&from=2026-10-22&to=2026-10-22&limit=2
{"underlying": "AAPL", "from": "2026-10-22 00:00:00", "to": "2026-10-23 00:00:00", "order": "desc", "limit": 2, "offset": 0, "returned": 2,
 "rows": [{"ts": "2026-10-22 19:59:58.091147533", "symbol": "AAPL  270416P00300000", "ticker": "AAPL270416P00300000", "underlying": "AAPL",
           "contract_type": "put", "strike": 300.0, "expiry": "2027-04-16", "dte": 174, "price": 7.33, "size": 340, "premium": 249219.99999999997,
           "venue": "A", "iv": 0.28817976, "delta": -0.17714728, "underlying_price": 348.28, ...}, ...]}
```

### Macro

The macro catalogue and its exports: economic observations, economic releases and government bond yields, by country.

#### GET /v3/macro

Without `kind`: `kinds[]` (`kind`, `series`, `countries`, `first`, `last`) and `countries[]` (`country`, `series`,
`kinds`). With `kind` (`observation`, `release`, `yield`): a page of series filtered by `country` (two letters),
`code`, `q` (search in code, name and category, at most 128 characters) and `group=credit` (the credit, loan,
mortgage, delinquency and lending series), paged with `limit` (0 to 1000, 100 when not given) and `offset`. A series has
`code`, `country`, `category`, `name`, `unit`, `frequency` with `frequency_source` (`recorded`, `inferred`,
`unavailable`) and `frequency_note`, `first`, `last`, `values` and `stored_records`, `last_value`, `latest_value`,
`latest_at`, `latest_source`; a release adds `released_records`, `scheduled_records`.

```text
GET /v3/macro?kind=observation&country=US&q=core%20inflation&limit=20
{"kind": "observation", "country": "US", "total": 2, "count": 2, "limit": 20, "offset": 0,
 "series": [{"code": "US.core_inflation_rate", "country": "US", "category": "core_inflation_rate", "name": "US Core Inflation Rate", "unit": "percent",
             "frequency": "monthly", "frequency_source": "recorded", "first": "1957-02-01", "last": "2026-09-01", "stored_records": 835, "latest_value": 2.6, "latest_at": "2026-09-01", ...}, ...]}
```

#### GET /v3/macro/preview, /v3/macro/estimate, /v3/macro/series, /v3/macro/export, /v3/macro/values

All take `kind` (`observation` when not given) and `code`; `from` and `to` as `YYYY-MM-DD`, both included, the series'
first and last day when not given. The columns are `day, value` for an observation, `day, close` for a yield,
`released_at, actual, previous, consensus, forecast` for a release (UTC; a scheduled event keeps a null `actual`).

- **preview**: the last ten rows, in time order: `kind`, `code`, `columns`, `rows[]`, `from`, `to`.
- **estimate**: `rows`, `estimated_bytes` (an upper bound; the file is usually much smaller), `mode` and `filename`,
  with `format` (`parquet` or `csv`).
- **series**: every row of the window as JSON, for a chart; above 100,000 rows `413 series-window-too-wide`.
- **export**: the file, with `format` and `prepare`, by the file rules above. `/v3/macro/values` is a second name for it.

```text
GET /v3/macro/estimate?kind=observation&code=US.core_inflation_rate&format=csv
{"rows": 835, "estimated_bytes": 493056, "mode": "stream", "filename": "US.core_inflation_rate_macro_observation_1957-02-01_2026-09-01.csv"}

GET /v3/macro/preview?kind=observation&code=US.core_inflation_rate
{"kind": "observation", "code": "US.core_inflation_rate", "columns": ["day", "value"], "from": "1957-02-01", "to": "2026-09-01",
 "rows": [{"day": "2025-12-01", "value": 2.6}, ..., {"day": "2026-09-01", "value": 2.6}]}
```

#### GET /v3/macro/chart/export

One to six observation series on their own dates, as `level`, `yoy` or `mom` change (`mode`), between `from` and
`to` (both required, `YYYY-MM-DD`), as `csv` (default) or `parquet`; `codes` is comma-separated and distinct. The file
has `date` and one column per series, named after the code with every character other than letters, digits and `_`
replaced by `_`. Year-on-year and month-on-month compare with the latest observation on or before 360 or 28 days
earlier; a missing or zero base leaves the cell blank.

### Reference

The research and reference collections: company profiles, financial reports, insider trades, dividends, bond yields
daily and intraday, the economic calendar, Commitments of Traders and the rest, 18 collections in all.

#### GET /v3/reference

Without `dataset`: `datasets[]` with `dataset`, `name`, `category`, `rows`, `series`, `first`, `last`, `as_of`,
`columns` and `time_column`. With `dataset`: that collection's series, filtered by `symbol`, `country` and `q`, paged
with `limit` (0 to 1000, 100 when not given) and `offset`: `total`, `count`, `series[]` (`symbol`, `name`, `country`,
`rows`, `first`, `last`, and `company_id` with `photo` where the series names a company), `countries[]`, `columns`,
`first`, `last`, `as_of`.

#### GET /v3/reference/preview, /v3/reference/estimate, /v3/reference/export

`dataset` is required; `symbol` selects one series (omit it for the whole collection), `country` and `q` filter, `from`
and `to` (`YYYY-MM-DD`, included) bound the collection's time column; omitting them selects the whole archive,
including rows with no date. The preview answers `dataset`, `symbol`, `columns` and the last ten `rows[]` in time
order; the estimate `rows`, `estimated_bytes` (an upper bound; the file is usually much smaller), `mode`, `filename`;
the export every column as `csv` or
`parquet` (`format`), with `prepare`.

```text
GET /v3/reference/estimate?dataset=company_profiles&symbol=AAPL&format=csv
{"rows": 1, "estimated_bytes": 70488, "mode": "stream", "filename": "AAPL_company_profiles_first_latest.csv"}
```

A company profile carries the company's own public details: `ceo`, `address`, `phone`, `website`, `isin`, `cusip`
and 31 more columns.

### Market data

The five calls that read prices take one selection, the same fields for all of them.

| Parameter | Meaning |
|---|---|
| `market` | the folder; required |
| `symbol` or `pair_id` | the instrument, as `/v3/pairs` spells it; required except for a preview of the latest rows |
| `data` | `candles` (default), `trades`, `quotes`, `orders` (Level 3), `book` (Level 2), `greeks` (US options) |
| `timeframe` | `tick`, `1s`, `5s`, `15s`, `30s`, `1m` (default), `3m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1w`, `1mo`; `tick` with `candles` is the trade slice; `greeks` need `1m` or longer |
| `from`, `to` | two dates or two UTC timestamps, as above |
| `format` | `parquet` (default) or `csv` |
| `adjusted` | `1` (default) adjusts prices and volumes for splits; `0` gives them as published; `as_published=1` does the same |

Trades, quotes, orders and book exist on the paid feeds only; on the included markets they answer
`400 data-unavailable` ("This market offers timeframes, including tick"). Forex and commodities are priced from quotes,
not trades, so they have candles but no trade slice: `tick` on them answers `404 data-unavailable`. Candles are `time, open, high, low, close, volume, trades`; a daily, weekly or
monthly candle's `time` is the start of its first minute with a trade, in UTC (AAPL's 1 October 2026 candle reads
`2026-10-01 04:02:00`), and it carries one more column, `date`: the session's date, or the first day of its week or month. Trades are `time, price, size, kind`; quotes `time, bid_price, ask_price, bid_size, ask_size`;
orders `time, action, side, order_id, price, size`. The book is 67 columns: `time`, `sequence`, `action`, `side`, `price`, `size`, `depth`, then
ten bid and ten ask levels of price, size and orders. Greeks are one row per traded minute with `price_at`, `iv`,
`delta`, `gamma`, `theta`, `vega`, `option_price`, `price_source`, `underlying_price`, `years_to_expiry`, `rate` and
`dividends_pv`; a figure the formula cannot give is null (`\N` in CSV).

#### GET /v3/preview

The last ten rows of a selection, or the rows of a short window. Without `from` and `to` the server finds the newest
ten buckets itself. The included markets' candles and Greeks answer any key for any dates; a paid market's candles and all
other data follow the plan checks, and so does an estimate. Answers
`market`, `symbol`, `timeframe`, `data`, `adjusted`, `columns[]`, `rows[]` in time order, `availability`, and
`message` when there are no rows.

```text
GET /v3/preview?market=us_stocks&symbol=AAPL&timeframe=5m&data=candles
{"market": "us_stocks", "symbol": "AAPL", "timeframe": "5m", "data": "candles", "adjusted": true, "columns": ["time", "open", "high", "low", "close", "volume", "trades"],
 "rows": [{"time": "2026-10-22 23:10:00", "open": 348.34, "high": 348.35, "low": 348.155, "close": 348.26, "volume": 873, "trades": 42}, ... ten rows ...], "message": null}
```

#### GET /v3/estimate

The rows and bytes an export would have. Answers `rows`, `basis` (what was counted), `estimated_bytes` (the expected
size of the file, usually a little above it), `ceiling_bytes` (the most the file can weigh), `mode` (`stream` or
`file`, a field kept from the prepared jobs of before: every export is now sent at once), `filename`, `availability`
and `message`. `rows` is the figure the rows limits are judged on. Without `from` and `to` it describes the same
newest-ten-rows window the preview shows, with `latest_latest` in the file name.

A selection of a paid market the key may not download (no plan, a level the tier does not sell, dates before the plan's
window) is answered all the same: `rows` is the selection's stored rows in the months its
dates touch, `basis` says what was counted, and `download` is the refusal a download would
meet (`status`, `code`, `message`, `required_plan` and the rest of its body). Where a market's rows are not counted
by month, `rows` comes from the instrument's whole-history count for trades or quotes, and is null for orders or book;
`basis` says which. A level the market does not serve is
`404 level-not-served` for every key; when the figures cannot be read, the answer is `503 estimate-unavailable`.

```text
GET /v3/estimate?market=nasdaq&symbol=NVDA&timeframe=tick&data=orders&from=2026-10-01&to=2026-10-23&format=csv   (a free key)
{"rows": 104252800, "estimated_bytes": 8757251584, "basis": "stored orders of NVDA in the months 202610 to 202610, as counted daily",
 "download": {"status": 403, "code": "plan-required", "message": "This feed requires a subscription", "required_plan": {"market": "nasdaq", "tier": "lite", "name": "Nasdaq Lite", ...}}, ...}
```

```text
GET /v3/estimate?market=us_stocks&symbol=AAPL&timeframe=5m&from=2026-10-08&to=2026-10-14&format=csv&adjusted=1&data=candles
{"rows": 1041, "estimated_bytes": 91336, "ceiling_bytes": 265408, "mode": "stream", "filename": "AAPL_us_stocks_5m_2026-10-08_2026-10-14.csv", "message": null, ...}

GET /v3/estimate?market=us_stocks&symbol=AAPL&timeframe=5m&from=2026-10-17&to=2026-10-18     (a weekend)
{"rows": 0, "estimated_bytes": 0, "ceiling_bytes": 0, "mode": "stream",
 "message": "No stored data for this selection: no candles of AAPL (pair 2000000040) are stored in the dates asked (no trading then, or dates not loaded).", ...}
```

#### GET /v3/export, and /v3/candles

The file, by the file rules above. `/v3/candles` is a second name for the same call; `prepare=1` is accepted and
ignored. The answer (200) carries the bytes with `Content-Disposition`, `Content-Length`, `x-checksum-sha256`,
`x-export-id`, `x-export-rows` and `x-lse3-adjusted`. A selection with no rows answers `404 data-unavailable`; an
export past the rows one export may read `413 export-too-wide`; past the free plan's rows a day `429 rows-exhausted`;
past its weekly allowance `429 allowance-exhausted` with the figures: "Your plan's weekly allowance of 12.5 GiB is
used up (12.54 GiB this week); it resets Monday 00:00 UTC. A paid plan has no weekly allowance." A daily, weekly or
monthly candle carries its session's `date` as a last column.

```text
GET /v3/export?market=us_stocks&symbol=AAPL&timeframe=1d&from=2026-10-01&to=2026-10-14&format=csv&adjusted=1&data=candles
200  content-type: text/csv; charset=utf-8   content-length: 851
     content-disposition: attachment; filename="AAPL_us_stocks_1d_2026-10-01_2026-10-14.csv"
     x-checksum-sha256: 05e6d3366eb6c948eb4dbc2460cb7b06c256d7992ec667dd0c88e8578055e812
     x-export-id: 81ec941184f44427ade9e06cf33820bf   x-export-rows: 10   x-lse3-adjusted: 1
"time","open","high","low","close","volume","trades","date"
"2026-10-01 04:02:00",334.18,334.83,325.82,330.79,15666143,265910,"2026-10-01"
"2026-10-02 04:00:00",331.14,334.54,330.17,333.53,15026716,228816,"2026-10-02"
...  10 rows, 851 bytes

GET /v3/export?market=us_stocks&symbol=AAPL&timeframe=5m&from=2026-10-17&to=2026-10-18&format=csv     (a weekend)
404  {"code": "data-unavailable", "message": "No stored data for this selection: no candles of AAPL (pair 2000000040) are stored in the dates asked (no trading then, or dates not loaded)."}

GET /v3/export?market=us_stocks&symbol=AAA&timeframe=1d&from=2026-10-01&to=2026-10-14
404  {"code": "data-unavailable", "message": "..."}
```

With `format=parquet` the same selection is a 4,108-byte Parquet file (`application/vnd.apache.parquet`).

#### GET /v3/jobs/{id}, and /v3/jobs/{id}/file

The prepared-file routes of before 2026-11-06, when a large export became a job that was polled and then fetched.
Exports are no longer kept after they are sent, so every id answers the same:

```text
GET /v3/jobs/21a88b355dc84b6982d4da81328764dc
404  {"code": "export-unknown", "message": "Exports are no longer kept after they are sent. Request the export again."}
```

#### Older calls

- **GET /v3/slice** is the call before `/v3/export` and answers exactly as it does. `table` (`bars`, `trades`,
  `quotes`, `orders`, `book`, `greeks`) stands for `data`, and for bars `bar_seconds` (1, 5, 15, 30, 60, 180, 300, 900,
  1800, 3600, 14400) for `timeframe`: `table=bars&bar_seconds=300` is the 5m file. `table` other than the six answers
  `400 data-bad`, `bar_seconds` outside the list `400 timeframe-bad`.
- **GET /v3/files** and **GET /v3/file/{kind}/{feed}/{a}/{b}** are retired. After the key and plan checks both answer
  `410 export-required` ("Use /v3/export for timeframe downloads and resumable files"); a tree that names no market
  answers `404 file-unknown`.

### Dataset builder

#### GET /v3/dataset/export

Candles with indicators, a companion instrument's columns and local-time filters, computed on the server and delivered by
the file rules. The selection fields of an export (candles only; `tick` and other data answer
`400 dataset-data-bad`), plus:

| Parameter | Meaning | Example | In `dataset_export()` |
|---|---|---|---|
| `indicators` | feature ids with periods, comma-separated | `rsi:14,sma:20` | `indicators=["rsi:14", "sma:20"]` |
| `companion_market` | a market `/v3/markets` lists; both markets are checked against the plan | `us_stocks` | `companion={"market": "us_stocks", "symbol": "MSFT"}` |
| `companion_symbol` or `companion_pair_id` | the companion instrument | `MSFT` | the `symbol` or `pair_id` of `companion` |
| `companion_features` | `close`, `return`, `corr`, comma-separated | `close,return,corr` | `companion_features=["close", "return", "corr"]` |
| `corr_window` | rolling correlation window; 60 when not given | `20` | `corr_window=20` |
| `hour_start`, `hour_end` | inclusive local hours; give both | `10`, `15` | `hour_start=10, hour_end=15` |
| `tz` | IANA zone for the hours and weekdays; UTC when not given | `America/New_York` | `timezone="America/New_York"` |
| `day_of_week` | comma-separated lowercase day names | `monday,tuesday,wednesday,thursday,friday` | `days=["monday", "tuesday", "wednesday", "thursday", "friday"]` |

The companion joins backward in time; indicator warm-up stays empty. An indicator or feature the builder does not
know answers `400 dataset-options-bad`; a companion market without its symbol `400 companion-missing`; a builder that
does not answer within 10 s `503 dataset-unavailable`.

```text
GET /v3/dataset/export?market=us_stocks&symbol=AAPL&timeframe=5m&from=2026-10-08&to=2026-10-13&indicators=rsi:14,sma:20
    &companion_market=us_stocks&companion_symbol=MSFT&companion_features=close,return,corr&corr_window=20
    &hour_start=10&hour_end=15&tz=America/New_York&day_of_week=monday,tuesday,wednesday,thursday,friday&format=csv
200  content-length: 28112   x-checksum-sha256: 18e8d02d2d469c31f5fbd88a482a01d090b6547265a66b0aaf95856d1da3ab35
     content-disposition: attachment; filename="AAPL_us_stocks_5m_2026-10-08_2026-10-13_dataset.csv"
time,open,high,low,close,volume,trades,rsi_14,sma_20,msft_close,msft_return,corr_msft
2026-10-08 14:00:00,337.27,337.66,337.06,337.325,186128.0,3492,,,531.145,,
2026-10-08 14:05:00,337.3106,337.42,336.94,337.12,121757.0,2486,,,530.91,-0.0442,
```

## Paid feeds: what changes with a plan

A key of a CME Group or Nasdaq TotalView tier opens its market's candles, `trades` and `quotes` (Level 1, from Lite),
`book` (Level 2) and `orders` (Level 3) as the tier sells them, within the tier's history months per level, lifts the free plan's weekly allowance
and its rows a day, and paces downloads at 250 Mbit/s instead of 50; one download at a time and 2,500,000 rows an export hold on every plan. Without the tier, its rows
answer `403 plan-required` naming the cheapest tier; its figures (counts, dates, sizes and estimates) answer every key. The pricing page shows an
illustrative sample of each feed's data and schema for anyone. The refusals a paid key meets are
`403 history-limit` (dates before the window, the body naming `earliest` and the next tier), `403 level-required` (a
level the tier does not sell) and `404 level-not-served` (a level the market does not serve: CME Globex serves L1 and
L3, Nasdaq TotalView L1, L2 and L3, as `/v3/markets` reports in `levels`). For example, a key whose tier does not sell
the book is refused `level-required` naming the cheapest tier that does, and any key asking CME Globex for the book is
refused `level-not-served`.

## Refusal codes

The codes the server answers, with their HTTP status. A refusal is `{"code": ..., "message": ...}` plus the fields
named in the text above (`required_plan`, `plan`, `level`, `levels`, `earliest`, `requested_from`, `retry_after`).

| Status | Code | When |
|---|---|---|
| 400 | `code-bad` | a release code without region.name |
| 400 | `code-missing` | a macro call without code |
| 400 | `codes-bad` | none, over six, repeated or malformed |
| 400 | `codes-missing` | /v3/macro/chart/export without codes |
| 400 | `companion-missing` | a companion market without its symbol or pair_id, or the reverse |
| 400 | `country-bad` | not two letters |
| 400 | `cursor-bad` | a cursor not from this server |
| 400 | `data-bad` | data not one of the six |
| 400 | `dataset-data-bad` | data other than candles, or tick |
| 400 | `dataset-options-bad` | an indicator, period, feature or filter the builder does not know |
| 400 | `date-bad` | a 10-character from or to that is not a valid date |
| 400 | `date-missing` | from or to not given |
| 400 | `date-mixed` | a date on one side and a timestamp on the other |
| 400 | `date-order` | from after to |
| 400 | `dte-bad` | a dte not a whole number of days, or min_dte above max_dte |
| 400 | `expiry-bad` | expiry not YYYY-MM-DD |
| 400 | `export-bad` | the built specification is not valid (columns, file name) |
| 400 | `export-kind-bad` | a non-market export that names a market |
| 400 | `filter-bad` | a filter too long (128 characters; 512 for a reference symbol) |
| 400 | `format-bad` | format not parquet or csv |
| 400 | `from-bad` | options flow: from is neither a date nor a UTC timestamp |
| 400 | `group-bad` | group not credit |
| 400 | `group-by-bad` | group_by not underlying |
| 400 | `group-market-bad` | group_by or underlying on a market other than cme or us_options |
| 400 | `instrument-type-bad` | instrument_type not future, option, spread, strategy, other or all |
| 400 | `kind-bad` | kind not observation, release or yield |
| 400 | `largest-bad` | largest not 1 to 50 |
| 400 | `limit-bad` | limit not a whole number up to 10000 |
| 400 | `market-missing` | symbol without market |
| 400 | `max_dte-bad` | max_dte not a whole number of days |
| 400 | `min_dte-bad` | min_dte not a whole number of days |
| 400 | `min_premium-bad` | min_premium not a number |
| 400 | `mode-bad` | mode not level, yoy or mom |
| 400 | `offset-bad` | offset not a whole number |
| 400 | `option-bad` | adjusted, as_published or prepare not 1/0/true/false |
| 400 | `option-type-bad` | option_type not call, put or all |
| 400 | `order-bad` | order not asc or desc |
| 400 | `pair-bad` | pair_id not a number |
| 400 | `pair-required` | us_options without symbol or pair_id |
| 400 | `q-bad` | a search text too long (64 characters on /v3/catalogue, 128 on us_options) |
| 400 | `range-bad` | from after to |
| 400 | `search-bad` | /v3/macro: q over 128 characters |
| 400 | `sort-bad` | sort not symbol or popular |
| 400 | `strike-bad` | a strike not a number, or strike_min above strike_max |
| 400 | `strike_max-bad` | strike_max not a number |
| 400 | `strike_min-bad` | strike_min not a number |
| 400 | `symbol-bad` | us_options: a symbol over 128 characters |
| 400 | `symbol-missing` | neither symbol nor pair_id |
| 400 | `time-bad` | a from or to that is neither a date nor a full timestamp with seconds |
| 400 | `timeframe-bad` | a timeframe the call does not take (on /v3/overview, anything but 1m) or a bar_seconds outside the list |
| 400 | `to-bad` | options flow: to is neither a date nor a UTC timestamp |
| 400 | `type-bad` | type not call or put |
| 400 | `underlying-bad` | over 6 characters or odd characters |
| 400 | `underlying-missing` | /v3/options/chain without underlying |
| 400 | `underlying-required` | contract filters on a sweep |
| 400 | `window-bad` | to not after from |
| 400 | `window-too-wide` | a sweep over more than 7 days |
| 400, 404 | `data-unavailable` | 400: trades, quotes, orders or book on an included market, or greeks outside us_options; 404: tick on a quotes-only market, a pair with no data yet, or a selection with no stored rows (the message begins "No stored data for this selection" and says why) |
| 401 | `key-missing` | no key in X-API-Key or ?key= |
| 401 | `key-unknown` | a key the server does not know |
| 403 | `account-required` | a key that belongs to no account |
| 403 | `history-limit` | from before the plan's window for the level; earliest, requested_from and the next tier are in the body |
| 403 | `key-withheld` | the key is past the plan's key allowance (withheld) |
| 403 | `level-required` | orders or book on a tier that does not sell the level; required_plan names the cheapest that does |
| 403 | `market-not-in-plan` | an included market the key's plan does not open |
| 403 | `plan-ended` | the key's subscription ended (withheld) |
| 403 | `plan-required` | a preview or an export of a paid market the key's plan does not open (its candles, trades, quotes, orders or book); required_plan names the cheapest tier |
| 404 | `dataset-unknown` | a dataset id the reference catalogue does not list |
| 404 | `date-not-trading` | cme or futures with no session in the dates asked |
| 404 | `export-unknown` | /v3/jobs: exports are no longer kept after they are sent |
| 404 | `file-unknown` | a kind and feed that name no market |
| 404 | `level-not-served` | orders or book on a market whose levels do not include L3 or L2 |
| 404 | `market-not-enabled` | a market switched off or coming soon; the message carries its label |
| 404 | `market-not-listed` | a market that is not offered |
| 404 | `market-unknown` | a market code the server does not know |
| 404 | `pair-unknown` | a pair_id the market does not list |
| 404 | `photo-missing` | the file is gone |
| 404 | `photo-unknown` | no picture for this key |
| 404 | `series-unknown` | a code not in the observation catalogue |
| 404 | `symbol-unknown` | a symbol the market does not list |
| 404 | `theme-unknown` | theme not light or dark |
| 404 | `underlying-unknown` | an underlying the market does not list |
| 404, 503 | `dataset-unavailable` | 404: a reference dataset not available yet; 503: the dataset builder did not answer |
| 502 | `export-failed` | the build failed |
| 410 | `export-required` | /v3/files and /v3/file, once the key passes the plan checks |
| 413 | `export-too-wide` | an export past the rows one export may read (2,500,000 on every plan), or one the server could not measure within 10 seconds |
| 413 | `series-window-too-wide` | more than 100,000 rows |
| 429 | `allowance-exhausted` | a free key past its weekly bytes (12.5 GiB); it resets Monday 00:00 UTC |
| 429 | `calls-exhausted` | past the plan's calls a minute; Retry-After says when |
| 429 | `connections-exhausted` | one more download while this key's one download is still open; Retry-After 5 |
| 429 | `rows-exhausted` | past the plan's rows a day (12,500,000 on the free plan); Retry-After and retry_after give the seconds to 00:00 UTC |
| 429, 503 | `exports-busy` | 429: this key's one download is still building (Retry-After 5); 503: the server's queue of waiting builds is full, or the export could not be measured just now (Retry-After 30) |
| 502 | `dataset-failed` | the dataset build failed |
| 502 | `query-invalid` | the data store refused the query as malformed |
| 502, 503 | `catalogue-invalid` | the catalogue answered badly (502), or could not be loaded (503) |
| 503 | `calendar-conflict` | two calendar rows for one session disagree |
| 503 | `calendar-invalid` | the exchange calendar answered badly |
| 503 | `calendar-unavailable` | the exchange calendar could not be read |
| 503 | `catalogue-busy` | too many catalogue reads at once |
| 503 | `catalogue-unavailable` | the catalogue did not answer |
| 503 | `estimate-unavailable` | the figures for a selection the key may not download could not be read |
| 503 | `overview-unavailable` | the overview figures could not be read |
| 503 | `query-busy` | too many reads at once |
| 504 | `macro-timeout` | past 25 seconds |
| 504 | `options-timeout` | past its 20-second budget |
| 504 | `reference-timeout` | past 25 seconds |

## The Python client

`pip install lse-data` and `client = LSE(api_key)`. Which method makes which call:

| Method | Call |
|---|---|
| `health()`, `usage()` | `/v3/health`, `/v3/usage` |
| `markets()`, `tables(market)`, `catalogue()` | `/v3/markets`, `/v3/tables`, `/v3/catalogue` |
| `pair_page()`, `iter_pairs()`, `pairs()`, `catalog()`, `datasets()`, `options_underlyings()` | `/v3/pairs`, following `offset` or `next_cursor` |
| `factors(market, symbol)` | `/v3/factors` |
| `overview(market, symbol)` | `/v3/overview` |
| `preview()`, `estimate()` | `/v3/preview`, `/v3/estimate` |
| `export()`, `history()` | `/v3/export` |
| `job()` | `/v3/jobs/{id}`, which no longer keeps anything |
| `rows()`, `candles()`, `option_candles()` | `/v3/export` as CSV, held in memory |
| `dataset_export()` | `/v3/dataset/export` |
| `options_chain()`, `options()` | `/v3/options/chain` |
| `options_flow_page()`, `options_flow()` | `/v3/options/flow` |
| `macro_catalogue()`, `iter_macro_series()`, `economics()` | `/v3/macro` |
| `macro_preview()`, `macro_series()`, `series()`, `macro_estimate()`, `macro_export()`, `macro_chart_export()` | the macro calls |
| `reference_catalogue()`, `iter_reference_series()`, `reference()` | `/v3/reference` |
| `reference_preview()`, `reference_estimate()`, `reference_export()`, `dataset()` | the reference calls |
| `reference_rows()`, `bond_yields()`, `economic_calendar()`, `insider_trades()`, `dividends()`, `splits()`, `cot()`, `financial_reports()`, `company_profiles()`, `fundamentals()` | `/v3/reference/export` as CSV, held in memory |

`catalog()` covers every market, Nasdaq TotalView included, and lists the two contract markets by underlying.

The client keeps the key in the header, follows no redirect, writes a download to a `.part` file beside the
destination, asks again from the start when a transfer is cut, and checks the SHA-256 before it places the
destination. A refusal raises `LSEError` with `status`, `code`, `message` and `required_plan`; an export that outlasts
its `timeout` raises it with code `timeout`. On a `429` that carries a `Retry-After` of two minutes or less
(`calls-exhausted`, `connections-exhausted`, `exports-busy`) the client waits that long once in a call and asks again,
unless it was made with `wait_for_limit=False`; a refusal that stands is raised with the server's code and sentence.

The same client streams live prices with the same key, `client.stream([...])`: the
[WebSocket documentation](https://londonstrategicedge.com/websocket-documentation) describes the sockets. Crypto
streams from `wss://london-ws.londonstrategicedge.com`, which a client on the default address opens beside the default
socket for the crypto symbols it is asked for.
