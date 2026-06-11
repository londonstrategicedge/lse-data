"""
LSE market data client: live streaming over WebSocket and historical
download over REST, both authorized by the same API key.

Streaming connects to wss://data-ws.londonstrategicedge.com; download calls
https://api.londonstrategicedge.com. One key, one monthly data allowance shared
across both.
"""

import asyncio
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Dict, Iterator, List, Optional, Set

import websockets
import websockets.client


WS_URL = "wss://data-ws.londonstrategicedge.com"
# REST download base. The same key authorizes streaming and download; the server
# caps each call at 5,000 rows and 100 calls/min and meters response bytes
# against the same monthly allowance as streaming.
API_URL = "https://api.londonstrategicedge.com/iso"
# The download host is behind a CDN that blocks the default Python-urllib
# User-Agent. Send our own so requests are not bounced before reaching the API.
_USER_AGENT = "lse-data-sdk (+https://londonstrategicedge.com)"
# Public instrument catalog (no key required). Lists every downloadable /
# streamable instrument with its display name and category.
_CATALOG_URL = "https://londonstrategicedge.com/feed-catalog.json"
# Public list of every underlying with listed options (no key required).
_OPTION_UNDERLYINGS_URL = "https://londonstrategicedge.com/option_underlyings.json"


class LSEError(Exception):
    """Raised when a REST download call returns a non-2xx response (bad filter,
    rate limit, quota exceeded, forbidden table, etc.)."""

    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(f"[{status}] {message}")

# Ping interval to keep the connection alive. The server expects a ping
# within its idle timeout window (currently 600s). We send every 25s to
# stay well under the server's 30s protocol-level ping interval.
PING_INTERVAL = 25


@dataclass
class Tick:
    """A single price tick from the LSE feed."""
    symbol: str
    price: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    # ISO-8601 string as sent by the server (e.g. "2026-06-04T16:32:00Z").
    # Use the `datetime` property for a parsed, timezone-aware value.
    timestamp: Optional[str] = None
    name: Optional[str] = None
    replay: bool = False  # True for historical ticks during replay

    @property
    def datetime(self):
        """The tick time as a timezone-aware ``datetime``, or None."""
        if not self.timestamp:
            return None
        from datetime import datetime as _datetime
        try:
            return _datetime.fromisoformat(str(self.timestamp).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

    def __repr__(self) -> str:
        r = " REPLAY" if self.replay else ""
        return f"Tick({self.symbol} {self.price}{r})"


# OSI contract symbol, as the feed publishes it (no "O:" prefix):
# root (1-10 chars) + YYMMDD + C/P + strike*1000 zero-padded to 8 digits.
_OSI_RE = re.compile(r"^([A-Z][A-Z0-9.]{0,9})(\d{6})([CP])(\d{8})$")


@dataclass(repr=False)
class OptionTick(Tick):
    """A tick for a single option contract.

    Subclass of :class:`Tick`, so existing code keeps working, with the OSI
    symbol parsed into named fields. ``premium`` is an alias of ``price``;
    ``notional`` is ``price * volume * 100`` (the US equity option
    multiplier), i.e. the dollar value that traded in this tick.
    """
    underlying: str = ""
    right: str = ""            # "call" or "put"
    strike: float = 0.0
    expiry: Optional[date] = None

    @property
    def premium(self) -> float:
        return self.price

    @property
    def dte(self) -> Optional[int]:
        """Calendar days until expiry (0 on expiry day), or None."""
        if self.expiry is None:
            return None
        return (self.expiry - date.today()).days

    @property
    def notional(self) -> Optional[float]:
        """Dollar value traded in this tick, or None when volume is absent."""
        if self.volume is None:
            return None
        return round(self.price * self.volume * 100, 2)

    @classmethod
    def from_symbol(cls, **kwargs) -> "Tick":
        """Build an OptionTick when ``symbol`` is an OSI contract, else a Tick."""
        m = _OSI_RE.match(kwargs.get("symbol", ""))
        if not m:
            return Tick(**kwargs)
        root, yymmdd, cp, strike_raw = m.groups()
        try:
            expiry = date(2000 + int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6]))
        except ValueError:
            return Tick(**kwargs)
        return cls(underlying=root, right="call" if cp == "C" else "put",
                   strike=int(strike_raw) / 1000.0, expiry=expiry, **kwargs)

    def __repr__(self) -> str:
        r = " REPLAY" if self.replay else ""
        return (f"OptionTick(underlying='{self.underlying}', right='{self.right}', "
                f"strike={self.strike:g}, expiry='{self.expiry}', dte={self.dte}, "
                f"premium={self.price}, volume={self.volume}, notional={self.notional}{r})")


def tape(stream=None):
    """Return a tick callback that prints an aligned, human-readable table.

    Option ticks render as columns (time, underlying, type, strike, expiry,
    DTE, premium, volume, notional); other ticks as a plain price line. The
    header prints once, before the first option row.

    Example:
        client.subscribe_options(["AAPL"])
        client.on("tick", tape())
        client.connect()
    """
    import sys as _sys
    out = stream or _sys.stdout
    state = {"header": False}

    def _t(tick):
        # Show when the trade printed (the tick's own timestamp), not when this
        # row was drawn. They differ for replay/historical ticks; for a live
        # feed they are within a second. Fall back to now if no timestamp.
        dt = tick.datetime
        return (dt or datetime.now()).strftime("%H:%M:%S")

    def _print(tick):
        if isinstance(tick, OptionTick):
            if not state["header"]:
                state["header"] = True
                hdr = (f"{'TIME':<9} {'UND':<6} {'TYPE':<4} {'STRIKE':>9}  {'EXPIRY':<10}  "
                       f"{'DTE':>4}  {'PREM':>8}  {'VOL':>6}  {'NOTIONAL':>12}")
                out.write(hdr + "\n" + "-" * len(hdr) + "\n")
            vol = int(tick.volume or 0)
            notional = f"${tick.notional or 0.0:,.0f}"
            out.write(f"{_t(tick):<9} {tick.underlying:<6} "
                      f"{'CALL' if tick.right == 'call' else 'PUT':<4} {tick.strike:>9.2f}  "
                      f"{tick.expiry}  {tick.dte:>3}d  {tick.price:>8.2f}  {vol:>6}  "
                      f"{notional:>12}\n")
        else:
            out.write(f"{_t(tick):<9} {tick.symbol:<13} {tick.price:g}\n")
        out.flush()

    return _print


class LSE:
    """Client for London Strategic Edge real-time market data.

    Args:
        api_key: Your LSE API key. Get one at https://londonstrategicedge.com/data
                 If omitted, the LSE_API_KEY environment variable is used.
        url: WebSocket endpoint. Defaults to the production server.

    Works as a context manager (``with LSE(...) as client:``), disconnecting
    on exit.

    Examples:
        Synchronous streaming:

            from lse import LSE

            client = LSE(api_key="your_key")
            for tick in client.stream(["BTC/USD", "AAPL"]):
                print(tick.symbol, tick.price)

        With a callback:

            def on_tick(tick):
                print(f"{tick.symbol}: {tick.price}")

            client = LSE(api_key="your_key")
            client.on("tick", on_tick)
            client.subscribe(["BTC/USD", "ETH/USD"])
            client.connect()  # blocks forever

        Async streaming:

            import asyncio
            from lse import LSE

            async def main():
                client = LSE(api_key="your_key")
                async for tick in client.stream_async(["BTC/USD"]):
                    print(tick)

            asyncio.run(main())

        Options chain streaming:

            client = LSE(api_key="your_key")
            client.on("tick", lambda t: print(t))
            client.subscribe_options(["AAPL", "TSLA"])
            client.connect()
    """

    def __init__(self, api_key: Optional[str] = None, url: str = WS_URL):
        api_key = api_key or os.environ.get("LSE_API_KEY")
        if not api_key:
            raise ValueError(
                "No API key. Pass api_key=... or set the LSE_API_KEY environment "
                "variable. Get a key at https://londonstrategicedge.com/data"
            )
        self._api_key = api_key
        self._url = url
        self._ws: Optional[websockets.client.WebSocketClientProtocol] = None
        self._callbacks: Dict[str, List[Callable]] = {}
        self._symbols: List[dict] = []
        self._tier: str = ""
        self._authenticated = False
        self._subscriptions: Set[str] = set()
        # Tracks option underlying subscriptions separately from symbol subs.
        # Options use a different server-side mechanism: subscribing to "AAPL"
        # options gives you ALL AAPL contracts (800+) as a single subscription.
        self._option_underlyings: Set[str] = set()
        # Set when disconnect() is called so reconnect loops know to stop
        self._disconnect_requested = False
        # Reference to the event loop running _run_forever, used by disconnect()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # Timeout (seconds) for REST download calls.
        self._rest_timeout = 30
        # Cached instrument catalog (fetched once, on first catalog() call).
        self._catalog_cache: Optional[List[dict]] = None

    def __enter__(self) -> "LSE":
        return self

    def __exit__(self, *exc) -> bool:
        # Ensure the WebSocket is torn down if a streaming block raises.
        self.disconnect()
        return False

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def symbols(self) -> List[dict]:
        """List of available symbols returned after authentication."""
        return self._symbols

    @property
    def tier(self) -> str:
        """Account tier (e.g. 'basic', 'pro')."""
        return self._tier

    @property
    def authenticated(self) -> bool:
        """Whether the client has successfully authenticated."""
        return self._authenticated

    @property
    def subscriptions(self) -> Set[str]:
        """Set of currently subscribed symbols."""
        return self._subscriptions.copy()

    # ------------------------------------------------------------------
    # Event callbacks
    # ------------------------------------------------------------------

    def on(self, event: str, callback: Callable) -> "LSE":
        """Register a callback for an event.

        Supported events:
            - "tick": called with a Tick object on each price update
            - "connected": called when WebSocket connects
            - "authenticated": called when auth succeeds
            - "disconnected": called when connection drops
            - "error": called with error message string

        Args:
            event: Event name.
            callback: Function to call.

        Returns:
            self, for chaining.
        """
        self._callbacks.setdefault(event, []).append(callback)
        return self

    def _emit(self, event: str, *args):
        for cb in self._callbacks.get(event, []):
            try:
                cb(*args)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Synchronous API (blocking)
    # ------------------------------------------------------------------

    def stream(self, symbols: List[str], reconnect: bool = True, start: Optional[str] = None) -> Iterator[Tick]:
        """Stream ticks. Blocks forever, yields Tick objects.

        If start is provided, the server replays historical ticks from that
        time first (with tick.replay=True), then transitions to live data on
        the same connection.

        Args:
            symbols: List of symbols to subscribe to (e.g. ["BTC/USD", "AAPL"]).
            reconnect: If True, automatically reconnect on disconnect.
            start: Optional start time for historical replay. Accepts ISO 8601
                   (e.g. "2026-04-18T09:00:00") or epoch timestamp. The server
                   replays ticks from this point, then switches to live. Max 24h.

        Yields:
            Tick objects as they arrive (replay ticks first, then live).

        Example:
            from lse import LSE

            client = LSE(api_key="your_key")

            # Live only
            for tick in client.stream(["BTC/USD"]):
                print(f"{tick.symbol}: ${tick.price}")

            # Replay last 2 hours, then live
            for tick in client.stream(["BTC/USD"], start="2026-04-18T07:00:00"):
                print(f"{'REPLAY' if tick.replay else 'LIVE'} {tick.symbol}: ${tick.price}")
        """
        self._disconnect_requested = False
        while not self._disconnect_requested:
            try:
                # Run the async generator in a new event loop.
                # We suppress "task was destroyed" warnings that occur when
                # the caller breaks out of the iterator mid-stream, which is
                # normal usage (e.g. "for tick in stream: if done: break").
                import warnings
                loop = asyncio.new_event_loop()
                self._loop = loop
                try:
                    gen = self.stream_async(symbols, reconnect=False, start=start).__aiter__()
                    while True:
                        tick = loop.run_until_complete(gen.__anext__())
                        yield tick
                except StopAsyncIteration:
                    # The single connection ended (drop, disconnect(), or a
                    # fatal error). Fall through to the stop/reconnect decision
                    # below instead of looping unconditionally.
                    pass
                except GeneratorExit:
                    return
                finally:
                    # Shut down pending tasks cleanly to avoid warnings
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.close()
                    self._loop = None
            except Exception as e:
                self._emit("error", str(e))
            # Stop on an explicit disconnect(), a fatal error (which sets
            # _disconnect_requested), or when auto-reconnect is off. This check
            # runs for BOTH a clean end and an exception, so a disconnect during
            # a stream()/replay loop exits instead of silently reconnecting.
            if self._disconnect_requested or not reconnect:
                return
            time.sleep(3)

    def connect(self, symbols: Optional[List[str]] = None):
        """Connect and block forever, dispatching events via callbacks.

        Use this with .on("tick", callback) for event-driven usage.
        For iterator-style usage, use .stream() instead.
        Call disconnect() from a callback to stop cleanly.

        Args:
            symbols: Optional list of symbols to subscribe to on connect.
                     You can also call .subscribe() separately.
        """
        self._disconnect_requested = False
        asyncio.run(self._run_forever(symbols or []))

    def subscribe(self, symbols: List[str]):
        """Subscribe to additional symbols (only works during .connect()).

        For most use cases, pass symbols directly to .stream() or .connect().
        """
        for sym in symbols:
            self._subscriptions.add(sym)
            if self._ws and self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps({"action": "subscribe", "symbol": sym})),
                    self._loop,
                )

    def unsubscribe(self, symbols: List[str]):
        """Unsubscribe from symbols. Stops receiving ticks for these symbols.

        The server confirms each unsubscription with a {"type": "unsubscribed"}
        message. The client's local subscription set is updated immediately.

        Args:
            symbols: List of symbols to unsubscribe from.

        Example:
            client.unsubscribe(["BTC/USD"])  # stop receiving BTC ticks
        """
        for sym in symbols:
            self._subscriptions.discard(sym)
            if self._ws and self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps({"action": "unsubscribe", "symbol": sym})),
                    self._loop,
                )

    def subscribe_options(self, underlyings: List[str]):
        """Subscribe to options chains for the given underlying symbols.

        Each underlying (e.g. "AAPL") subscribes you to ALL of that stock's
        option contracts (calls + puts, all strikes/expiries) as a single
        subscription. This is much more efficient than subscribing to each
        contract individually (800+ contracts per underlying).

        Ticks arrive as normal Tick objects where symbol is the contract
        name (e.g. "AAPL250620C00200000").

        Args:
            underlyings: List of underlying stock symbols (e.g. ["AAPL", "TSLA"]).

        Example:
            client.subscribe_options(["AAPL"])  # all AAPL calls + puts
        """
        for sym in underlyings:
            self._option_underlyings.add(sym.upper())
            if self._ws and self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps({"action": "subscribe_options", "underlying": sym})),
                    self._loop,
                )

    def unsubscribe_options(self, underlyings: List[str]):
        """Unsubscribe from options chains for the given underlyings.

        Args:
            underlyings: List of underlying stock symbols to unsubscribe from.

        Example:
            client.unsubscribe_options(["AAPL"])
        """
        for sym in underlyings:
            self._option_underlyings.discard(sym.upper())
            if self._ws and self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._ws.send(json.dumps({"action": "unsubscribe_options", "underlying": sym})),
                    self._loop,
                )

    def disconnect(self):
        """Gracefully close the WebSocket connection and stop reconnecting.

        After calling disconnect(), the stream()/connect() loop will exit
        instead of retrying. Safe to call from any thread (e.g. from a
        callback registered with .on()).

        Example:
            def on_tick(tick):
                if tick.price > 100000:
                    client.disconnect()  # done, exit cleanly

            client.on("tick", on_tick)
            client.connect(["BTC/USD"])  # returns after disconnect()
        """
        self._disconnect_requested = True
        # Close the live WebSocket so the receive loop exits immediately
        if self._ws:
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
            else:
                # Fallback: force-close the underlying transport
                try:
                    self._ws.transport.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # REST data download (historical) — same key as streaming
    # ------------------------------------------------------------------

    @staticmethod
    def _symbol_slug(symbol: str) -> str:
        return symbol.lower().replace("/", "_").replace("-", "_").replace(".", "_")

    def _rest_get(self, table: str, params: List[tuple]) -> List[dict]:
        """GET /iso/<table> with the API key. `params` is a list of (key, value)
        tuples so a column can repeat (the API ANDs repeated filters, e.g. a
        gte and an lt on timestamp). Returns a list of row dicts; raises
        LSEError on any non-2xx response or API error body."""
        # Keep filter operator punctuation (. , ( ) : *) unescaped.
        qs = urllib.parse.urlencode(params, safe=".,():*")
        url = f"{API_URL}/{table}" + (f"?{qs}" if qs else "")
        req = urllib.request.Request(url, headers={
            "x-api-key": self._api_key,
            "User-Agent": _USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=self._rest_timeout) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")
            msg = raw
            try:
                j = json.loads(raw)
                msg = j.get("message") or j.get("error") or raw
            except Exception:
                pass
            raise LSEError(e.code, msg)
        data = json.loads(body)
        if isinstance(data, dict) and data.get("code") and "message" in data:
            raise LSEError(400, data["message"])
        return data

    def candles(self, symbol: str, timeframe: str = "1m", start: Optional[str] = None,
                end: Optional[str] = None, limit: int = 5000, order: str = "asc") -> List[dict]:
        """Historical OHLCV candles for any non-option instrument (stocks, FX,
        crypto, commodities, indices, ETFs).

        timeframe: "1m", "5m", "15m", "1h", "4h", or "1d".
        start / end: ISO timestamps (e.g. "2026-01-01") filtering the bar time.
        The server caps each call at 5,000 rows; page with start/end for more.

        Example:
            client.candles("BTC/USD", "1d", start="2026-01-01")
            client.candles("AAPL", "1h", limit=200, order="desc")
        """
        tf = timeframe.lower()
        p: List[tuple] = [("order", f"timestamp.{order}"), ("limit", str(min(int(limit), 5000)))]
        if start:
            p.append(("timestamp", f"gte.{start}"))
        if end:
            p.append(("timestamp", f"lt.{end}"))
        htf = {"5m": "x_candles_5m", "15m": "x_candles_15m", "1h": "x_candles_1h",
               "4h": "x_candles_4h", "1d": "x_candles_1d"}
        if tf in htf:
            return self._rest_get(htf[tf], [("symbol", f"eq.{symbol}")] + p)
        if tf == "1m":
            # Per-symbol 1m table: most instruments use candles_<sym>; US stocks
            # and ETFs use d_candles_<sym>. Try the common one, fall back on 404.
            slug = self._symbol_slug(symbol)
            try:
                return self._rest_get(f"candles_{slug}", p)
            except LSEError as e:
                if e.status == 404:
                    return self._rest_get(f"d_candles_{slug}", p)
                raise
        raise LSEError(400, f"unsupported timeframe '{timeframe}' (use 1m/5m/15m/1h/4h/1d)")

    def economic_calendar(self, region=None, event: Optional[str] = None,
                          start: Optional[str] = None, end: Optional[str] = None,
                          released_only: bool = False, order: str = "asc",
                          limit: int = 5000) -> List[dict]:
        """Macro economic events (CPI, NFP, rate decisions, GDP, ...).
        region: a code like "US" or a list like ["US","EU","GB"].
        released_only: only events whose `actual` has printed."""
        p: List[tuple] = [("is_stale", "is.false"), ("order", f"datetime.{order}"),
                          ("limit", str(min(int(limit), 5000)))]
        if region is not None:
            if isinstance(region, (list, tuple, set)):
                p.append(("region_code", f"in.({','.join(region)})"))
            else:
                p.append(("region_code", f"eq.{region}"))
        if event:
            p.append(("event", f"ilike.*{event}*"))
        if start:
            p.append(("datetime", f"gte.{start}"))
        if end:
            p.append(("datetime", f"lt.{end}"))
        if released_only:
            p.append(("actual", "not.is.null"))
        return self._rest_get("economic_calender", p)

    def insider_trades(self, symbol: Optional[str] = None, type: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       order: str = "desc", limit: int = 5000) -> List[dict]:
        """SEC Form 3/4/5 insider transactions. `type` is an SEC code, e.g.
        "P-Purchase" or "S-Sale"; start/end filter `transaction_date`."""
        p: List[tuple] = [("order", f"transaction_date.{order}"), ("limit", str(min(int(limit), 5000)))]
        if symbol:
            p.append(("symbol", f"eq.{symbol}"))
        if type:
            p.append(("transaction_type", f"eq.{type}"))
        if start:
            p.append(("transaction_date", f"gte.{start}"))
        if end:
            p.append(("transaction_date", f"lt.{end}"))
        return self._rest_get("z_insider_trades", p)

    def dividends(self, symbol: Optional[str] = None, start: Optional[str] = None,
                  end: Optional[str] = None, order: str = "desc", limit: int = 5000) -> List[dict]:
        """Dividend events; start/end filter the ex-date (`effective_date`)."""
        p: List[tuple] = [("order", f"effective_date.{order}"), ("limit", str(min(int(limit), 5000)))]
        if symbol:
            p.append(("symbol", f"eq.{symbol}"))
        if start:
            p.append(("effective_date", f"gte.{start}"))
        if end:
            p.append(("effective_date", f"lt.{end}"))
        return self._rest_get("dividends", p)

    def splits(self, symbol: Optional[str] = None, start: Optional[str] = None,
               end: Optional[str] = None, order: str = "desc", limit: int = 5000) -> List[dict]:
        """Stock split events; start/end filter `effective_date`."""
        p: List[tuple] = [("order", f"effective_date.{order}"), ("limit", str(min(int(limit), 5000)))]
        if symbol:
            p.append(("symbol", f"eq.{symbol}"))
        if start:
            p.append(("effective_date", f"gte.{start}"))
        if end:
            p.append(("effective_date", f"lt.{end}"))
        return self._rest_get("stock_splits", p)

    # ------------------------------------------------------------------
    # Options data (REST) — chain, flow, per-contract history
    # ------------------------------------------------------------------

    _OPTION_TYPE_ALIASES = {"c": "call", "call": "call", "calls": "call",
                            "p": "put", "put": "put", "puts": "put"}

    # numeric columns arrive with full binary float expansion (a price stored
    # from a float serializes as 2.0299999999999998); round to the precision
    # the feed actually quotes.
    _OPTION_ROUND = {"last_price": 4, "premium": 2, "premium_today": 2,
                     "underlying_price": 4, "iv": 4, "iv_avg": 4,
                     "delta": 4, "delta_avg": 4, "gamma": 6, "gamma_avg": 6,
                     "theta": 4, "theta_avg": 4, "vega": 4, "vega_avg": 4,
                     "rho": 4, "rho_avg": 4, "open": 4, "high": 4, "low": 4,
                     "close": 4}

    @classmethod
    def _clean_option_rows(cls, rows: List[dict]) -> List[dict]:
        for r in rows:
            for k, nd in cls._OPTION_ROUND.items():
                v = r.get(k)
                if isinstance(v, float):
                    r[k] = round(v, nd)
        return rows

    def _resolve_underlying(self, query: str) -> str:
        """Accept a ticker in any case ("AAPL", "aapl") or a company name
        ("apple", "Nvidia") and return the ticker. A string that matches a
        catalog symbol always wins; otherwise the closest catalog name match
        is used (prefix matches first, then the shortest name, so "apple"
        resolves to Apple Inc. rather than Apple Hospitality REIT)."""
        q = (query or "").strip()
        if not q:
            raise LSEError(400, "underlying is required")
        try:
            items = self.catalog()
        except Exception:
            # Catalog briefly unreachable: assume the caller passed a ticker.
            return q.upper()
        if q.upper() in {x.get("symbol", "").upper() for x in items}:
            return q.upper()
        ql = q.lower()
        hits = [x for x in items if ql in (x.get("name") or "").lower()]
        if not hits:
            return q.upper()
        hits.sort(key=lambda x: (not x["name"].lower().startswith(ql), len(x["name"])))
        return hits[0]["symbol"]

    def _resolve_contract(self, contract: str, strike=None, expiry=None,
                          type: Optional[str] = None) -> str:
        """Return an OSI contract ticker. Either `contract` already is one,
        or it is an underlying and strike + expiry + type spell out the rest."""
        if strike is None and expiry is None and type is None:
            osi = contract.strip().upper()
            if not _OSI_RE.match(osi):
                raise LSEError(400,
                    f"'{contract}' is not an option contract; pass an OSI ticker "
                    "like AAPL260612C00205000, or an underlying plus "
                    "strike=, expiry=, type=")
            return osi
        if strike is None or expiry is None or type is None:
            raise LSEError(400, "strike, expiry and type are all required "
                                "when addressing a contract by its parts")
        right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
        if not right:
            raise LSEError(400, f"type must be 'call' or 'put', got '{type}'")
        exp = date.fromisoformat(str(expiry)) if not isinstance(expiry, date) else expiry
        root = self._resolve_underlying(contract)
        return (f"{root}{exp.strftime('%y%m%d')}"
                f"{'C' if right == 'call' else 'P'}{int(round(float(strike) * 1000)):08d}")

    def options(self, underlying: str, type: Optional[str] = None,
                expiry: Optional[str] = None, strike=None,
                min_dte: Optional[int] = None, max_dte: Optional[int] = None,
                limit: int = 5000) -> List[dict]:
        """The current option chain for an underlying: one row per contract
        with the latest traded price, implied volatility, greeks, and today's
        volume and premium totals. Refreshed continuously while the market is
        open. Each row carries its OSI ticker, ready for option_candles() or
        subscribe_options() drill down.

        underlying: ticker or company name ("AAPL", "apple", "Nvidia").
        type:       "call" or "put" (default both).
        expiry:     one expiry date, e.g. "2026-06-19".
        strike:     one strike (205) or an inclusive (low, high) window.
        min_dte / max_dte: days-to-expiry window.

        Example:
            chain = client.options("apple", type="call", max_dte=30)
            near = client.options("NVDA", expiry="2026-06-19", strike=(180, 220))
        """
        sym = self._resolve_underlying(underlying)
        p: List[tuple] = [("underlying", f"eq.{sym}"),
                          ("order", "expiry.asc,strike.asc"),
                          ("limit", str(min(int(limit), 5000)))]
        if type:
            right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
            if not right:
                raise LSEError(400, f"type must be 'call' or 'put', got '{type}'")
            p.append(("contract_type", f"eq.{right}"))
        if expiry:
            p.append(("expiry", f"eq.{expiry}"))
        if strike is not None:
            if isinstance(strike, (tuple, list)):
                p.append(("strike", f"gte.{strike[0]}"))
                p.append(("strike", f"lte.{strike[1]}"))
            else:
                p.append(("strike", f"eq.{strike}"))
        if min_dte is not None:
            p.append(("dte", f"gte.{int(min_dte)}"))
        if max_dte is not None:
            p.append(("dte", f"lte.{int(max_dte)}"))
        return self._clean_option_rows(self._rest_get("x_options_chain", p))

    def options_flow(self, underlying: Optional[str] = None,
                     type: Optional[str] = None,
                     min_premium: Optional[float] = None,
                     expiry: Optional[str] = None,
                     max_dte: Optional[int] = None,
                     start: Optional[str] = None, end: Optional[str] = None,
                     order: str = "desc", limit: int = 5000) -> List[dict]:
        """Recent option prints (time and sales): every trade with its
        premium, IV and greeks at print time. Covers the trailing week;
        older history is served as 1 minute bars by option_candles().

        Omit underlying to sweep the whole tape, e.g. every print above
        $250k premium across all names.

        Example:
            client.options_flow("apple", min_premium=100_000)
            client.options_flow(type="put", min_premium=250_000, max_dte=7)
        """
        p: List[tuple] = [("order", f"ts.{order}"),
                          ("limit", str(min(int(limit), 5000)))]
        if underlying:
            p.append(("underlying", f"eq.{self._resolve_underlying(underlying)}"))
        if type:
            right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
            if not right:
                raise LSEError(400, f"type must be 'call' or 'put', got '{type}'")
            p.append(("contract_type", f"eq.{right}"))
        if min_premium is not None:
            p.append(("premium", f"gte.{min_premium}"))
        if expiry:
            p.append(("expiry", f"eq.{expiry}"))
        if max_dte is not None:
            p.append(("dte", f"lte.{int(max_dte)}"))
        if start:
            p.append(("ts", f"gte.{start}"))
        if end:
            p.append(("ts", f"lt.{end}"))
        return self._clean_option_rows(self._rest_get("x_options_flow", p))

    @staticmethod
    def _bars_from_prints(prints: List[dict]) -> List[dict]:
        """Fold raw prints into 1 minute bars with the same shape and
        semantics as the server's nightly compaction (epoch-floored minutes,
        OHLC on last_price ordered by ts, summed volume/premium, averaged
        greeks), so archive bars and freshly built bars are indistinguishable."""
        buckets: Dict[int, List[dict]] = {}
        for r in prints:
            try:
                ts = datetime.fromisoformat(str(r["ts"]).replace("Z", "+00:00"))
            except (ValueError, TypeError, KeyError):
                continue
            buckets.setdefault(int(ts.timestamp()) // 60 * 60, []).append(r)
        bars = []
        for epoch in sorted(buckets):
            rows = buckets[epoch]
            rows.sort(key=lambda r: r["ts"])
            first = rows[0]

            def avg(col):
                vals = [r[col] for r in rows if r.get(col) is not None]
                return sum(vals) / len(vals) if vals else None

            bars.append({
                "ticker": first["ticker"], "underlying": first["underlying"],
                "strike": first["strike"], "expiry": first["expiry"],
                "contract_type": first["contract_type"],
                "minute": datetime.fromtimestamp(epoch, timezone.utc).isoformat(),
                "dte": first.get("dte"),
                "open": rows[0]["last_price"], "high": max(r["last_price"] for r in rows),
                "low": min(r["last_price"] for r in rows), "close": rows[-1]["last_price"],
                "volume": sum(r["volume"] for r in rows),
                "premium": sum(r["premium"] for r in rows),
                "print_count": len(rows),
                "iv_avg": avg("iv"), "delta_avg": avg("delta"),
                "gamma_avg": avg("gamma"), "theta_avg": avg("theta"),
                "vega_avg": avg("vega"), "rho_avg": avg("rho"),
                "underlying_price": avg("underlying_price"),
            })
        return bars

    def option_candles(self, contract: str, strike=None, expiry=None,
                       type: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       order: str = "asc", limit: int = 5000) -> List[dict]:
        """1 minute premium OHLC history for one contract, with volume,
        premium and averaged greeks per bar.

        Address the contract either way:
            client.option_candles("AAPL260612C00205000")
            client.option_candles("AAPL", strike=205, expiry="2026-06-12", type="call")

        Bars older than about a week come from the compacted archive; the
        trailing week is folded from raw prints on the fly, so recent bars
        always agree with options_flow(). For very active contracts narrow
        the window with start/end (the print fetch behind recent bars is
        capped at 5,000 rows per call).
        """
        osi = self._resolve_contract(contract, strike=strike, expiry=expiry, type=type)
        lim = min(int(limit), 5000)

        p: List[tuple] = [("ticker", f"eq.{osi}"), ("order", f"minute.{order}"),
                          ("limit", str(lim))]
        if start:
            p.append(("minute", f"gte.{start}"))
        if end:
            p.append(("minute", f"lt.{end}"))
        archive = self._rest_get("x_options_flow_1m", p)

        q: List[tuple] = [("ticker", f"eq.{osi}"), ("order", "ts.asc"),
                          ("limit", "5000")]
        if start:
            q.append(("ts", f"gte.{start}"))
        if end:
            q.append(("ts", f"lt.{end}"))
        recent = self._bars_from_prints(self._rest_get("x_options_flow", q))

        # The archive only holds bars older than the compaction window and
        # prints only cover the trailing week, so the two never overlap.
        merged = archive + recent
        merged.sort(key=lambda b: datetime.fromisoformat(str(b["minute"]).replace("Z", "+00:00")),
                    reverse=(order == "desc"))
        return self._clean_option_rows(merged[:lim])

    def options_underlyings(self) -> List[dict]:
        """Every underlying with listed options, as [{"symbol", "name"}, ...].
        No key required. Feed any entry straight into options(),
        options_flow() or subscribe_options()."""
        req = urllib.request.Request(_OPTION_UNDERLYINGS_URL,
                                     headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req, timeout=self._rest_timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def get(self, table: str, **filters) -> List[dict]:
        """Generic download: any downloadable table with raw query filters.
        Company profiles are not downloadable and return an error.

        Example:
            client.get("z_insider_trades", symbol="eq.AAPL", limit="10")
        """
        return self._rest_get(table, [(k, str(v)) for k, v in filters.items()])

    def catalog(self, category: Optional[str] = None) -> List[dict]:
        """List every available instrument, each a dict of
        {"symbol", "name", "category"}. No connection or key required; use it
        to discover exact symbols before streaming or downloading.

        category (optional): one of stock, forex, crypto, etf, commodity, index
        (singular or plural). Omit for the full list.

        Example:
            client.catalog()                   # all ~4,100 instruments
            crypto = client.catalog("crypto")  # [{"symbol": "BTC/USD", ...}, ...]
            symbols = [x["symbol"] for x in client.catalog("forex")]
        """
        if self._catalog_cache is None:
            req = urllib.request.Request(_CATALOG_URL, headers={"User-Agent": _USER_AGENT})
            with urllib.request.urlopen(req, timeout=self._rest_timeout) as resp:
                self._catalog_cache = json.loads(resp.read().decode("utf-8"))
        items = self._catalog_cache
        if category:
            norm = {
                "stock": "Stocks", "stocks": "Stocks", "equity": "Stocks", "equities": "Stocks",
                "forex": "Forex", "fx": "Forex", "crypto": "Crypto",
                "etf": "ETFs", "etfs": "ETFs",
                "commodity": "Commodities", "commodities": "Commodities",
                "index": "Indices", "indices": "Indices",
            }
            want = norm.get(category.lower(), category)
            items = [x for x in items if x.get("category") == want]
        return list(items)

    # ------------------------------------------------------------------
    # Async API
    # ------------------------------------------------------------------

    async def stream_async(self, symbols: List[str], reconnect: bool = True, start: Optional[str] = None):
        """Async generator that yields Tick objects.

        Args:
            symbols: List of symbols to subscribe to.
            reconnect: If True, automatically reconnect on disconnect.
            start: Optional start time for historical replay (ISO 8601 or epoch).

        Yields:
            Tick objects as they arrive.

        Example:
            import asyncio
            from lse import LSE

            async def main():
                client = LSE(api_key="your_key")
                async for tick in client.stream_async(["BTC/USD"], start="2026-04-18T09:00:00"):
                    print(tick)

            asyncio.run(main())
        """
        self._disconnect_requested = False
        while not self._disconnect_requested:
            try:
                async for tick in self._stream_once(symbols, start=start):
                    yield tick
            except Exception as e:
                self._emit("error", str(e))
                self._emit("disconnected")
            # Stop on disconnect(), a fatal error (bad key / over quota, which
            # sets _disconnect_requested), or reconnect=off, whether the
            # connection ended cleanly or via exception. Otherwise back off.
            if self._disconnect_requested or not reconnect:
                return
            await asyncio.sleep(3)

    async def connect_async(self, symbols: Optional[List[str]] = None):
        """Async version of connect(). Blocks forever until disconnect()."""
        self._disconnect_requested = False
        await self._run_forever(symbols or [])

    async def disconnect_async(self):
        """Async version of disconnect(). Call from within an async context."""
        self._disconnect_requested = True
        if self._ws:
            await self._ws.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _stream_once(self, symbols: List[str], start: Optional[str] = None):
        """Single connection session. Yields ticks until disconnect."""
        # Store the event loop so sync methods (subscribe, disconnect) can
        # schedule coroutines from other threads via run_coroutine_threadsafe
        self._loop = asyncio.get_running_loop()

        async with websockets.connect(
            self._url,
            ping_interval=PING_INTERVAL,
            ping_timeout=30,
        ) as ws:
            self._ws = ws
            self._authenticated = False

            # Wait for welcome
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("type") == "welcome":
                self._emit("connected")

            # Authenticate
            await ws.send(json.dumps({
                "action": "auth",
                "api_key": self._api_key,
            }))

            # Start keepalive pings in background
            ping_task = asyncio.create_task(self._ping_loop(ws))

            try:
                async for raw in ws:
                    msg = json.loads(raw)
                    msg_type = msg.get("type")

                    if msg_type == "authenticated":
                        self._authenticated = True
                        self._tier = msg.get("tier", "")
                        self._symbols = msg.get("symbols", [])
                        self._emit("authenticated")

                        # Subscribe to requested symbols: the connect()/stream()
                        # argument PLUS anything added via subscribe() (before
                        # connect, or mid-session before a reconnect). Both live
                        # in _subscriptions, so replaying the whole set fixes
                        # subscribe()-before-connect and restores subscriptions
                        # after a reconnect, mirroring _option_underlyings below.
                        for sym in symbols:
                            self._subscriptions.add(sym)
                        for sym in self._subscriptions:
                            sub_msg = {"action": "subscribe", "symbol": sym}
                            if start:
                                sub_msg["start"] = start
                            await ws.send(json.dumps(sub_msg))

                        # Re-subscribe to any option underlyings that were
                        # added via subscribe_options() before connect, or
                        # that need restoring after a reconnect
                        for underlying in self._option_underlyings:
                            await ws.send(json.dumps({
                                "action": "subscribe_options",
                                "underlying": underlying,
                            }))

                    elif msg_type == "tick":
                        # Option contracts arrive with OSI symbols; from_symbol
                        # upgrades those to OptionTick (parsed strike/expiry/right)
                        # and returns a plain Tick for everything else.
                        tick = OptionTick.from_symbol(
                            symbol=msg.get("symbol", ""),
                            price=msg.get("price", 0.0),
                            bid=msg.get("bid"),
                            ask=msg.get("ask"),
                            volume=msg.get("volume"),
                            timestamp=msg.get("ts"),
                            name=msg.get("name"),
                            replay=msg.get("replay", False),
                        )
                        self._emit("tick", tick)
                        yield tick

                    elif msg_type == "replay_complete":
                        # Historical replay finished, live ticks follow
                        self._emit("replay_complete", msg)

                    elif msg_type == "replay_started":
                        # Server confirmed replay is starting
                        self._emit("replay_started", msg)

                    elif msg_type == "error":
                        code = msg.get("code", "")
                        self._emit("error", msg.get("message", "Unknown error"))
                        # Fatal errors will never succeed on retry: a bad/inactive
                        # key, or a key that is over its monthly data limit. Any
                        # error that arrives before we authenticate is fatal too.
                        # Stop the (re)connect loop instead of hammering forever.
                        if code in ("INVALID_KEY", "MISSING_KEY", "QUOTA_EXCEEDED") or not self._authenticated:
                            self._disconnect_requested = True
                            break

                    elif msg_type in ("pong", "unsubscribed", "subscribed",
                                      "options_subscribed", "options_unsubscribed"):
                        # Server confirmations for lifecycle actions. No user
                        # action needed; the local state was already updated
                        # when the corresponding method was called.
                        pass

            finally:
                ping_task.cancel()
                self._ws = None
                self._authenticated = False

    async def _run_forever(self, symbols: List[str]):
        """Connect, subscribe, and dispatch events forever with auto-reconnect.

        Exits cleanly when disconnect() or disconnect_async() is called,
        which sets _disconnect_requested = True and closes the WebSocket.
        """
        while not self._disconnect_requested:
            try:
                async for tick in self._stream_once(symbols):
                    pass  # ticks dispatched via callbacks in _stream_once
            except Exception as e:
                if self._disconnect_requested:
                    break
                self._emit("error", str(e))
                self._emit("disconnected")
                await asyncio.sleep(3)

    async def _ping_loop(self, ws):
        """Send application-level pings to keep the connection alive."""
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                try:
                    await ws.send(json.dumps({"action": "ping"}))
                except Exception:
                    break
        except asyncio.CancelledError:
            pass
