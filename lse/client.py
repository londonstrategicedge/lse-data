"""
LSE market data client: live streaming over WebSocket and history over HTTPS,
both authorised by the same API key.

Streaming connects to wss://data-ws.londonstrategicedge.com. Every read of
history goes to LSE Data 3.0 at https://download.londonstrategicedge.com/v3:
the catalogue of eleven markets, previews, estimates, resumable CSV and
Parquet exports with checksums, macro and reference data, options chains and
flow (see :mod:`lse.data3`). One key authorises everything.
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
from datetime import date, datetime, timedelta
from typing import Callable, Dict, Iterator, List, Optional, Set

import websockets
import websockets.client

from .data3 import (DATA_KINDS, DEFAULT_BASE_URL, MARKETS, PAID_FEEDS, TIMEFRAMES, Data3Mixin,
                    LSEError, load)


WS_URL = "wss://data-ws.londonstrategicedge.com"
# The LSE Data 3.0 server. Every read of history goes here.
DATA_URL = DEFAULT_BASE_URL
# The retired REST base of releases before 3.0. Kept so older scripts that
# import the constant keep importing; nothing in this client calls it.
API_URL = "https://api.londonstrategicedge.com/iso"

# Seconds between the client's keep-alive pings on the WebSocket.
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


# OSI contract symbol as the stream sends it:
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


class LSE(Data3Mixin):
    """Client for London Strategic Edge market data: live streaming over the
    WebSocket and history from LSE Data 3.0 (see :mod:`lse.data3`).

    Args:
        api_key: Your LSE API key. Get one at https://londonstrategicedge.com/data
                 If omitted, the LSE_API_KEY environment variable is used.
        url: WebSocket endpoint. Defaults to the production server.
        timeout: Seconds to wait for each HTTP call (default 60).
        data_url: The LSE Data 3.0 server. Defaults to the production server.
        retries: Retries of a call the server answered 429 or 5xx (default 3).
        poll_seconds: How often a prepared export is polled (default 0.5).
        wait_for_limit: Past the plan's calls a minute the server answers 429
                 with Retry-After; by default the client waits that long once
                 and tries again. False raises LSEError at once.

    Works as a context manager (``with LSE(...) as client:``), disconnecting
    on exit.

    Examples:
        Synchronous streaming:

            from lse import LSE

            client = LSE(api_key="your_key")
            for tick in client.stream(["BTC/USD", "AAPL"]):
                print(tick.symbol, tick.price)

        History as rows, and as a file:

            rows = client.candles("AAPL", "1h", start="2026-10-01", end="2026-10-14")
            path = client.export("us_stocks", "AAPL", "5m", "2026-10-08", "2026-10-14", format="csv")

        Async streaming:

            import asyncio
            from lse import LSE

            async def main():
                client = LSE(api_key="your_key")
                async for tick in client.stream_async(["BTC/USD"]):
                    print(tick)

            asyncio.run(main())
    """

    def __init__(self, api_key: Optional[str] = None, url: str = WS_URL,
                 timeout: float = 60, *, data_url: str = DATA_URL,
                 retries: int = 3, poll_seconds: float = 0.5, wait_for_limit: bool = True):
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
        # options gives you every AAPL contract as a single subscription.
        self._option_underlyings: Set[str] = set()
        # Set when disconnect() is called so reconnect loops know to stop
        self._disconnect_requested = False
        # Reference to the event loop running _run_forever, used by disconnect()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # Timeout (seconds) for HTTP calls.
        self._rest_timeout = timeout
        # The LSE Data 3.0 server, checked once: an origin, nothing more.
        base = data_url.rstrip("/")
        parsed = urllib.parse.urlsplit(base)
        if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
            raise ValueError("data_url must contain only the server origin")
        if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1")):
            raise ValueError("The data server requires HTTPS; plain HTTP is allowed only for loopback development")
        if retries < 0 or poll_seconds < 0:
            raise ValueError("retries and poll_seconds must be non-negative")
        self.base_url = base
        self.retries, self.poll_seconds = retries, poll_seconds
        self.wait_for_limit = wait_for_limit
        self._market_rows: Optional[Dict[str, dict]] = None
        # Cached instrument catalogue (fetched once, on first catalog() call).
        self._catalog_cache: Optional[List[dict]] = None

    @property
    def api_key(self) -> str:
        return self._api_key

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
        """Account tier as reported by the server (e.g. 'registered')."""
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
            start: Optional start of a replay: an ISO 8601 time in UTC
                   (e.g. "2026-10-24T09:00:00") or an epoch timestamp, at most
                   24 hours back. The server replays ticks from this point,
                   then switches to live.

        Yields:
            Tick objects as they arrive (replay ticks first, then live).

        Example:
            from lse import LSE

            client = LSE(api_key="your_key")

            # Live only
            for tick in client.stream(["BTC/USD"]):
                print(f"{tick.symbol}: ${tick.price}")

            # Replay the last 2 hours, then live
            from datetime import datetime, timedelta, timezone
            start = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
            for tick in client.stream(["BTC/USD"], start=start):
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
        """Subscribe to more symbols. Works before connect() or stream() and
        while connected: the client sends them once it is authenticated, and
        again after a reconnect.
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
        contract individually.

        Ticks arrive as OptionTick objects (a Tick subclass) whose symbol is
        the contract, e.g. "AAPL261120C00100000".

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
    # History as rows: the readers of releases before 3.0, kept with their
    # names and row shapes, answered by LSE Data 3.0 underneath.
    # ------------------------------------------------------------------

    # Times arrive as "YYYY-MM-DD hh:mm:ss[.fraction]" in UTC. The readers below
    # have always said ISO-8601 with an explicit Z, so keep that contract.
    _TIME_KEYS = {"timestamp", "ts", "minute", "datetime", "last_trade_at", "greeks_at",
                  "underlying_at", "price_at", "released_at", "updated_at", "created_at",
                  "accepted_date", "fetched_at", "latest_at"}

    @classmethod
    def _isoify(cls, rows: List[dict]) -> List[dict]:
        for r in rows:
            for k in cls._TIME_KEYS:
                v = r.get(k)
                if isinstance(v, str) and len(v) >= 19 and v[10] == " ":
                    r[k] = v.replace(" ", "T", 1) + "Z"
        return rows

    # The asset classes of releases before 3.0, as market codes.
    _DATASET_MARKETS = {
        "stock": "us_stocks", "stocks": "us_stocks", "equity": "us_stocks", "equities": "us_stocks",
        "etf": "us_stocks", "etfs": "us_stocks", "us_stocks": "us_stocks", "uk_stocks": "uk_stocks",
        "ca_stocks": "ca_stocks", "fx": "forex", "forex": "forex", "crypto": "crypto",
        "index": "indices", "indices": "indices", "commodity": "commodities", "commodities": "commodities",
        "future": "futures", "futures": "futures", "option": "us_options",
        "options": "us_options", "us_options": "us_options", "opra": "us_options", "cme": "cme",
        "nasdaq": "nasdaq",
    }
    _CATEGORY_LABELS = {
        "us_stocks": "Stocks", "uk_stocks": "UK stocks", "ca_stocks": "Canadian stocks",
        "forex": "Forex", "crypto": "Crypto", "indices": "Indices", "commodities": "Commodities",
        "futures": "Futures", "us_options": "Options", "cme": "CME Globex", "nasdaq": "Nasdaq TotalView",
    }
    # Markets whose instruments are listed one by one by catalog(). The two
    # contract markets (US options and CME Globex) are listed by underlying
    # instead; pairs() pages through their contracts.
    _LISTED_MARKETS = ("us_stocks", "uk_stocks", "ca_stocks", "forex", "crypto", "futures",
                       "indices", "commodities")
    # catalog() also lists Nasdaq TotalView's instruments one by one. The symbol
    # resolver keeps to the markets above: a paid feed is never chosen by guess.
    _CATALOG_MARKETS = _LISTED_MARKETS + ("nasdaq",)
    _CONTRACT_MARKETS = ("us_options", "cme")
    _TIMEFRAME_SECONDS = {"tick": 60, "1s": 1, "5s": 5, "15s": 15, "30s": 30, "1m": 60, "3m": 180,
                          "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
                          "1w": 604800, "1mo": 2678400}

    def _resolve_market(self, symbol: Optional[str], dataset: Optional[str] = None,
                        market: Optional[str] = None) -> str:
        return self._resolve_instrument(symbol, dataset, market)[0]

    def _resolve_instrument(self, symbol: Optional[str], dataset: Optional[str] = None,
                            market: Optional[str] = None) -> tuple:
        """The (market, symbol) of an instrument: the market as given, from the
        old asset class name, or found in the catalogue; the symbol as the
        catalogue spells it, so "EUR/USD" resolves to EUR_USD in forex and
        "BTC/USD" to BTCUSDT in crypto. A symbol listed in several markets
        resolves to US stocks when it is one, else to the one listed market that
        has it; the paid feeds and the contract markets are never chosen by
        guess. A symbol found once is not looked up again by this client."""
        if dataset and not market:
            market = self._DATASET_MARKETS.get(str(dataset).lower())
            if not market:
                raise LSEError(400, f"'{dataset}' is not a market; markets() lists them", "market-unknown")
        if market:
            return market, symbol
        if not symbol:
            raise LSEError(400, "pass a symbol, a market, or both", "symbol-missing")
        want = symbol.strip().upper()
        cache = self.__dict__.setdefault("_instruments", {})
        if want in cache:
            return cache[want]
        page = self._json("/v3/pairs", symbol=symbol, limit=50)
        rows = [r for r in page.get("pairs") or [] if str(r.get("symbol", "")).upper() == want and r.get("folder")]
        if not rows:
            # The name spelling, such as EUR/USD for EUR_USD.
            page = self._json("/v3/pairs", q=symbol, limit=50)
            rows = [r for r in page.get("pairs") or []
                    if str(r.get("name", "")).upper() == want and r.get("folder") not in self._CONTRACT_MARKETS]
        base, sep, quote = want.replace("_", "/").replace("-", "/").partition("/")
        if not rows and sep and base and quote:
            # A crypto pair as 0.x wrote it: BTC/USD is listed against Tether as BTCUSDT.
            for spelt in dict.fromkeys([base + quote, base + "USDT" if quote == "USD" else base + quote]):
                page = self._json("/v3/pairs", market="crypto", symbol=spelt, limit=5)
                rows = [dict(r, folder=r.get("folder") or "crypto") for r in page.get("pairs") or []
                        if str(r.get("symbol", "")).upper() == spelt]
                if rows:
                    break
        if not rows:
            raise LSEError(404, f"'{symbol}' is not in the catalogue; try client.catalog() or pass market=",
                           "symbol-unknown")
        cache[want] = found = self._pick_market(symbol, rows)
        return found

    def _pick_market(self, symbol: str, rows: List[dict]) -> tuple:
        by_market = {}
        for r in rows:
            by_market.setdefault(r["folder"], r["symbol"])
        if "us_stocks" in by_market:
            return "us_stocks", by_market["us_stocks"]
        listed = [m for m in by_market if m in self._LISTED_MARKETS]
        if len(listed) == 1:
            return listed[0], by_market[listed[0]]
        if not listed and len(by_market) == 1:
            m = next(iter(by_market))
            return m, by_market[m]
        raise LSEError(400, f"'{symbol}' exists in {sorted(by_market)}; pass market= to pick one", "market-ambiguous")

    @staticmethod
    def _day(value) -> Optional[str]:
        return str(value)[:10] if value else None

    def _window(self, timeframe: str, start, end, limit: int):
        """The date window of a row read: as given, else enough recent days to
        hold ``limit`` bars of this timeframe."""
        if start and end:
            return start, end
        today = date.today()
        if end and not start:
            end_day = date.fromisoformat(str(end)[:10])
            days = max(3, min(3660, int(limit * self._TIMEFRAME_SECONDS.get(timeframe, 60) * 2 / 86400) + 3))
            return str(end_day - timedelta(days=days)), end
        if start and not end:
            return start, str(today)
        days = max(3, min(3660, int(limit * self._TIMEFRAME_SECONDS.get(timeframe, 60) * 2 / 86400) + 3))
        return str(today - timedelta(days=days)), str(today)

    def _span_rows(self, market: str, symbol: str, timeframe: str, start: str, end: Optional[str], limit: int,
                   forward: bool, adjusted: bool) -> List[dict]:
        """At most ``limit`` rows of a window, from its start (forward) or from
        its end, read a span of days at a time, so a window of years is never
        fetched whole to keep its first few thousand rows. A span with no
        stored rows doubles the next one. The spans keep the window's own
        spelling: dates (both ends included) or UTC timestamps (the upper
        bound excluded); since the server refuses one of each, a date beside a
        timestamp is read as that whole UTC day, so a page can start at the
        last bar's timestamp and still end on a date."""
        timed = len(str(start)) > 10 or (end is not None and len(str(end)) > 10)
        lo = date.fromisoformat(str(start)[:10])
        hi = date.fromisoformat(str(end)[:10]) if end else date.today()
        one = timedelta(days=1)
        base = timedelta(days=max(3, min(3660, int(limit * self._TIMEFRAME_SECONDS.get(timeframe, 60) * 2 / 86400) + 3)))

        def edge(day: date, upper: bool) -> str:
            given = end if upper else start
            if given and day == (hi if upper else lo) and (len(str(given)) > 10 or not timed):
                return given
            if not timed:
                return str(day)
            return f"{day + one if upper else day} 00:00:00"

        rows: List[dict] = []
        step, missing = base, None
        a, b = (lo, min(hi, lo + base)) if forward else (max(lo, hi - base), hi)
        while True:
            try:
                part = self.rows(market, symbol, timeframe, edge(a, False), edge(b, True), adjusted=adjusted)
            except LSEError as e:
                if e.code not in ("data-unavailable", "date-not-trading"):
                    raise
                part, missing = [], e
            rows = rows + part if forward else part + rows
            if len(rows) >= limit or (b >= hi if forward else a <= lo):
                break
            step = base if part else step * 2
            a, b = (b + one, min(hi, b + one + step)) if forward else (max(lo, a - one - step), a - one)
        if not rows and missing:
            raise missing
        return rows[:limit] if forward else rows[-limit:]

    def candles(self, symbol: str, timeframe: str = "1m", start: Optional[str] = None,
                end: Optional[str] = None, limit: int = 5000, order: str = "asc",
                dataset: Optional[str] = None, market: Optional[str] = None,
                adjusted: bool = True) -> List[dict]:
        """OHLCV candles as rows: ``timestamp`` (ISO 8601, UTC), ``symbol``,
        ``open``, ``high``, ``low``, ``close``, ``volume``, ``trades``.

        timeframe: "1s", "5s", "15s", "30s", "1m", "3m", "5m", "15m", "30m",
        "1h", "4h", "1d", "1w", "1mo". start / end: dates (both included) or
        UTC timestamps (end excluded), not one of each. At most ``limit`` bars
        come back, as in 0.x: from ``start`` onwards in ``asc`` order, the
        newest of the window in ``desc``, so a long window is read page by page
        from the last bar's timestamp. Without ``start`` the most recent
        ``limit`` bars are read. The market resolves from the symbol; pass
        ``market=`` (or the old ``dataset=`` class name) when a symbol exists
        in more than one. The rows are held in memory; for years of candles
        use :meth:`history` or :meth:`export`, which save a file.

        Example:
            client.candles("BTCUSDT", "1d", start="2026-01-01", end="2026-10-01")
            client.candles("AAPL", "1h", limit=200, order="desc")
        """
        if order not in ("asc", "desc"):
            raise ValueError("order is asc or desc")
        tf = str(timeframe).lower()
        if tf == "tick":
            raise ValueError("candles() reads candle timeframes; use rows(..., timeframe='tick') for the trade slice")
        mkt, sym = self._resolve_instrument(symbol, dataset, market)
        if start:
            rows = self._span_rows(mkt, sym, tf, start, end, limit, order == "asc", adjusted)
        else:
            start, end = self._window(tf, start, end, limit)
            rows = self.rows(mkt, sym, tf, start, end, adjusted=adjusted)[-limit:]
        for r in rows:
            r["timestamp"] = r.pop("time", None)
            r["symbol"] = symbol
            r.setdefault("volume", 0)
        rows = self._isoify(rows)
        if order == "desc":
            rows.reverse()
        return rows

    # ── macro series ───────────────────────────────────────────────────

    # Series names of releases before 3.0 that have a different code now.
    _SERIES_ALIASES: Dict[str, str] = {"cpi_yoy": "US.inflation_rate", "fdtr": "US.interest_rate"}

    def _series_code(self, symbol: str) -> tuple:
        """The (kind, code) of a series named the old way or the new way: a
        yield tenor such as US10Y, an observation code such as
        US.core_inflation_rate, or a search when neither matches exactly. Of
        several search hits, the one named exactly, else the shortest code
        when every other hit is a variant of it (US.core_inflation_rate and
        its _mom); hits that are different series are refused, never guessed."""
        code = self._SERIES_ALIASES.get(symbol.strip().lower(), symbol)
        for kind in ("yield", "observation", "release"):
            page = self._json("/v3/macro", kind=kind, code=code, limit=1)
            if page.get("total"):
                return kind, page["series"][0]["code"]
        page = self._json("/v3/macro", kind="observation", q=code, limit=20)
        found = page.get("series") or []
        if not found:
            raise LSEError(404, f"'{symbol}' is not a macro series; macro_catalogue('observation', q=...) finds the code",
                           "series-unknown")
        q = code.strip().lower()
        named = [f for f in found if str(f.get("name", "")).lower() == q or str(f.get("code", "")).lower().endswith("." + q.replace(" ", "_"))]
        if named:
            return "observation", named[0]["code"]
        best = min(found, key=lambda f: len(str(f.get("code", ""))))
        if all(str(f.get("code", "")).startswith(best["code"]) for f in found):
            return "observation", best["code"]
        codes = ", ".join(str(f.get("code")) for f in found[:5])
        raise LSEError(400, f"'{symbol}' matches {page.get('total', len(found))} series ({codes}, ...); pass the code",
                       "series-ambiguous")

    def series(self, symbol: str, dataset: Optional[str] = None, start: Optional[str] = None,
               end: Optional[str] = None, order: str = "asc", limit: int = 5000) -> List[dict]:
        """One (date, value) series: a macro observation or a bond yield
        tenor, as rows ``{"symbol", "date", "value"}`` in time order.

        Example:
            client.series("US10Y")
            client.series("US.core_inflation_rate", start="2020-01-01")
        """
        if order not in ("asc", "desc"):
            raise ValueError("order is asc or desc")
        kind, code = self._series_code(symbol)
        page = self.macro_series(kind, code, self._day(start), self._day(end))
        value_key = {"observation": "value", "yield": "close", "release": "actual"}[kind]
        time_key = {"observation": "day", "yield": "day", "release": "released_at"}[kind]
        rows = [{"symbol": code, "date": r.get(time_key), "value": r.get(value_key)} for r in page.get("rows") or []]
        if order == "desc":
            rows.reverse()
        return rows[:limit]

    def economics(self, symbol: Optional[str] = None, start: Optional[str] = None,
                  end: Optional[str] = None, order: str = "asc", limit: int = 5000):
        """Macro economics series. Without a symbol, the catalogue of
        observation series (code, country, name, unit, frequency, first,
        last, stored_records); with one, its observations as rows."""
        if symbol is None:
            return list(self.iter_macro_series("observation"))
        return self.series(symbol, "economics", start=start, end=end, order=order, limit=limit)

    def bond_yields(self, symbol: Optional[str] = None, start: Optional[str] = None,
                    end: Optional[str] = None, order: str = "asc", limit: int = 5000) -> List[dict]:
        """Government bond yield history, daily OHLC per tenor symbol such as
        "US10Y", from the bond_yields reference collection."""
        return self._reference_read("bond_yields", symbol, start, end, order, limit)

    # ── reference readers ──────────────────────────────────────────────

    def _reference_read(self, dataset: str, symbol, start, end, order: str, limit: int,
                        country: Optional[str] = None, q: Optional[str] = None,
                        keep: Optional[Callable[[dict], bool]] = None) -> List[dict]:
        if order not in ("asc", "desc"):
            raise ValueError("order is asc or desc")
        rows = self.reference_rows(dataset, symbol, self._day(start), self._day(end), country=country, q=q)
        if keep:
            rows = [r for r in rows if keep(r)]
        rows = self._isoify(rows)
        if order == "desc":
            rows.reverse()
        return rows[:limit]

    def economic_calendar(self, region=None, event: Optional[str] = None,
                          start: Optional[str] = None, end: Optional[str] = None,
                          released_only: bool = False, order: str = "asc",
                          limit: int = 5000) -> List[dict]:
        """Macro economic events (CPI, NFP, rate decisions, GDP, ...) from the
        economic_calendar reference collection. ``region`` is a code like "US"
        or a list like ["US", "EU", "GB"]; ``released_only`` keeps the events
        whose ``actual`` has printed."""
        regions = None
        if isinstance(region, (list, tuple, set)):
            regions = {str(r).upper() for r in region}
        elif region:
            regions = {str(region).upper()}
        country = next(iter(regions)) if regions and len(regions) == 1 else None

        def keep(r):
            if regions and str(r.get("region_code") or "").upper() not in regions:
                return False
            if event and str(event).lower() not in str(r.get("event") or "").lower():
                return False
            if released_only and r.get("actual") in (None, ""):
                return False
            return True
        return self._reference_read("economic_calendar", None, start, end, order, limit,
                                    country=country, q=event, keep=keep)

    def insider_trades(self, symbol: Optional[str] = None, type: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       order: str = "desc", limit: int = 5000) -> List[dict]:
        """SEC Form 3/4/5 insider transactions. ``type`` is an SEC code such as
        "P-Purchase" or "S-Sale", matched against the transaction type."""
        want = str(type).lower() if type else None

        def keep(r):
            if not want:
                return True
            fields = " ".join(str(r.get(k) or "") for k in ("transaction_type", "trade_type", "acquisition_or_disposition"))
            return want in fields.lower() or want[:1] == str(r.get("transaction_type") or "").lower()[:1]
        return self._reference_read("insider_trades", symbol, start, end, order, limit, keep=keep)

    def dividends(self, symbol: Optional[str] = None, start: Optional[str] = None,
                  end: Optional[str] = None, order: str = "desc", limit: int = 5000) -> List[dict]:
        """Dividend events from the dividends reference collection."""
        return self._reference_read("dividends", symbol, start, end, order, limit)

    def splits(self, symbol: Optional[str] = None, start: Optional[str] = None,
               end: Optional[str] = None, order: str = "desc", limit: int = 5000) -> List[dict]:
        """Stock split events from the stock_splits reference collection."""
        return self._reference_read("stock_splits", symbol, start, end, order, limit)

    def cot(self, symbol: Optional[str] = None, start: Optional[str] = None,
            end: Optional[str] = None, order: str = "asc", limit: int = 5000) -> List[dict]:
        """CFTC Commitments of Traders: weekly positioning per futures market."""
        return self._reference_read("cot", symbol, start, end, order, limit)

    def financial_reports(self, symbol: Optional[str] = None,
                          report_type: Optional[str] = None, period: Optional[str] = None,
                          start: Optional[str] = None, end: Optional[str] = None,
                          order: str = "desc", limit: int = 5000) -> List[dict]:
        """Company financial statements. ``report_type``: "income", "balance"
        or "cashflow"; ``period``: "FY" or a quarter like "Q1". Each row's
        ``data`` field holds the statement, parsed from JSON."""
        want_type = str(report_type).lower() if report_type else None
        want_period = str(period).upper() if period else None

        def keep(r):
            if want_type and want_type not in str(r.get("report_type") or "").lower():
                return False
            if want_period and want_period != str(r.get("period") or "").upper():
                return False
            return True
        rows = self._reference_read("financial_reports", symbol, start, end, order, limit, keep=keep)
        for r in rows:
            if isinstance(r.get("data"), str):
                try:
                    r["data"] = json.loads(r["data"])
                except ValueError:
                    pass
        return rows

    def company_profiles(self, symbol: Optional[str] = None, limit: int = 5000) -> List[dict]:
        """Company reference profiles (sector, industry, description, listing details)."""
        return self._reference_read("company_profiles", symbol, None, None, "asc", limit)

    def fundamentals(self, symbol: Optional[str] = None, limit: int = 5000) -> List[dict]:
        """Snapshot fundamentals per stock from the stock_fundamentals collection."""
        return self._reference_read("stock_fundamentals", symbol, None, None, "asc", limit)

    # ── options ────────────────────────────────────────────────────────

    _OPTION_TYPE_ALIASES = {"c": "call", "call": "call", "calls": "call",
                            "p": "put", "put": "put", "puts": "put"}
    _OPTION_ROUND = {"last_price": 4, "premium": 2, "premium_today": 2, "price": 4,
                     "underlying_price": 4, "greeks_underlying_price": 4, "iv": 4,
                     "delta": 4, "gamma": 6, "theta": 4, "vega": 4,
                     "open": 4, "high": 4, "low": 4, "close": 4}

    @classmethod
    def _clean_option_rows(cls, rows: List[dict]) -> List[dict]:
        for r in rows:
            for k, nd in cls._OPTION_ROUND.items():
                v = r.get(k)
                if isinstance(v, float):
                    r[k] = round(v, nd)
        return rows

    @staticmethod
    def _occ(ticker: str) -> str:
        """The catalogue's spelling of a contract: the root padded to six characters."""
        m = _OSI_RE.match(ticker.replace(" ", "").upper())
        if not m:
            raise LSEError(400, f"'{ticker}' is not an option contract; pass a ticker like AAPL261120C00100000, "
                                "or an underlying plus strike=, expiry=, type=", "symbol-bad")
        root, yymmdd, cp, strike = m.groups()
        return f"{root:<6}{yymmdd}{cp}{strike}"

    def _resolve_underlying(self, query: str) -> str:
        """A ticker in any case ("AAPL", "aapl") or a company name ("apple",
        "Nvidia") to the underlying's symbol, through the options catalogue."""
        q = (query or "").strip()
        if not q:
            raise LSEError(400, "underlying is required", "underlying-missing")
        if re.fullmatch(r"[A-Za-z0-9.]{1,6}", q) and q.upper() == q:
            return q
        try:
            page = self._json("/v3/pairs", market="us_options", group_by="underlying", q=q, limit=20)
        except LSEError:
            return q.upper()
        hits = page.get("underlyings") or []
        exact = [h for h in hits if str(h.get("symbol", "")).upper() == q.upper()]
        if exact:
            return exact[0]["symbol"]
        if not hits:
            return q.upper()
        ql = q.lower()
        hits.sort(key=lambda h: (not str(h.get("name") or "").lower().startswith(ql), len(str(h.get("name") or ""))))
        return hits[0]["symbol"]

    def _resolve_contract(self, contract: str, strike=None, expiry=None, type: Optional[str] = None) -> str:
        if strike is None and expiry is None and type is None:
            return self._occ(contract)
        if strike is None or expiry is None or type is None:
            raise LSEError(400, "strike, expiry and type are all required when addressing a contract by its parts",
                           "contract-bad")
        right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
        if not right:
            raise LSEError(400, f"type must be 'call' or 'put', got '{type}'", "type-bad")
        exp = date.fromisoformat(str(expiry)) if not isinstance(expiry, date) else expiry
        root = self._resolve_underlying(contract)
        return self._occ(f"{root}{exp.strftime('%y%m%d')}{'C' if right == 'call' else 'P'}{int(round(float(strike) * 1000)):08d}")

    def options(self, underlying: str, type: Optional[str] = None,
                expiry: Optional[str] = None, strike=None,
                min_dte: Optional[int] = None, max_dte: Optional[int] = None,
                limit: int = 5000) -> List[dict]:
        """The chain of an underlying as rows: one per listed contract with its
        last trade, today's volume and premium, and the newest implied
        volatility and Greeks. ``underlying`` may be a ticker or a company
        name; ``strike`` one strike or an inclusive (low, high) window.
        :meth:`options_chain` returns the same with the page's totals."""
        sym = self._resolve_underlying(underlying)
        right = None
        if type:
            right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
            if not right:
                raise LSEError(400, f"type must be 'call' or 'put', got '{type}'", "type-bad")
        strike_min = strike_max = one = None
        if strike is not None:
            if isinstance(strike, (tuple, list)):
                strike_min, strike_max = strike[0], strike[1]
            else:
                one = strike
        rows: List[dict] = []
        offset = 0
        while len(rows) < limit:
            page = self.options_chain(sym, type=right, expiry=expiry, strike=one, strike_min=strike_min,
                                      strike_max=strike_max, min_dte=min_dte, max_dte=max_dte,
                                      limit=min(5000, limit - len(rows)), offset=offset)
            got = page.get("rows") or []
            rows.extend(got)
            offset += len(got)
            if not got or offset >= int(page.get("contracts") or 0):
                break
        return self._clean_option_rows(self._isoify(rows))

    def options_flow(self, underlying: Optional[str] = None,
                     type: Optional[str] = None,
                     min_premium: Optional[float] = None,
                     expiry: Optional[str] = None,
                     max_dte: Optional[int] = None,
                     start: Optional[str] = None, end: Optional[str] = None,
                     order: str = "desc", limit: int = 5000, min_dte: Optional[int] = None) -> List[dict]:
        """Prints of the options tape as rows, newest first, each with its
        premium, venue and the Greeks of its minute. ``start`` and ``end``
        bound the window, today by default; without an underlying the call
        sweeps every name over at most seven days.
        :meth:`options_flow_page` returns the page with its window."""
        right = None
        if type:
            right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
            if not right:
                raise LSEError(400, f"type must be 'call' or 'put', got '{type}'", "type-bad")
        sym = self._resolve_underlying(underlying) if underlying else None
        rows: List[dict] = []
        offset = 0
        while len(rows) < limit:
            page = self.options_flow_page(sym, type=right, min_premium=min_premium, expiry=expiry, min_dte=min_dte,
                                          max_dte=max_dte, start=start, end=end, order=order,
                                          limit=min(5000, limit - len(rows)), offset=offset)
            got = page.get("rows") or []
            rows.extend(got)
            offset += len(got)
            if len(got) < min(5000, limit - len(rows) + len(got)):
                break
        return self._clean_option_rows(self._isoify(rows))

    def option_candles(self, contract: str, strike=None, expiry=None,
                       type: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       order: str = "asc", limit: int = 5000, timeframe: str = "1m") -> List[dict]:
        """Candles of one contract as rows (``minute``, open, high, low, close,
        volume, trades, ``ticker``). Address the contract by its ticker
        ("AAPL261120C00100000") or by an underlying plus strike, expiry and
        type. Without dates, the last 30 days."""
        if order not in ("asc", "desc"):
            raise ValueError("order is asc or desc")
        occ = self._resolve_contract(contract, strike=strike, expiry=expiry, type=type)
        if not (start and end):
            today = date.today()
            start, end = start or str(today - timedelta(days=30)), end or str(today)
        rows = self.rows("us_options", occ, timeframe, start, end)
        ticker = occ.replace(" ", "")
        for r in rows:
            r["minute"] = r.pop("time", None)
            r["ticker"] = ticker
        rows = self._isoify(self._clean_option_rows(rows))
        if order == "desc":
            rows.reverse()
        return rows[:limit]

    def options_underlyings(self) -> List[dict]:
        """Every underlying with listed options, as [{"symbol", "name"}, ...]."""
        return [{"symbol": r.get("symbol") or r.get("id"), "name": r.get("name") or ""}
                for r in self.iter_pairs("us_options", group_by="underlying")]

    # ── catalogue in the old shape ─────────────────────────────────────

    def catalog(self, category: Optional[str] = None) -> List[dict]:
        """Every instrument, one dict per market and symbol: {"symbol", "name",
        "category", "dataset", "ticks", "first", "last", "country", "pair_id"}.
        Nasdaq TotalView's instruments ("nasdaq") are listed one by one. The two
        contract markets are listed by underlying: "options" gives the
        underlyings with listed options, "cme" the CME product families;
        :meth:`pairs` pages through their contracts.

        Example:
            client.catalog()                   # every listed instrument
            crypto = client.catalog("crypto")  # [{"symbol": "BTCUSDT", ...}, ...]
        """
        norm = {
            "stock": "us_stocks", "stocks": "us_stocks", "equity": "us_stocks", "equities": "us_stocks",
            "etf": "us_stocks", "etfs": "us_stocks", "us_stocks": "us_stocks", "uk_stocks": "uk_stocks",
            "ca_stocks": "ca_stocks", "forex": "forex", "fx": "forex", "crypto": "crypto",
            "commodity": "commodities", "commodities": "commodities", "index": "indices", "indices": "indices",
            "option": "us_options", "options": "us_options", "us_options": "us_options",
            "future": "futures", "futures": "futures", "cme": "cme",
            "nasdaq": "nasdaq", "totalview": "nasdaq",
        }
        if category:
            market = norm.get(str(category).lower())
            if not market:
                raise LSEError(400, f"'{category}' is not a market; markets() lists them", "market-unknown")
            return self._catalog_rows(market)
        if self._catalog_cache is None:
            rows: List[dict] = []
            for market in self._CATALOG_MARKETS + self._CONTRACT_MARKETS:
                try:
                    rows.extend(self._catalog_rows(market))
                except LSEError as e:
                    if e.code not in ("market-unknown", "market-not-enabled"):
                        raise
            self._catalog_cache = rows
        return list(self._catalog_cache)

    def _catalog_rows(self, market: str) -> List[dict]:
        label = self._CATEGORY_LABELS.get(market, market)
        if market in self._CONTRACT_MARKETS:
            return [{"symbol": r.get("symbol") or r.get("id"), "name": r.get("name") or "", "category": label,
                     "dataset": market, "ticks": None, "first": None, "last": None, "country": None,
                     "pair_id": None, "contracts": r.get("contract_count")}
                    for r in self.iter_pairs(market, group_by="underlying")]
        # Pages of 5,000: the server's tick counts come with pages that size,
        # not with pages of 10,000.
        return [{"symbol": r.get("symbol"), "name": r.get("name") or "", "category": label, "dataset": market,
                 "ticks": r.get("tick_count"), "first": r.get("tick_first"), "last": r.get("tick_last"),
                 "country": None, "pair_id": r.get("pair_id")}
                for r in self.iter_pairs(market, page_size=5000)]

    def datasets(self, dataset: Optional[str] = None) -> List[dict]:
        """Every listed instrument with its stored rows and span, one dict per
        market and symbol: {"dataset", "symbol", "name", "ticks", "first_tick",
        "last_tick"}. Pass a market code to filter to one."""
        rows = [{"dataset": r["dataset"], "symbol": r["symbol"], "name": r["name"], "ticks": r["ticks"],
                 "first_tick": r["first"], "last_tick": r["last"]}
                for r in (self.catalog(dataset) if dataset else self.catalog())]
        return rows

    def reference(self) -> List[dict]:
        """The reference collections: dataset, name, rows, series, first, last, as_of, columns."""
        return self.reference_catalogue()["datasets"]

    # ── files ──────────────────────────────────────────────────────────

    def history(self, symbol: Optional[str] = None, *, market: Optional[str] = None,
                dataset: Optional[str] = None, timeframe: str = "1m",
                start: Optional[str] = None, end: Optional[str] = None,
                dest: Optional[str] = None, dataframe: bool = True,
                format: str = "parquet", adjusted: bool = True, data: str = "candles",
                pair_id=None, progress: Optional[Callable] = None, timeout: float = 1800.0,
                poll_seconds: Optional[float] = None):
        """Pull history as a file, resumable and checked.

        ``history("AAPL", timeframe="1d", start="2020-01-01", end="2026-10-01")``
        resolves the market from the catalogue; pass ``market=`` when a symbol
        exists in more than one. ``timeframe`` is a candle width or "tick" for
        the trade slice. Without dates the whole stored history is asked for,
        which can be a large job. Returns a pandas DataFrame (needs the
        ``frames`` extra) or the saved file's path when ``dataframe=False``.
        """
        if not symbol and pair_id is None:
            raise LSEError(400, "pass a symbol or a pair_id", "symbol-missing")
        mkt, sym = self._resolve_instrument(symbol, dataset, market) if pair_id is None else (self._resolve_market(None, dataset, market), symbol)
        today = date.today()
        start = start or "2000-01-01"
        end = end or str(today)
        if poll_seconds is not None:
            self.poll_seconds = poll_seconds
        path = self.export(mkt, sym, timeframe, start, end, format=format, adjusted=adjusted, data=data,
                           pair_id=pair_id, dest=dest, progress=progress, timeout=timeout)
        return load(path) if dataframe else path

    def dataset(self, name: str, *, symbol: Optional[str] = None, start: Optional[str] = None,
                end: Optional[str] = None, dest: Optional[str] = None, dataframe: bool = True,
                format: str = "parquet", progress: Optional[Callable] = None, timeout: float = 1800.0,
                poll_seconds: Optional[float] = None):
        """Download a whole reference collection (insider_trades, dividends,
        bond_yields, economic_calendar, financial_reports, stock_splits, cot,
        company_profiles, stock_fundamentals and the rest) as one file,
        optionally one symbol or a date range."""
        if poll_seconds is not None:
            self.poll_seconds = poll_seconds
        path = self.reference_export(name, symbol, self._day(start), self._day(end), format=format, dest=dest,
                                     progress=progress, timeout=timeout)
        return load(path) if dataframe else path

    def get(self, table: str, **filters):
        """Removed in 3.0: the table reader of the retired REST base."""
        raise LSEError(410, "get() was removed in 3.0; use the named methods: candles(), series(), "
                            "insider_trades(), dividends(), splits(), options(), export() ...", "removed")

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
                async for tick in client.stream_async(["BTC/USD"]):
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

                        # Subscribe to everything in _subscriptions: the symbols
                        # passed to connect() or stream() and any added with
                        # subscribe(), so subscriptions made before connecting or
                        # before a reconnect take effect. Option underlyings follow.
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
                        # key, or a key the server refuses to serve. Any
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
