"""
LSE market data client: live streaming over WebSocket and historical
download over REST, both authorized by the same API key.

Streaming connects to wss://data-ws.londonstrategicedge.com. Every REST read
(candles, reference data, options, catalog, deep history) is served from the
LSE vault at https://api.londonstrategicedge.com/vault: synchronous JSON row
queries for interactive pulls, async Parquet export jobs for bulk history
(see lse.vault). One key authorizes everything.
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
from datetime import date, datetime
from typing import Callable, Dict, Iterator, List, Optional, Set

import websockets
import websockets.client

from .vault import VaultMixin


WS_URL = "wss://data-ws.londonstrategicedge.com"
# Legacy /iso REST base (PostgREST grammar). Not called by this client any more:
# every REST read goes to the vault (lse.vault.VAULT_URL). The constant remains so
# older scripts importing it keep working against the still-live legacy endpoint.
API_URL = "https://api.londonstrategicedge.com/iso"
# The download host is behind a CDN that blocks the default Python-urllib
# User-Agent. Send our own so requests are not bounced before reaching the API.
_USER_AGENT = "lse-data-sdk (+https://londonstrategicedge.com)"


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


class LSE(VaultMixin):
    """Client for London Strategic Edge market data: live streaming, REST
    download, and deep vault history (see :mod:`lse.vault`).

    Args:
        api_key: Your LSE API key. Get one at https://londonstrategicedge.com/data
                 If omitted, the LSE_API_KEY environment variable is used.
        url: WebSocket endpoint. Defaults to the production server.
        timeout: Seconds to wait for each REST call (default 60). Raise it for
                 the heaviest queries, e.g. LSE(api_key=..., timeout=120).

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

    def __init__(self, api_key: Optional[str] = None, url: str = WS_URL,
                 timeout: float = 60):
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
        # Timeout (seconds) for REST calls. Defaults to 60 because the deepest
        # vault queries (for example 1s candles over a long span) can take tens
        # of seconds server side; 30s was cutting them off mid-read.
        self._rest_timeout = timeout
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
    # REST data download (historical) — same key as streaming. Every read here
    # is a synchronous JSON row query against the vault; bulk pulls are the
    # async Parquet exports in lse.vault (history()/dataset()).
    # ------------------------------------------------------------------

    # Vault row queries return UTC times as "YYYY-MM-DD hh:mm:ss[.ffffff]".
    # Normalise to ISO-8601 with an explicit Z so downstream parsers see an
    # unambiguous, timezone-aware string.
    _TIME_KEYS = {"timestamp", "ts", "minute", "datetime", "last_trade_at",
                  "updated_at", "created_at", "accepted_date", "fetched_at"}

    @classmethod
    def _isoify(cls, rows: List[dict]) -> List[dict]:
        for r in rows:
            for k in cls._TIME_KEYS:
                v = r.get(k)
                if isinstance(v, str) and len(v) >= 19 and v[10] == " ":
                    r[k] = v.replace(" ", "T", 1) + "Z"
        return rows

    @staticmethod
    def _symbol_slug(symbol: str) -> str:
        return symbol.lower().replace("/", "_").replace("-", "_").replace(".", "_")

    def _vault_rows(self, path: str, params: List[tuple]) -> List[dict]:
        """GET a synchronous vault query endpoint with the API key; returns row
        dicts. Raises LSEError on any non-2xx (bad filter, over quota, rate
        limited)."""
        from .vault import VAULT_URL
        qs = urllib.parse.urlencode([(k, str(v)) for k, v in params if v is not None])
        url = f"{VAULT_URL}{path}" + (f"?{qs}" if qs else "")
        req = urllib.request.Request(url, headers={
            "x-api-key": self._api_key,
            "User-Agent": _USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=self._rest_timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")
            msg = raw
            try:
                j = json.loads(raw)
                msg = j.get("detail") or j.get("message") or raw
            except Exception:
                pass
            raise LSEError(e.code, str(msg)[:300])
        except OSError as e:
            # Timeouts and transport failures must also surface as LSEError so
            # one except clause covers every failed call; URLError and
            # TimeoutError are both OSError subclasses. status 0 = no HTTP
            # response was received.
            raise LSEError(0, "request failed before an HTTP response: "
                              f"{getattr(e, 'reason', None) or e}")

    def candles(self, symbol: str, timeframe: str = "1m", start: Optional[str] = None,
                end: Optional[str] = None, limit: int = 5000, order: str = "asc",
                dataset: Optional[str] = None) -> List[dict]:
        """Historical OHLCV candles for any non-option instrument, straight from
        the vault (stocks back to 2003, FX to 2009, crypto to 2017).

        timeframe: any vault resolution: "1s", "5s", "15s", "30s", "1m", "3m",
        "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mo".
        start / end: ISO timestamps (e.g. "2026-01-01") filtering the bar time.
        Page with start/end for longer ranges; each call returns at most the
        plan's row cap. dataset pins the asset class when a symbol exists in
        more than one (rarely needed).

        Example:
            client.candles("BTC/USD", "1d", start="2026-01-01")
            client.candles("AAPL", "1h", limit=200, order="desc")
            client.candles("EUR/USD", "5s", start="2026-07-01", end="2026-07-02")
        """
        p: List[tuple] = [("symbol", symbol), ("timeframe", str(timeframe).lower()),
                          ("order", order), ("limit", min(int(limit), 5000)),
                          ("dataset", dataset), ("start", start), ("end", end)]
        rows = self._vault_rows("/candles", p)
        for r in rows:
            # The vault labels the bar-open time `ts`; this API has always said
            # `timestamp`, so keep that contract.
            if "ts" in r:
                r["timestamp"] = r.pop("ts")
            r.setdefault("volume", 0.0)  # fx candles carry no consolidated volume
        return self._isoify(rows)

    def _ref_rows(self, dataset: str, extra: List[tuple], start, end, order, limit) -> List[dict]:
        p: List[tuple] = list(extra) + [("start", start), ("end", end),
                                        ("order", order), ("limit", min(int(limit), 5000))]
        return self._isoify(self._vault_rows(f"/ref/{dataset}", p))

    def economic_calendar(self, region=None, event: Optional[str] = None,
                          start: Optional[str] = None, end: Optional[str] = None,
                          released_only: bool = False, order: str = "asc",
                          limit: int = 5000) -> List[dict]:
        """Macro economic events (CPI, NFP, rate decisions, GDP, ...).
        region: a code like "US" or a list like ["US","EU","GB"].
        released_only: only events whose `actual` has printed."""
        if isinstance(region, (list, tuple, set)):
            region = ",".join(region)
        extra = [("region", region), ("event", event),
                 ("released", 1 if released_only else None)]
        return self._ref_rows("economic_calendar", extra, start, end, order, limit)

    def insider_trades(self, symbol: Optional[str] = None, type: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       order: str = "desc", limit: int = 5000) -> List[dict]:
        """SEC Form 3/4/5 insider transactions. `type` is an SEC code, e.g.
        "P-Purchase" or "S-Sale"; start/end filter `transaction_date`."""
        return self._ref_rows("insider_trades", [("symbol", symbol), ("type", type)],
                              start, end, order, limit)

    def dividends(self, symbol: Optional[str] = None, start: Optional[str] = None,
                  end: Optional[str] = None, order: str = "desc", limit: int = 5000) -> List[dict]:
        """Dividend events; start/end filter the ex-date (`effective_date`)."""
        return self._ref_rows("dividends", [("symbol", symbol)], start, end, order, limit)

    def splits(self, symbol: Optional[str] = None, start: Optional[str] = None,
               end: Optional[str] = None, order: str = "desc", limit: int = 5000) -> List[dict]:
        """Stock split events; start/end filter `effective_date`."""
        return self._ref_rows("stock_splits", [("symbol", symbol)], start, end, order, limit)

    def series(self, symbol: str, dataset: Optional[str] = None,
               start: Optional[str] = None, end: Optional[str] = None,
               order: str = "asc", limit: int = 5000) -> List[dict]:
        """One (date, value) observation series from the vault: any macro
        economics series or a bond yield tenor, and every series-shaped
        dataset added later. The class resolves from the catalog, so
        ``series("cpi_yoy")`` and ``series("US10Y")`` both just work.

        Example:
            client.series("cpi_yoy")                       # US inflation rate
            client.series("fdtr", start="1980-01-01")      # Fed funds since 1980
            client.series("DE10Y", order="desc", limit=30)
        """
        p: List[tuple] = [("symbol", symbol), ("dataset", dataset),
                          ("start", start), ("end", end),
                          ("order", order), ("limit", min(int(limit), 5000))]
        return self._vault_rows("/series", p)

    def cot(self, symbol: Optional[str] = None, start: Optional[str] = None,
            end: Optional[str] = None, order: str = "asc", limit: int = 5000) -> List[dict]:
        """CFTC Commitments of Traders: weekly positioning per futures market
        (commercial, non-commercial and non-reportable longs/shorts, open
        interest, week-over-week changes)."""
        return self._ref_rows("cot", [("symbol", symbol)], start, end, order, limit)

    def financial_reports(self, symbol: Optional[str] = None,
                          report_type: Optional[str] = None, period: Optional[str] = None,
                          start: Optional[str] = None, end: Optional[str] = None,
                          order: str = "desc", limit: int = 5000) -> List[dict]:
        """Company financial statements. report_type: "income", "balance" or
        "cashflow"; period: "FY" or a quarter like "Q1". Each row's `data` field
        holds the full statement and is returned parsed."""
        rows = self._ref_rows("financial_reports",
                              [("symbol", symbol), ("report_type", report_type),
                               ("period", period)], start, end, order, limit)
        for r in rows:
            if isinstance(r.get("data"), str):
                try:
                    r["data"] = json.loads(r["data"])
                except ValueError:
                    pass
        return rows

    def company_profiles(self, symbol: Optional[str] = None, limit: int = 5000) -> List[dict]:
        """Company reference profiles (sector, industry, description, listing
        details). Omit symbol for the whole set."""
        return self._ref_rows("company_profiles", [("symbol", symbol)], None, None, "asc", limit)

    def fundamentals(self, symbol: Optional[str] = None, limit: int = 5000) -> List[dict]:
        """Snapshot fundamentals per stock (market cap, PE, margins, 52 week
        range, dividend yield)."""
        return self._ref_rows("stock_fundamentals", [("symbol", symbol)], None, None, "asc", limit)

    def bond_yields(self, symbol: Optional[str] = None, start: Optional[str] = None,
                    end: Optional[str] = None, order: str = "asc", limit: int = 5000) -> List[dict]:
        """Government bond yield history (daily OHLC per tenor symbol, e.g.
        "US10Y", 31 countries back to 1990)."""
        return self._ref_rows("bond_yields", [("symbol", symbol)], start, end, order, limit)

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
        catalog symbol always wins; otherwise the closest match among the
        optionable underlyings is used (prefix matches first, then the
        shortest name, so "apple" resolves to Apple Inc. rather than Apple
        Hospitality REIT). Name matching is restricted to the options dataset
        so an economics series can never shadow a company."""
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
        pool = [x for x in items if x.get("dataset") == "options"] or items
        hits = [x for x in pool if ql in (x.get("name") or "").lower()]
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
        p: List[tuple] = [("underlying", sym), ("limit", min(int(limit), 5000))]
        if type:
            right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
            if not right:
                raise LSEError(400, f"type must be 'call' or 'put', got '{type}'")
            p.append(("type", right))
        if expiry:
            p.append(("expiry", expiry))
        if strike is not None:
            if isinstance(strike, (tuple, list)):
                p.append(("strike_min", strike[0]))
                p.append(("strike_max", strike[1]))
            else:
                p.append(("strike", strike))
        if min_dte is not None:
            p.append(("min_dte", int(min_dte)))
        if max_dte is not None:
            p.append(("max_dte", int(max_dte)))
        return self._clean_option_rows(self._isoify(self._vault_rows("/options/chain", p)))

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
        p: List[tuple] = [("order", order), ("limit", min(int(limit), 5000)),
                          ("start", start), ("end", end)]
        if underlying:
            p.append(("underlying", self._resolve_underlying(underlying)))
        if type:
            right = self._OPTION_TYPE_ALIASES.get(str(type).lower())
            if not right:
                raise LSEError(400, f"type must be 'call' or 'put', got '{type}'")
            p.append(("type", right))
        if min_premium is not None:
            p.append(("min_premium", min_premium))
        if expiry:
            p.append(("expiry", expiry))
        if max_dte is not None:
            p.append(("max_dte", int(max_dte)))
        return self._clean_option_rows(self._isoify(self._vault_rows("/options/flow", p)))

    def option_candles(self, contract: str, strike=None, expiry=None,
                       type: Optional[str] = None,
                       start: Optional[str] = None, end: Optional[str] = None,
                       order: str = "asc", limit: int = 5000) -> List[dict]:
        """1 minute premium OHLC history for one contract, with volume,
        premium and averaged greeks per bar.

        Address the contract either way:
            client.option_candles("AAPL260612C00205000")
            client.option_candles("AAPL", strike=205, expiry="2026-06-12", type="call")

        The vault merges the compacted bar archive with bars folded live from
        the most recent prints, so the trailing days always agree with
        options_flow().
        """
        osi = self._resolve_contract(contract, strike=strike, expiry=expiry, type=type)
        p: List[tuple] = [("ticker", osi), ("order", order),
                          ("limit", min(int(limit), 5000)),
                          ("start", start), ("end", end)]
        return self._clean_option_rows(self._isoify(self._vault_rows("/options/candles", p)))

    def options_underlyings(self) -> List[dict]:
        """Every underlying with listed options, as [{"symbol", "name"}, ...],
        from the vault catalog. Feed any entry straight into options(),
        options_flow() or subscribe_options()."""
        return [{"symbol": r["symbol"], "name": r.get("name") or ""}
                for r in self.datasets("options")]

    _LEGACY_HTF = {"x_candles_5m": "5m", "x_candles_15m": "15m", "x_candles_1h": "1h",
                   "x_candles_4h": "4h", "x_candles_1d": "1d"}

    def _symbol_from_slug(self, slug: str) -> str:
        for x in self.catalog():
            if self._symbol_slug(x["symbol"]) == slug:
                return x["symbol"]
        raise LSEError(404, f"no instrument matches '{slug}'")

    def get(self, table: str, **filters) -> List[dict]:
        """Legacy escape hatch, kept for compatibility: accepts the previously
        documented table names with PostgREST-style filters and maps them onto
        the vault query endpoints. New code should call the named methods
        (candles, insider_trades, options, ...) directly.

        Example:
            client.get("z_insider_trades", symbol="eq.AAPL", limit="10")
        """
        f = {k: str(v) for k, v in filters.items()}
        order = f.pop("order", "")
        direction = "desc" if order.endswith(".desc") else "asc"
        try:
            limit = int(f.pop("limit", 5000))
        except ValueError:
            limit = 5000

        def val(key, ops=("eq",)):
            raw = f.get(key)
            if raw is None:
                return None
            for op in ops:
                if raw.startswith(op + "."):
                    return raw[len(op) + 1:]
            return raw

        def rng(key):
            # kwargs carry one value per column, so a get() range was always
            # single-sided: gte. means start, lt./lte. means end.
            raw = f.get(key)
            if raw is None:
                return None, None
            if raw.startswith("gte."):
                return raw[4:], None
            if raw.startswith(("lt.", "lte.")):
                return None, raw.split(".", 1)[1]
            return None, None

        if table in self._LEGACY_HTF:
            sym = val("symbol")
            if not sym:
                raise LSEError(400, "symbol=eq.<SYMBOL> is required")
            start, end = rng("timestamp")
            return self.candles(sym, self._LEGACY_HTF[table], start=start, end=end,
                                limit=limit, order=direction)
        if table.startswith(("candles_", "d_candles_")):
            sym = self._symbol_from_slug(table.split("candles_", 1)[1])
            start, end = rng("timestamp")
            return self.candles(sym, "1m", start=start, end=end, limit=limit, order=direction)
        if table == "economic_calender":
            start, end = rng("datetime")
            region = val("region_code", ("eq", "in"))
            if region:
                region = region.strip("()")
            event = val("event", ("ilike",))
            return self.economic_calendar(region=region,
                                          event=event.strip("*") if event else None,
                                          start=start, end=end,
                                          released_only=f.get("actual") == "not.is.null",
                                          order=direction, limit=limit)
        if table == "z_insider_trades":
            start, end = rng("transaction_date")
            return self.insider_trades(val("symbol"), type=val("transaction_type"),
                                       start=start, end=end, order=direction, limit=limit)
        if table == "dividends":
            start, end = rng("effective_date")
            return self.dividends(val("symbol"), start=start, end=end,
                                  order=direction, limit=limit)
        if table == "stock_splits":
            start, end = rng("effective_date")
            return self.splits(val("symbol"), start=start, end=end,
                               order=direction, limit=limit)
        if table == "x_options_chain":
            und = val("underlying")
            if not und:
                raise LSEError(400, "underlying=eq.<SYMBOL> is required")
            dmin, dmax = None, None
            raw_dte = f.get("dte")
            if raw_dte:
                if raw_dte.startswith("gte."):
                    dmin = int(raw_dte[4:])
                elif raw_dte.startswith("lte."):
                    dmax = int(raw_dte[4:])
            return self.options(und, type=val("contract_type"), expiry=val("expiry"),
                                strike=val("strike"), min_dte=dmin, max_dte=dmax, limit=limit)
        if table == "x_options_flow":
            start, end = rng("ts")
            prem = f.get("premium")
            return self.options_flow(val("underlying"), type=val("contract_type"),
                                     min_premium=float(prem[4:]) if prem and prem.startswith("gte.") else None,
                                     expiry=val("expiry"), start=start, end=end,
                                     order=direction, limit=limit)
        if table == "x_options_flow_1m":
            tick = val("ticker")
            if not tick:
                raise LSEError(400, "ticker=eq.<OSI> is required")
            start, end = rng("minute")
            return self.option_candles(tick, start=start, end=end,
                                       order=direction, limit=limit)
        raise LSEError(400, f"'{table}' is not served any more; the REST API reads "
                            "the vault now. Use candles(), economic_calendar(), "
                            "insider_trades(), dividends(), splits(), cot(), "
                            "financial_reports(), company_profiles(), fundamentals(), "
                            "bond_yields(), options(), options_flow(), option_candles(), "
                            "or history()/dataset() for bulk Parquet pulls.")

    # Vault dataset -> the category label this API has always used, plus labels
    # for the classes the vault added. Kept as a mapping so catalog(category=...)
    # accepts both vocabularies.
    _CATEGORY_LABELS = {
        "stocks": "Stocks", "fx": "Forex", "crypto": "Crypto", "etf": "ETFs",
        "index": "Indices", "commodity": "Commodities", "options": "Options",
        "eurex": "Futures", "economics": "Economics", "bonds": "Bonds",
        "volatility": "Volatility", "interest_rates": "Interest rates",
        "currency_index": "Currency index",
    }

    def catalog(self, category: Optional[str] = None) -> List[dict]:
        """Every instrument in the vault, one dict per (dataset, symbol) with
        {"symbol", "name", "category", "dataset", "ticks", "first", "last",
        "country"}. Requires the API key (it reads the live vault catalog);
        use it to discover exact symbols and their history span before
        streaming or downloading.

        category (optional): stock(s), forex/fx, crypto, etf(s), commodity(ies),
        index/indices, options, futures/eurex, economics, bonds.

        Example:
            client.catalog()                   # every instrument, 22,000+ rows
            crypto = client.catalog("crypto")  # [{"symbol": "BTC/USD", ...}, ...]
            symbols = [x["symbol"] for x in client.catalog("forex")]
        """
        if self._catalog_cache is None:
            self._catalog_cache = [
                {"symbol": r.get("symbol", ""), "name": r.get("name") or "",
                 "category": self._CATEGORY_LABELS.get(r.get("dataset", ""),
                                                       str(r.get("dataset", "")).title()),
                 "dataset": r.get("dataset", ""), "ticks": r.get("ticks"),
                 "first": r.get("first_tick"), "last": r.get("last_tick"),
                 "country": r.get("country_name") or None}
                for r in self.datasets()
            ]
        items = self._catalog_cache
        if category:
            norm = {
                "stock": "Stocks", "stocks": "Stocks", "equity": "Stocks", "equities": "Stocks",
                "forex": "Forex", "fx": "Forex", "crypto": "Crypto",
                "etf": "ETFs", "etfs": "ETFs",
                "commodity": "Commodities", "commodities": "Commodities",
                "index": "Indices", "indices": "Indices",
                "option": "Options", "options": "Options",
                "future": "Futures", "futures": "Futures", "eurex": "Futures",
                "economic": "Economics", "economics": "Economics",
                "bond": "Bonds", "bonds": "Bonds",
                "volatility": "Volatility",
                "interest_rates": "Interest rates", "rates": "Interest rates",
                "currency_index": "Currency index",
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
