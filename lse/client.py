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
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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

    def get(self, table: str, **filters) -> List[dict]:
        """Generic download: any downloadable table with raw query filters.
        Options and company profiles are not downloadable and return an error.

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

                        # Subscribe to requested symbols
                        for sym in symbols:
                            self._subscriptions.add(sym)
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
                        tick = Tick(
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
