"""
LSE WebSocket client for real-time market data streaming.

Connects to wss://data-ws.londonstrategicedge.com and provides a clean
interface for subscribing to symbols and receiving live price ticks.
"""

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional, Set

import websockets
import websockets.client


WS_URL = "wss://data-ws.londonstrategicedge.com"

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
    timestamp: Optional[float] = None
    name: Optional[str] = None
    replay: bool = False  # True for historical ticks during replay

    def __repr__(self) -> str:
        r = " REPLAY" if self.replay else ""
        return f"Tick({self.symbol} {self.price}{r})"


class LSE:
    """Client for London Strategic Edge real-time market data.

    Args:
        api_key: Your LSE API key. Get one at https://londonstrategicedge.com/data
        url: WebSocket endpoint. Defaults to the production server.

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

    def __init__(self, api_key: str, url: str = WS_URL):
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
        while True:
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
                if not reconnect or self._disconnect_requested:
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
        while True:
            try:
                async for tick in self._stream_once(symbols, start=start):
                    yield tick
            except Exception as e:
                self._emit("error", str(e))
                self._emit("disconnected")
                if not reconnect or self._disconnect_requested:
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
                        self._emit("error", msg.get("message", "Unknown error"))

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
