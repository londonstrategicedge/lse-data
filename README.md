# lse-data

Python WebSocket client for London Strategic Edge live market data.

## Install

```bash
pip install lse-data
```

## Usage

```python
from lse import LSE

client = LSE(api_key="your_api_key")
for tick in client.stream(["BTC/USD", "AAPL"]):
    print(tick.symbol, tick.price)
```

API key: https://londonstrategicedge.com/data

## Tick fields

| Field | Type |
|---|---|
| `symbol` | str |
| `price` | float |
| `bid` | float |
| `ask` | float |
| `volume` | float |
| `timestamp` | float |
| `name` | str |
| `replay` | bool |

## Async

```python
import asyncio
from lse import LSE

async def main():
    client = LSE(api_key="your_api_key")
    async for tick in client.stream_async(["BTC/USD"]):
        print(tick)

asyncio.run(main())
```

## Callbacks

```python
client = LSE(api_key="your_api_key")
client.on("tick", lambda t: print(t.symbol, t.price))
client.connect(["BTC/USD"])
```

Events: `tick`, `connected`, `authenticated`, `disconnected`, `error`.

## Options

```python
client.subscribe_options(["AAPL"])
client.connect()
```

## Replay

```python
for tick in client.stream(["BTC/USD"], start="2026-04-18T07:00:00"):
    print(tick)
```

`start` is ISO 8601 or epoch. Max 24h.

## CLI

```bash
lse auth lse_live_xxxxxxxxxxxx
lse stream BTC/USD AAPL
```

## License

MIT
