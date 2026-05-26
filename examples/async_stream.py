"""
Async streaming example - for use in async applications.

Usage:
    pip install lse-data
    python async_stream.py
"""

import asyncio
from lse import LSE


async def main():
    client = LSE(api_key="YOUR_API_KEY")

    async for tick in client.stream_async(["BTC/USD", "AAPL", "EUR/USD"]):
        print(f"{tick.symbol:12s} {tick.price:>12,.2f}  {tick.name or ''}")


asyncio.run(main())
