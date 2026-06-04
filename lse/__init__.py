"""Client for London Strategic Edge market data: live streaming and historical download."""

from lse.client import LSE, Tick, LSEError

__version__ = "0.10.0"
__all__ = ["LSE", "Tick", "LSEError"]
