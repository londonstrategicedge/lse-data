"""Client for London Strategic Edge market data: live streaming and historical download."""

from lse.client import LSE, Tick, OptionTick, LSEError, tape

__version__ = "0.14.0"
__all__ = ["LSE", "Tick", "OptionTick", "LSEError", "tape"]
