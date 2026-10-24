"""Client for London Strategic Edge market data: live streaming and LSE Data 3.0 history."""

from lse.client import LSE, Tick, OptionTick, LSEError, tape
from lse.data3 import DATA_KINDS, MACRO_KINDS, MARKETS, TIMEFRAMES, load

__version__ = "3.0"
__all__ = ["LSE", "Tick", "OptionTick", "LSEError", "tape", "load",
           "MARKETS", "TIMEFRAMES", "DATA_KINDS", "MACRO_KINDS"]
