"""Alias for lse-data. The canonical package is lse-data; the canonical import is `import lse`.

This module exists so that `import lsedata` also works for anyone who installed
the unhyphenated spelling, re-exporting the public API of `lse` unchanged.
"""

from lse import *  # noqa: F401,F403
from lse import __version__ as _lse_version  # noqa: F401
