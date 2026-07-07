"""
Vault access: deep history as Parquet, straight from the LSE databank.

The vault holds the full recorded history behind the platform: years of raw
ticks across every asset class, precomputed candles at fourteen resolutions,
macro economics series, and the reference tables (insider trades, dividends,
bond yields, the economic calendar and more). A pull is an async export job:
the vault builds a Parquet file server side, the client polls, then downloads
with resume support. The artifact lands on disk and, when pandas + pyarrow are
installed, also comes back as a DataFrame.

Everything here uses the same API key as streaming and REST download. No extra
dependency is required to fetch files; `pip install lse-data[frames]` adds the
DataFrame return path.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import List, Optional

VAULT_URL = "https://api.londonstrategicedge.com/vault"
_USER_AGENT = "lse-data-sdk (+https://londonstrategicedge.com)"


def _err(status: int, message: str):
    # LSEError lives in client.py, which imports this module; import lazily so the
    # two modules do not import each other at load time.
    from .client import LSEError
    return LSEError(status, message)


def _load_dataframe(path: str):
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        raise _err(0, (
            "Parquet is saved at %s. To get a DataFrame back directly, install the "
            "frames extra: pip install 'lse-data[frames]' (or pass dataframe=False "
            "to keep just the file)." % path))
    return pq.read_table(path).to_pandas()


class VaultMixin:
    """Vault methods mixed into :class:`lse.LSE`. All share the client's key."""

    # ── plumbing ─────────────────────────────────────────────────────────

    def _vault_call(self, path: str, payload: Optional[dict] = None) -> dict:
        url = f"{VAULT_URL}{path}"
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(url, data=data, headers={
            "x-api-key": self._api_key,
            "User-Agent": _USER_AGENT,
            **({"Content-Type": "application/json"} if data else {}),
        })
        try:
            with urllib.request.urlopen(req, timeout=self._rest_timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            raise _err(e.code, e.read().decode("utf-8", "replace")[:300])
        except OSError as e:
            # Wrap timeouts and transport failures like HTTP errors, so callers
            # only ever need to catch LSEError (URLError/TimeoutError are OSError).
            raise _err(0, "request failed before an HTTP response: "
                          f"{getattr(e, 'reason', None) or e}")

    # ── discovery ────────────────────────────────────────────────────────

    def vault_meta(self) -> dict:
        """The vault's shape: datasets, candle classes, timeframes, reference list."""
        cached = getattr(self, "_vault_meta_cache", None)
        if cached is None:
            cached = self._vault_call("/meta")
            self._vault_meta_cache = cached
        return cached

    def datasets(self, dataset: Optional[str] = None) -> List[dict]:
        """Every instrument the vault holds, with tick counts and history span.

        One row per (dataset, symbol): ``{dataset, symbol, ticks, first_tick,
        last_tick, years}``. Pass a dataset name ("stocks", "economics", ...) to
        filter to one class.
        """
        cached = getattr(self, "_vault_catalog_cache", None)
        if cached is None:
            cached = self._vault_call("/catalog")
            self._vault_catalog_cache = cached
        if dataset:
            return [r for r in cached if r["dataset"] == dataset]
        return cached

    def reference(self) -> List[dict]:
        """The reference datasets (whole-file downloads): rows and date span each."""
        return self._vault_call("/reference")

    # ── the pull ─────────────────────────────────────────────────────────

    def history(self, symbol: Optional[str] = None, *, dataset: Optional[str] = None,
                timeframe: str = "tick", start: Optional[str] = None,
                end: Optional[str] = None, dest: Optional[str] = None,
                dataframe: bool = True, poll_seconds: float = 1.5,
                timeout: float = 1800.0):
        """Pull history from the vault as Parquet.

        ``history("AAPL", timeframe="1m", start="2020-01-01")`` resolves the
        dataset from the catalog automatically; pass ``dataset=`` explicitly when
        a symbol exists in more than one class. ``timeframe`` is ``"tick"`` for
        the raw tape or any candle resolution from :meth:`vault_meta`.

        Returns a DataFrame (default, needs the ``frames`` extra) or the saved
        file path when ``dataframe=False``. The Parquet file is kept either way.
        """
        if not symbol and not dataset:
            raise _err(0, "pass a symbol, a dataset, or both")
        if symbol and not dataset:
            rows = [r for r in self.datasets() if r["symbol"] == symbol]
            found = sorted({r["dataset"] for r in rows})
            # Every optionable stock appears in BOTH stocks and options, so options
            # never wins auto-resolution: history("AAPL") means the stock tape, and
            # the option prints are an explicit history("AAPL", dataset="options").
            picks = [d for d in found if d != "options"] or found
            if not picks:
                raise _err(0, f"'{symbol}' is not in the vault catalog; "
                              f"browse client.datasets() or the /databank page")
            if len(picks) > 1:
                raise _err(0, f"'{symbol}' exists in {picks}; pass dataset= to pick one")
            dataset = picks[0]
        job = self._vault_call("/export", {
            "dataset": dataset, "symbol": symbol or "all", "timeframe": timeframe,
            "start": start, "end": end, "format": "parquet",
        })
        info = self._vault_wait(job["job_id"], poll_seconds, timeout)
        name = f"{dataset}_{(symbol or 'all').replace('/', '_')}_{timeframe}.parquet"
        path = self._vault_download(job["job_id"], name, dest, info)
        return _load_dataframe(path) if dataframe else path

    def dataset(self, name: str, *, start: Optional[str] = None,
                end: Optional[str] = None, dest: Optional[str] = None,
                dataframe: bool = True, poll_seconds: float = 1.5,
                timeout: float = 1800.0):
        """Download a whole reference dataset (insider_trades, dividends,
        bond_yields, economic_calendar, financial_reports, stock_splits, cot,
        company_profiles, stock_fundamentals) as one Parquet file, optionally
        limited to a date range on the dataset's own date column."""
        job = self._vault_call("/export", {
            "dataset": name, "start": start, "end": end, "format": "parquet",
        })
        info = self._vault_wait(job["job_id"], poll_seconds, timeout)
        path = self._vault_download(job["job_id"], f"{name}.parquet", dest, info)
        return _load_dataframe(path) if dataframe else path

    def economics(self, symbol: Optional[str] = None, start: Optional[str] = None,
                  end: Optional[str] = None, order: str = "asc", limit: int = 5000):
        """Macro economics series from the vault.

        Without a symbol, returns the catalog of series (one row each with its
        observation count and span). With one, returns that series' observations
        as (date, value) rows, instantly: ``client.economics("fdtr")``. Every
        series fits in one call. For a Parquet file instead, use
        ``history("fdtr", dataset="economics")``.
        """
        if symbol is None:
            return self.datasets("economics")
        return self.series(symbol, dataset="economics", start=start, end=end,
                           order=order, limit=limit)

    # ── job mechanics ────────────────────────────────────────────────────

    def _vault_wait(self, job_id: str, poll_seconds: float, timeout: float) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            info = self._vault_call(f"/export/{job_id}")
            status = info.get("status")
            if status == "ready":
                return info
            if status in ("failed", "expired"):
                raise _err(0, f"export {status}: {info.get('error') or ''}")
            if time.monotonic() > deadline:
                raise _err(0, f"export still {status} after {int(timeout)}s; "
                              f"job {job_id} keeps building, retry later")
            time.sleep(poll_seconds)

    def _vault_download(self, job_id: str, filename: str, dest: Optional[str],
                        info: dict) -> str:
        out = os.path.join(dest or ".", filename)
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        part = out + ".part"
        # Resume a previous interrupted pull of the same job when a .part exists.
        offset = os.path.getsize(part) if os.path.exists(part) else 0
        headers = {"x-api-key": self._api_key, "User-Agent": _USER_AGENT}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        req = urllib.request.Request(f"{VAULT_URL}/export/{job_id}/download",
                                     headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=max(self._rest_timeout, 300)) as resp:
                mode = "ab" if offset and resp.status == 206 else "wb"
                with open(part, mode) as f:
                    while True:
                        chunk = resp.read(1 << 20)
                        if not chunk:
                            break
                        f.write(chunk)
        except urllib.error.HTTPError as e:
            raise _err(e.code, e.read().decode("utf-8", "replace")[:300])
        except OSError as e:
            # A dropped transfer keeps the .part file, so the same call resumes;
            # surface the failure as LSEError like every other error here.
            raise _err(0, f"download interrupted ({getattr(e, 'reason', None) or e}); "
                          "run the same call again to resume")
        # Integrity: the job record carries the artifact's true size; a short file
        # here means a broken transfer, so keep the .part for resume and say so.
        want = int(info.get("bytes") or 0)
        got = os.path.getsize(part)
        if want and got != want:
            raise _err(0, f"download incomplete ({got}/{want} bytes); "
                          f"run the same call again to resume")
        os.replace(part, out)
        return out
