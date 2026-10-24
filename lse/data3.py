"""
LSE Data 3.0 over HTTPS: the catalogue, previews, estimates, resumable file
exports, macro and reference data, options chains and flow, all from
https://download.londonstrategicedge.com/v3 with the same key that streams.

Everything here is plain Python on the standard library: one key in the
``X-API-Key`` header, no redirects followed, no key in any URL or resume
file, and every finished file checked by size and SHA-256 before it is
placed. :class:`Data3Mixin` is mixed into :class:`lse.LSE`.
"""

import csv
import hashlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

DEFAULT_BASE_URL = "https://download.londonstrategicedge.com"
# The markets this release was written against. The server's own list wins:
# markets() reads it live, and a market added or removed there needs no
# client release.
MARKETS = ("us_stocks", "uk_stocks", "ca_stocks", "forex", "crypto", "futures",
           "indices", "commodities", "us_options", "cme", "nasdaq")
PAID_FEEDS = frozenset(("cme", "nasdaq"))
TIMEFRAMES = ("tick", "1s", "5s", "15s", "30s", "1m", "3m", "5m", "15m", "30m",
              "1h", "4h", "1d", "1w", "1mo")
DATA_KINDS = ("candles", "trades", "quotes", "orders", "book", "greeks")
MACRO_KINDS = ("observation", "yield", "release")
_RETRY_STATUSES = frozenset((429, 500, 502, 503, 504))
_USER_AGENT = "lse-data/3.0 (+https://londonstrategicedge.com)"
# Row reads (candles(), the reference readers) hold one export in memory.
# Anything larger belongs in history() or export(), which write a file.
_ROWS_MAX_BYTES = 256 << 20
_CHUNK = 1 << 20


class LSEError(Exception):
    """A failed call: the HTTP status, the server's message and its code.

    ``status`` is 0 when no HTTP answer arrived (a timeout, a connection
    failure, a file that failed its checksum). ``code`` is the server's
    refusal code, such as ``plan-required`` or ``history-limit``, and
    ``required_plan`` names the cheapest plan that opens a paid feed.
    """

    def __init__(self, status: int, message: str, code: str = "request-failed",
                 required_plan: Optional[dict] = None):
        self.status = status
        self.message = message
        self.code = code
        self.required_plan = required_plan
        super().__init__(f"[{status}] {message}")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    # A download address is accepted only on the server's own origin; a
    # redirect anywhere else is refused rather than followed with the key.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def _error(status: int, body: bytes) -> LSEError:
    try:
        value = json.loads(bytes(body).decode("utf-8", "replace"))
    except ValueError:
        value = None
    if isinstance(value, dict):
        message = value.get("message") or value.get("detail") or "Request failed"
        return LSEError(status, str(message), str(value.get("code") or "request-failed"),
                        value.get("required_plan"))
    text = bytes(body).decode("utf-8", "replace").strip()[:300]
    return LSEError(status, text or "the server returned an unexpected response", "invalid-response")


def _sha256(path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(_CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def _market(market) -> str:
    if not isinstance(market, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", market):
        raise ValueError("market must be a market code such as us_stocks; markets() lists them")
    return market


def _sort(sort) -> str:
    if sort not in ("symbol", "popular"):
        raise ValueError("sort must be popular or symbol")
    return sort


def _number(text: str):
    """A CSV cell as a number when it is one, None when it is empty or \\N."""
    if text is None or text == "" or text == "\\N":
        return None
    try:
        if re.fullmatch(r"-?\d+", text):
            return int(text)
        return float(text)
    except ValueError:
        return text


def load(path):
    """The delivered CSV or Parquet as a pandas DataFrame (``pip install 'lse-data[frames]'``)."""
    path = str(path)
    try:
        import pandas as pd  # type: ignore
    except ImportError:
        raise LSEError(0, (
            f"The file is saved at {path}. For a DataFrame, install the frames extra: "
            "pip install 'lse-data[frames]' (or pass dataframe=False to keep the file)."),
            "frames-missing")
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    import pyarrow.parquet as pq  # type: ignore
    return pq.read_table(path).to_pandas()


class Data3Mixin:
    """The LSE Data 3.0 calls. Expects ``self._api_key``, ``self.base_url``,
    ``self.retries``, ``self.poll_seconds`` and
    ``self._rest_timeout`` on the instance."""

    # ── plumbing ─────────────────────────────────────────────────────────

    def _data_url(self, path: str, **params) -> str:
        url = urllib.parse.urljoin(self.base_url + "/", path)
        base, target = urllib.parse.urlsplit(self.base_url), urllib.parse.urlsplit(url)
        if ((base.scheme, base.hostname, base.port) != (target.scheme, target.hostname, target.port)
                or target.username or target.password):
            raise LSEError(0, "the server gave a download address outside its own origin", "invalid-origin")
        if any(k.lower() in ("key", "api_key", "x-api-key") for k, _ in urllib.parse.parse_qsl(target.query)):
            raise LSEError(0, "keys travel in the request header, never in an address", "invalid-address")
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return url + (("&" if target.query else "?") + query if query else "")

    def _wait_out(self, retry_after) -> bool:
        """Whether a 429 is waited out: once, for the server's Retry-After (at
        most two minutes), when the client was made with wait_for_limit."""
        if not getattr(self, "wait_for_limit", True) or getattr(self, "_waited", False):
            self._waited = False
            return False
        try:
            seconds = float(retry_after or 0)
        except ValueError:
            seconds = 0
        if not 0 < seconds <= 120:
            return False
        self._waited = True
        time.sleep(seconds + 0.5)
        return True

    def _headers(self, extra: Optional[dict] = None) -> dict:
        h = {"X-API-Key": self._api_key, "User-Agent": _USER_AGENT, "Accept": "application/json, text/csv, */*"}
        if extra:
            h.update(extra)
        return h

    def _open(self, url: str, headers: Optional[dict] = None, timeout: Optional[float] = None):
        """One GET with the key; returns the open response, or raises HTTPError for a refusal."""
        req = urllib.request.Request(url, headers=self._headers(headers), method="GET")
        return _OPENER.open(req, timeout=timeout or self._rest_timeout)

    def _json(self, path: str, **params) -> Any:
        url = self._data_url(path, **params)
        for attempt in range(self.retries + 1):
            try:
                with self._open(url) as resp:
                    status, body = resp.status, resp.read()
            except urllib.error.HTTPError as e:
                status, body = e.code, e.read()
                if status == 429 and self._wait_out(e.headers.get("Retry-After")):
                    continue
                if status in _RETRY_STATUSES and attempt < self.retries:
                    time.sleep(min(0.25 * 2 ** attempt, 2))
                    continue
                raise _error(status, body)
            except (urllib.error.URLError, OSError) as e:
                if attempt == self.retries:
                    raise LSEError(0, f"request failed before an HTTP response: {getattr(e, 'reason', None) or e}",
                                   "no-response")
                time.sleep(min(0.25 * 2 ** attempt, 2))
                continue
            if status != 200:
                raise _error(status, body)
            try:
                return json.loads(body.decode("utf-8"))
            except ValueError:
                raise LSEError(status, "expected a JSON answer from the server", "invalid-response") from None

    # ── service ──────────────────────────────────────────────────────────

    def health(self) -> dict:
        """Is the data server up: its version and how many markets it serves."""
        return self._json("/v3/health")

    def usage(self) -> dict:
        """What this key may do and has used: plan, markets, calls a minute,
        pacing, downloads at once, history per level and the weekly allowance."""
        return self._json("/v3/usage")

    # ── catalogue ────────────────────────────────────────────────────────

    def markets(self) -> List[dict]:
        """The markets served now, as the server lists them, with what this key may open."""
        rows = self._json("/v3/markets").get("markets")
        if not isinstance(rows, list) or not rows or not all(isinstance(r, dict) and r.get("folder") for r in rows):
            raise LSEError(0, "the server returned no readable market list", "invalid-catalogue")
        self._market_rows = {row["folder"]: row for row in rows}
        return rows

    def _offers(self, market: str, data: str) -> bool:
        if getattr(self, "_market_rows", None) is None:
            try:
                self.markets()
            except LSEError:
                return market in PAID_FEEDS
        capabilities = (self._market_rows.get(market) or {}).get("capabilities")
        if isinstance(capabilities, dict) and isinstance(capabilities.get(data), bool):
            return capabilities[data]
        return market in PAID_FEEDS

    def overview(self, market: Optional[str] = None, symbol: Optional[str] = None, pair_id=None) -> dict:
        """The country overview and featured archives; with a market and a
        symbol, that instrument's stored one-minute history bounds."""
        if (symbol is not None or pair_id is not None) and market is None:
            raise ValueError("Choose a market when requesting an instrument's history")
        return self._json("/v3/overview", market=_market(market) if market is not None else None,
                          symbol=symbol, pair_id=pair_id)

    def catalogue(self, market: Optional[str] = None, q: Optional[str] = None, limit: int = 500,
                  offset: int = 0, *, sort: str = "symbol", largest: Optional[int] = None) -> dict:
        """What the archive holds: every market's size, one market's symbols
        with their stored rows, or with ``largest`` the biggest archives."""
        return self._json("/v3/catalogue", market=_market(market) if market else None, q=q, limit=limit,
                          offset=offset, sort=_sort(sort), largest=largest)

    def pair_page(self, market: str, symbol: Optional[str] = None, q: Optional[str] = None, limit: int = 10000,
                  offset: int = 0, cursor: Optional[str] = None, *, sort: str = "symbol", **filters) -> dict:
        """One page of a market's instruments. ``filters`` are passed through:
        ``underlying``, ``group_by``, ``option_type``, ``expiry``, ``product``."""
        return self._json("/v3/pairs", market=_market(market), symbol=symbol, q=q, limit=limit,
                          offset=offset if cursor is None else None, cursor=cursor, sort=_sort(sort), **filters)

    def iter_pairs(self, market: str, symbol: Optional[str] = None, q: Optional[str] = None,
                   page_size: int = 10000, *, sort: str = "symbol", **filters) -> Iterator[dict]:
        """Every instrument of a market, page by page, by offset or by cursor."""
        sort = _sort(sort)
        if not 1 <= page_size <= 10000:
            raise ValueError("page_size must be between 1 and 10000")
        offset, cursor = 0, None
        while True:
            page = self.pair_page(market, symbol, q, page_size, offset, cursor, sort=sort, **filters)
            rows = page.get("pairs") or page.get("underlyings") or []
            yield from rows
            offset += len(rows)
            next_cursor = page.get("next_cursor")
            if cursor is not None or "next_cursor" in page:
                # A contract catalogue pages by cursor; only its exhaustion ends it.
                if page.get("has_more") is False:
                    return
                if next_cursor is None:
                    if page.get("has_more") is True:
                        raise LSEError(0, "the server reported another page without a cursor", "incomplete-catalogue")
                    return
                if not rows or next_cursor == cursor:
                    raise LSEError(0, "the server returned a catalogue cursor without advancing", "incomplete-catalogue")
                cursor = next_cursor
            elif "has_more" in page:
                # An underlying listing pages by offset and says whether more follow.
                if not page["has_more"]:
                    return
                if not rows:
                    raise LSEError(0, "the server returned an empty page before the catalogue ended", "incomplete-catalogue")
            else:
                if offset >= int(page.get("total") or 0):
                    return
                if not rows:
                    raise LSEError(0, "the server returned an empty page before the catalogue ended", "incomplete-catalogue")

    def pairs(self, market: str, symbol: Optional[str] = None, q: Optional[str] = None, *, sort: str = "symbol",
              **filters) -> List[dict]:
        """Every instrument of a market as a list (iter_pairs() pages through them)."""
        return list(self.iter_pairs(market, symbol, q, sort=sort, **filters))

    def tables(self, market: str) -> List[dict]:
        """The archive tables of one market and their sizes, for any key."""
        return self._json("/v3/tables", market=_market(market))["tables"]

    def factors(self, market: str, symbol: Optional[str] = None, pair_id=None) -> dict:
        """A pair's splits, and for a stock its dividends."""
        return self._json("/v3/factors", market=_market(market), symbol=symbol, pair_id=pair_id)

    # ── options ──────────────────────────────────────────────────────────

    def options_chain(self, underlying: str, *, type: Optional[str] = None, expiry: Optional[str] = None,
                      strike=None, strike_min=None, strike_max=None, min_dte=None, max_dte=None,
                      limit: int = 1000, offset: int = 0) -> dict:
        """The listed, unexpired contracts of one underlying with their last
        trade, today's volume and premium and the newest Greeks, as a page:
        ``rows`` plus ``contracts``, ``underlying_price`` and ``greeks_as_of``."""
        if type is not None and type not in ("call", "put"):
            raise ValueError("type is call or put")
        return self._json("/v3/options/chain", underlying=underlying, type=type, expiry=expiry, strike=strike,
                          strike_min=strike_min, strike_max=strike_max, min_dte=min_dte, max_dte=max_dte,
                          limit=limit, offset=offset)

    def options_flow_page(self, underlying: Optional[str] = None, *, type: Optional[str] = None,
                          min_premium=None, expiry: Optional[str] = None, min_dte=None, max_dte=None,
                          start: Optional[str] = None, end: Optional[str] = None, order: str = "desc",
                          limit: int = 1000, offset: int = 0) -> dict:
        """Prints of the options tape, newest first, as a page with its window
        (today by default). Without an underlying the call sweeps the whole
        tape over at most seven days."""
        if type is not None and type not in ("call", "put"):
            raise ValueError("type is call or put")
        if order not in ("asc", "desc"):
            raise ValueError("order is asc or desc")
        return self._json("/v3/options/flow", underlying=underlying, type=type, min_premium=min_premium,
                          expiry=expiry, min_dte=min_dte, max_dte=max_dte, order=order, limit=limit, offset=offset,
                          **{"from": start, "to": end})

    # ── market data ──────────────────────────────────────────────────────

    def _params(self, market, symbol, timeframe, start, end, format, adjusted, data, pair_id=None) -> dict:
        _market(market)
        if timeframe not in TIMEFRAMES:
            raise ValueError("Unsupported timeframe; TIMEFRAMES lists them")
        if format not in ("csv", "parquet"):
            raise ValueError("format must be csv or parquet")
        if data not in DATA_KINDS:
            raise ValueError("data must be candles, trades, quotes, orders, book or greeks")
        if data != "candles" and not self._offers(market, data):
            if data == "greeks":
                raise ValueError("Greeks are computed for US options only (market us_options)")
            if data == "book":
                raise ValueError("The Level 2 book is served on the feeds whose capabilities list book; see markets()")
            if data == "orders":
                raise ValueError("Level 3 orders are served on the feeds whose capabilities list orders; see markets()")
            raise ValueError("Trades and Quotes are available on the paid feeds; use timeframe='tick' for a trade slice")
        if not symbol and pair_id is None:
            raise ValueError("Pass symbol or pair_id")
        return {"market": market, "symbol": symbol, "pair_id": pair_id, "timeframe": timeframe,
                "from": start, "to": end, "format": format, "adjusted": "1" if adjusted else "0", "data": data}

    def preview(self, market: str, symbol: Optional[str], timeframe: str = "1m", start: Optional[str] = None,
                end: Optional[str] = None, *, adjusted: bool = True, data: str = "candles", pair_id=None) -> dict:
        """The last ten rows of a selection, or the rows of a short window."""
        return self._json("/v3/preview", **self._params(market, symbol, timeframe, start, end, "csv", adjusted, data, pair_id))

    def estimate(self, market: str, symbol: Optional[str], timeframe: str, start: Optional[str], end: Optional[str],
                 *, format: str = "parquet", adjusted: bool = True, data: str = "candles", pair_id=None) -> dict:
        """The rows and bytes an export would have, and whether it streams or is prepared."""
        return self._json("/v3/estimate", **self._params(market, symbol, timeframe, start, end, format, adjusted, data, pair_id))

    def job(self, job_id: str) -> dict:
        """A prepared export's state."""
        if not re.fullmatch(r"[0-9a-f]{32}", str(job_id)):
            raise ValueError("Invalid export job id")
        return self._json(f"/v3/jobs/{job_id}")

    @staticmethod
    def _save_state(path, value) -> None:
        temp = Path(str(path) + ".tmp")
        temp.write_text(json.dumps(value), encoding="utf-8")
        temp.chmod(0o600)
        os.replace(temp, path)

    def _wait_job(self, job_id: str, deadline: float, progress) -> dict:
        while time.monotonic() < deadline:
            info = self.job(job_id)
            status = info.get("status")
            if progress:
                progress({"phase": status, "bytes": info.get("bytes", 0), "job_id": job_id})
            if status == "ready":
                if not re.fullmatch(r"[0-9a-fA-F]{64}", str(info.get("checksum", ""))):
                    raise LSEError(0, "the finished export has no valid SHA-256 checksum", "checksum-missing")
                return info
            if status in ("failed", "expired"):
                raise LSEError(410 if status == "expired" else 500, str(info.get("error") or "Export " + status),
                               "export-" + status)
            if status not in ("building", "queued"):
                raise LSEError(0, "the server returned an unknown export state", "invalid-job")
            time.sleep(min(self.poll_seconds, max(0.0, deadline - time.monotonic())))
        raise LSEError(0, "the export is still building; repeat the same call to continue with its saved job", "timeout")

    def _saved_job(self, state_path, state, deadline, progress) -> dict:
        try:
            return self._wait_job(state["job_id"], deadline, progress)
        except LSEError as e:
            if e.code not in ("export-failed", "export-expired", "export-unknown"):
                raise
            state.pop("job_id", None)
            self._save_state(state_path, state)
            if e.code == "export-failed" and "no stored data" in str(e.message):
                raise LSEError(404, e.message, "data-unavailable") from None
            raise

    def _fetch_export(self, url: str, dest: str, offset: int, etag: Optional[str], progress, received,
                      timeout: float) -> Tuple[int, dict, bytes, int]:
        """One transfer into the partial file. A refusal or a job answer never
        touches the file; a resume appends only a valid 206 for the requested
        byte; a 200 starts the file again."""
        headers = {}
        if offset > 0:
            headers["Range"] = f"bytes={offset}-"
            if etag:
                headers["If-Range"] = etag
        try:
            resp = self._open(url, headers, timeout=max(1.0, min(timeout, 3600.0)))
        except urllib.error.HTTPError as e:
            body = e.read(65536)
            return e.code, {k.lower(): v for k, v in e.headers.items()}, body, 0
        with resp:
            status = resp.status
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            if received:
                received(status, hdrs)
            if status not in (200, 206) or hdrs.get("content-type", "").startswith("application/json"):
                return status, hdrs, resp.read(65536), 0
            append = status == 206
            if append:
                if not hdrs.get("content-range", "").startswith(f"bytes {offset}-"):
                    raise LSEError(0, "the resume answer has an invalid Content-Range", "resume-invalid")
                if etag and hdrs.get("etag") != etag:
                    raise LSEError(0, "the resume answer changed its ETag", "resume-invalid")
                actual = os.path.getsize(dest) if os.path.exists(dest) else 0
                if actual != offset:
                    raise LSEError(0, "the partial file changed size before the resume", "resume-invalid")
            parent = os.path.dirname(dest)
            if parent:
                os.makedirs(parent, exist_ok=True)
            total = None
            if "content-range" in hdrs:
                try:
                    total = int(hdrs["content-range"].rsplit("/", 1)[1])
                except (ValueError, IndexError):
                    total = None
            elif hdrs.get("content-length", "").isdigit():
                total = int(hdrs["content-length"]) + (offset if append else 0)
            delivered = offset if append else 0
            with open(dest, "ab" if append else "wb") as f:
                while True:
                    chunk = resp.read(_CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
                    delivered += len(chunk)
                    if progress:
                        progress(delivered, total)
            if total is not None and total != delivered:
                # A cut transfer: the retry loop resumes it from the bytes written.
                raise ConnectionError("the download ended before its stated length")
            return status, hdrs, b"", delivered

    def _export(self, export_path: str, params: dict, filename: str, dest, progress, timeout: float) -> str:
        """The shared file protocol: key-owned jobs, persistence, ranges and checks."""
        if timeout <= 0:
            raise ValueError("Export needs a positive timeout")
        target = Path(dest) if dest is not None else Path(filename)
        if target.is_dir():
            target = target / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        part, state_path = Path(str(target) + ".part"), Path(str(target) + ".part.json")
        identity = [self.base_url, params] if export_path == "/v3/export" else [self.base_url, export_path, params]
        signature = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        state: Dict[str, Any] = {"signature": signature}
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (ValueError, OSError):
                raise LSEError(0, "the resume file is unreadable; choose a different destination", "invalid-resume") from None
            if state.get("signature") != signature:
                raise LSEError(0, "this destination holds a partial download of a different request", "resume-conflict")
        self._save_state(state_path, state)
        deadline = time.monotonic() + timeout
        checksum, expected, job_info = None, None, None

        def received(status, headers):
            if status in (200, 206) and headers.get("x-export-job"):
                state["job_id"] = headers["x-export-job"]
                self._save_state(state_path, state)

        def transferred(count, total):
            if progress:
                progress({"phase": "downloading", "bytes": count, "total_bytes": total})

        for attempt in range(self.retries + 1):
            if time.monotonic() >= deadline:
                raise LSEError(0, "the download timed out; repeat the same call to resume", "timeout")
            if state.get("job_id"):
                job_info = self._saved_job(state_path, state, deadline, progress)
                checksum, expected = job_info["checksum"].lower(), int(job_info["bytes"])
                url = self._data_url(job_info.get("download_url") or f"/v3/jobs/{state['job_id']}/file")
                offset = part.stat().st_size if part.exists() else 0
                if part.exists() and offset == expected:
                    if checksum == _sha256(part):
                        break
                    part.rename(Path(str(part) + ".checksum-failed"))
                    offset = 0
                if offset > expected:
                    raise LSEError(0, "the partial download is larger than the finished export", "resume-size")
                etag = '"' + checksum + '"'
            else:
                url, offset, etag = self._data_url(export_path, **params), 0, None
            try:
                status, headers, body, written = self._fetch_export(
                    url, str(part), offset, etag, transferred, received, timeout=max(0.1, deadline - time.monotonic()))
            except (urllib.error.URLError, OSError) as e:
                if attempt == self.retries:
                    raise LSEError(0, f"download interrupted ({getattr(e, 'reason', None) or e}); "
                                      "run the same call again to resume", "no-response")
                time.sleep(min(0.25 * 2 ** attempt, 2))
                continue
            if status == 202:
                try:
                    state["job_id"] = json.loads(body.decode("utf-8"))["job_id"]
                except (ValueError, KeyError, UnicodeDecodeError):
                    raise LSEError(202, "the server did not identify the export job", "invalid-job") from None
                self._save_state(state_path, state)
                return self._export(export_path, params, filename, target, progress, max(0.001, deadline - time.monotonic()))
            if status not in (200, 206):
                if status == 429 and self._wait_out(headers.get("retry-after")):
                    continue
                if status in _RETRY_STATUSES and attempt < self.retries:
                    time.sleep(min(0.25 * 2 ** attempt, 2))
                    continue
                if not state.get("job_id") and not part.exists():
                    state_path.unlink()  # a refusal before any byte leaves nothing to resume
                raise _error(status, body)
            checksum = checksum or headers.get("x-checksum-sha256")
            if state.get("job_id") and not job_info:
                job_info = self._saved_job(state_path, state, deadline, progress)
                checksum, expected = job_info["checksum"].lower(), int(job_info["bytes"])
            break
        if not part.exists():
            raise LSEError(0, "the server completed the request without a file", "download-missing")
        if expected is not None and part.stat().st_size != expected:
            raise LSEError(0, "the downloaded size does not match the finished export", "size-mismatch")
        if not checksum:
            raise LSEError(0, "the server did not supply an export checksum", "checksum-missing")
        if _sha256(part) != checksum.lower():
            part.rename(Path(str(part) + ".checksum-failed"))
            raise LSEError(0, "the downloaded file failed its SHA-256 check; repeat the call to download again", "checksum-mismatch")
        os.replace(part, target)
        state_path.unlink()
        if progress:
            progress({"phase": "complete", "bytes": target.stat().st_size, "path": str(target)})
        return str(target)

    def export(self, market: str, symbol: Optional[str], timeframe: str, start: str, end: str, *,
               format: str = "parquet", adjusted: bool = True, data: str = "candles", pair_id=None,
               dest=None, progress: Optional[Callable] = None, timeout: float = 1800) -> str:
        """Save a selection as CSV or Parquet and return the file's path.

        A small export streams at once; a large one is prepared as a job and
        polled. An interrupted call keeps ``.part`` and ``.part.json`` beside
        the destination, and repeating the same call resumes it. The file is
        placed only after its size and SHA-256 match what the server states.
        """
        if not start or not end or timeout <= 0:
            raise ValueError("Export needs start, end, and a positive timeout")
        params = self._params(market, symbol, timeframe, start, end, format, adjusted, data, pair_id)
        label = re.sub(r"[^A-Za-z0-9._-]", "_", str(symbol or pair_id))
        resolution = timeframe if data == "candles" else f"{timeframe}_{data}"
        filename = f"{label}_{market}_{resolution}_{start[:10]}_{end[:10]}_{'adjusted' if adjusted else 'published'}.{format}"
        return self._export("/v3/export", params, filename, dest, progress, timeout)

    download = export

    def _fetch_rows_bytes(self, path: str, params: dict, timeout: float = 600.0) -> bytes:
        """A CSV export held in memory: streamed at once, or prepared and then fetched."""
        url = self._data_url(path, **params)
        deadline = time.monotonic() + timeout
        for attempt in range(self.retries + 1):
            try:
                resp = self._open(url, timeout=max(1.0, min(timeout, 3600.0)))
            except urllib.error.HTTPError as e:
                body = e.read(65536)
                if e.code == 429 and self._wait_out(e.headers.get("Retry-After")):
                    continue
                if e.code in _RETRY_STATUSES and attempt < self.retries:
                    time.sleep(min(0.25 * 2 ** attempt, 2))
                    continue
                raise _error(e.code, body)
            except (urllib.error.URLError, OSError) as e:
                if attempt == self.retries:
                    raise LSEError(0, f"request failed before an HTTP response: {getattr(e, 'reason', None) or e}", "no-response")
                time.sleep(min(0.25 * 2 ** attempt, 2))
                continue
            with resp:
                if resp.status == 202:
                    job = json.loads(resp.read(65536).decode("utf-8"))
                    info = self._wait_job(job["job_id"], deadline, None)
                    return self._fetch_file_bytes(self._data_url(info.get("download_url") or f"/v3/jobs/{job['job_id']}/file"),
                                                  int(info["bytes"]), info["checksum"].lower(), deadline)
                return self._read_capped(resp)
        raise LSEError(0, "the request could not be completed", "no-response")

    def _read_capped(self, resp) -> bytes:
        buf = io.BytesIO()
        while True:
            chunk = resp.read(_CHUNK)
            if not chunk:
                break
            buf.write(chunk)
            if buf.tell() > _ROWS_MAX_BYTES:
                raise LSEError(0, "this selection is larger than a row read holds in memory; "
                                  "use history() or export() to save it as a file", "too-large")
        return buf.getvalue()

    def _fetch_file_bytes(self, url: str, expected: int, checksum: str, deadline: float) -> bytes:
        if expected > _ROWS_MAX_BYTES:
            raise LSEError(0, "this selection is larger than a row read holds in memory; "
                              "use history() or export() to save it as a file", "too-large")
        with self._open(url, timeout=max(1.0, deadline - time.monotonic())) as resp:
            data = self._read_capped(resp)
        if len(data) != expected or hashlib.sha256(data).hexdigest() != checksum:
            raise LSEError(0, "the downloaded rows failed their size or SHA-256 check; call again", "checksum-mismatch")
        return data

    def _csv_rows(self, data: bytes) -> List[dict]:
        text = data.decode("utf-8", "replace")
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for raw in reader:
            rows.append({k: _number(v) if k not in ("time", "ts", "date", "day", "released_at", "price_at", "symbol",
                                                      "kind", "action", "side", "price_source", "order_id")
                         else (None if v in ("", "\\N") else v)
                         for k, v in raw.items()})
        return rows

    def rows(self, market: str, symbol: Optional[str], timeframe: str, start: str, end: str, *,
             adjusted: bool = True, data: str = "candles", pair_id=None) -> List[dict]:
        """A selection as a list of dicts, held in memory (at most 256 MB);
        use export() for anything larger."""
        params = self._params(market, symbol, timeframe, start, end, "csv", adjusted, data, pair_id)
        return self._csv_rows(self._fetch_rows_bytes("/v3/export", params))

    # ── dataset builder ──────────────────────────────────────────────────

    def dataset_export(self, market: str, symbol: Optional[str], timeframe: str, start: str, end: str, *,
                       indicators=(), companion: Optional[dict] = None, companion_features=(), corr_window: int = 60,
                       hour_start: Optional[int] = None, hour_end: Optional[int] = None, timezone: str = "UTC",
                       days=(), adjusted: bool = True, format: str = "csv", pair_id=None, dest=None,
                       progress: Optional[Callable] = None, timeout: float = 1800) -> str:
        """Candles with indicators, a companion instrument's columns and
        local-time filters, computed on the server and saved as a file.
        A companion is ``{"market": "us_stocks", "symbol": "MSFT"}``."""
        if timeframe == "tick" or not start or not end:
            raise ValueError("Datasets need a candle timeframe, start and end")
        params = self._params(market, symbol, timeframe, start, end, format, adjusted, "candles", pair_id)
        params.pop("data")
        if isinstance(indicators, str):
            indicators = indicators.split(",") if indicators else []
        indicators = list(indicators)
        if any(not isinstance(v, str) or not re.fullmatch(r"[a-z_]+(?::[0-9]+)?", v) for v in indicators):
            raise ValueError("Indicators use names and optional periods, for example rsi:14")
        if indicators:
            params["indicators"] = ",".join(indicators)
        if isinstance(companion_features, str):
            companion_features = companion_features.split(",") if companion_features else []
        companion_features = list(companion_features)
        if any(v not in ("close", "return", "corr") for v in companion_features):
            raise ValueError("Companion features are close, return and corr")
        if companion is not None:
            if not isinstance(companion, dict) or not companion_features:
                raise ValueError("A companion needs its market, symbol or pair_id, and selected features")
            params["companion_market"] = _market(companion.get("market"))
            if companion.get("pair_id") is not None:
                params["companion_pair_id"] = companion["pair_id"]
            elif companion.get("symbol"):
                params["companion_symbol"] = companion["symbol"]
            else:
                raise ValueError("A companion needs a symbol or pair_id")
            params["companion_features"] = ",".join(companion_features)
            if "corr" in companion_features:
                params["corr_window"] = corr_window
        elif companion_features:
            raise ValueError("Companion features need a companion")
        if (hour_start is None) != (hour_end is None):
            raise ValueError("Supply both hour_start and hour_end")
        if hour_start is not None:
            if any(not isinstance(h, int) or not 0 <= h <= 23 for h in (hour_start, hour_end)):
                raise ValueError("Hours must be between 0 and 23")
            params.update(hour_start=hour_start, hour_end=hour_end)
        if isinstance(days, str):
            days = days.split(",") if days else []
        days = list(days)
        if any(d not in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday") for d in days):
            raise ValueError("Use lowercase weekday names")
        if days:
            params["day_of_week"] = ",".join(days)
        params["tz"] = timezone
        label = re.sub(r"[^A-Za-z0-9._-]", "_", str(symbol or pair_id))
        filename = f"{label}_{market}_{timeframe}_{start[:10]}_{end[:10]}_dataset.{format}"
        return self._export("/v3/dataset/export", params, filename, dest, progress, timeout)

    # ── macro ────────────────────────────────────────────────────────────

    def macro_catalogue(self, kind: Optional[str] = None, *, country: Optional[str] = None, code: Optional[str] = None,
                        q: Optional[str] = None, group: Optional[str] = None, limit: int = 100, offset: int = 0) -> dict:
        """The macro catalogue: kinds and countries, or a page of series of one kind."""
        if group is not None and (group != "credit" or kind not in (None, "observation")):
            raise ValueError("group=credit is available for economic observations")
        if kind is not None and kind not in MACRO_KINDS:
            raise ValueError("kind must be observation, yield or release")
        if kind is None and any(value is not None for value in (country, code, q)):
            raise ValueError("Choose a macro kind when filtering the catalogue")
        if country is not None and not re.fullmatch(r"[A-Z]{2}", country):
            raise ValueError("country must be a two-letter uppercase code")
        if not isinstance(limit, int) or not 0 <= limit <= 1000 or not isinstance(offset, int) or offset < 0:
            raise ValueError("limit must be between 0 and 1000; offset must be non-negative")
        return self._json("/v3/macro", kind=kind, country=country, code=code, q=q, group=group, limit=limit, offset=offset)

    def iter_macro_series(self, kind: str = "observation", *, country: Optional[str] = None, q: Optional[str] = None,
                          group: Optional[str] = None, page_size: int = 1000) -> Iterator[dict]:
        """Every series of one macro kind, page by page."""
        if not isinstance(page_size, int) or not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        offset = 0
        while True:
            page = self.macro_catalogue(kind, country=country, q=q, group=group, limit=page_size, offset=offset)
            rows = page["series"]
            yield from rows
            offset += len(rows)
            if offset >= int(page["total"]):
                return
            if not rows:
                raise LSEError(0, "the server returned an empty macro page before the catalogue ended", "incomplete-catalogue")

    @staticmethod
    def _macro_params(kind, code, start, end, format) -> dict:
        from datetime import date
        if kind not in MACRO_KINDS:
            raise ValueError("kind must be observation, yield or release")
        if not isinstance(code, str) or not code.strip() or len(code.strip()) > 128:
            raise ValueError("Choose a macro series code from the catalogue")
        if format not in ("csv", "parquet"):
            raise ValueError("format must be csv or parquet")
        for value in (start, end):
            if value is not None:
                if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise ValueError("Macro dates use YYYY-MM-DD")
                date.fromisoformat(value)
        if start is not None and end is not None and start > end:
            raise ValueError("start must be on or before end")
        return {"kind": kind, "code": code.strip(), "from": start, "to": end, "format": format}

    def macro_preview(self, kind: str, code: str, start: Optional[str] = None, end: Optional[str] = None) -> dict:
        """The last ten rows of a macro series, in time order."""
        return self._json("/v3/macro/preview", **self._macro_params(kind, code, start, end, "csv"))

    def macro_series(self, kind: str, code: str, start: Optional[str] = None, end: Optional[str] = None) -> dict:
        """Every row of a series in a window, as JSON; above 100,000 rows use macro_export()."""
        return self._json("/v3/macro/series", **self._macro_params(kind, code, start, end, "csv"))

    def macro_estimate(self, kind: str, code: str, start: Optional[str] = None, end: Optional[str] = None, *,
                       format: str = "parquet") -> dict:
        """The rows and bytes a macro export would have; the bytes are an upper bound."""
        return self._json("/v3/macro/estimate", **self._macro_params(kind, code, start, end, format))

    def macro_export(self, kind: str, code: str, start: str, end: str, *, format: str = "parquet", dest=None,
                     progress: Optional[Callable] = None, timeout: float = 1800) -> str:
        """Save a macro series as CSV or Parquet, resumable and checked like export()."""
        if not start or not end:
            raise ValueError("Export needs start and end")
        params = self._macro_params(kind, code, start, end, format)
        label = re.sub(r"[^A-Za-z0-9._-]", "_", params["code"])
        filename = f"{label}_macro_{kind}_{start}_{end}.{format}"
        return self._export("/v3/macro/export", params, filename, dest, progress, timeout)

    def macro_chart_export(self, codes, start: str, end: str, *, mode: str = "level", format: str = "csv", dest=None,
                           progress: Optional[Callable] = None, timeout: float = 1800) -> str:
        """One to six observation series on their own dates, as level, yoy or mom."""
        if isinstance(codes, str):
            codes = [codes]
        codes = list(codes)
        if not 1 <= len(codes) <= 6 or len(set(codes)) != len(codes):
            raise ValueError("Choose one to six distinct series codes")
        if not start or not end or mode not in ("level", "yoy", "mom"):
            raise ValueError("Choose start, end and level, yoy or mom")
        for code in codes:
            self._macro_params("observation", code, start, end, format)
            if "," in code:
                raise ValueError("A series code cannot contain a comma")
        params = {"codes": ",".join(codes), "from": start, "to": end, "mode": mode, "format": format}
        label = hashlib.sha256(params["codes"].encode()).hexdigest()[:12]
        filename = f"macro_chart_{mode}_{label}_{start}_{end}.{format}"
        return self._export("/v3/macro/chart/export", params, filename, dest, progress, timeout)

    # ── reference ────────────────────────────────────────────────────────

    def reference_catalogue(self, dataset: Optional[str] = None, *, symbol: Optional[str] = None,
                            country: Optional[str] = None, q: Optional[str] = None, limit: int = 100,
                            offset: int = 0) -> dict:
        """The research and reference collections, or one collection's series."""
        if dataset is not None and (not isinstance(dataset, str) or not re.fullmatch(r"[a-z_]+", dataset)):
            raise ValueError("Choose a dataset from reference_catalogue()")
        if dataset is None and any(v is not None for v in (symbol, country, q)):
            raise ValueError("Choose a dataset before filtering its series")
        if not isinstance(limit, int) or not 0 <= limit <= 1000 or not isinstance(offset, int) or offset < 0:
            raise ValueError("limit must be between 0 and 1000; offset must be non-negative")
        return self._json("/v3/reference", dataset=dataset, symbol=symbol, country=country, q=q, limit=limit, offset=offset)

    def iter_reference_series(self, dataset: str, *, country: Optional[str] = None, q: Optional[str] = None,
                              page_size: int = 1000) -> Iterator[dict]:
        """Every series of one reference collection, page by page."""
        if not isinstance(page_size, int) or not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        offset = 0
        while True:
            page = self.reference_catalogue(dataset, country=country, q=q, limit=page_size, offset=offset)
            rows = page["series"]
            yield from rows
            offset += len(rows)
            if offset >= int(page["total"]):
                return
            if not rows:
                raise LSEError(0, "the server returned an empty reference page before the catalogue ended", "incomplete-catalogue")

    @staticmethod
    def _reference_params(dataset, symbol, start, end, format, country=None, q=None) -> dict:
        from datetime import date
        if not isinstance(dataset, str) or not re.fullmatch(r"[a-z_]+", dataset):
            raise ValueError("Choose a dataset from reference_catalogue()")
        if format not in ("csv", "parquet"):
            raise ValueError("format must be csv or parquet")
        if symbol is not None and (not isinstance(symbol, str) or not symbol.strip() or len(symbol) > 512):
            raise ValueError("Choose a symbol from reference_catalogue(dataset)")
        for value in (start, end):
            if value is not None:
                if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise ValueError("Reference dates use YYYY-MM-DD")
                date.fromisoformat(value)
        if start is not None and end is not None and start > end:
            raise ValueError("start must be on or before end")
        return {"dataset": dataset, "symbol": symbol, "from": start, "to": end, "format": format, "country": country, "q": q}

    def reference_preview(self, dataset: str, symbol: Optional[str] = None, start: Optional[str] = None,
                          end: Optional[str] = None, *, country: Optional[str] = None, q: Optional[str] = None) -> dict:
        """The last ten rows of a reference selection, every native column."""
        return self._json("/v3/reference/preview", **self._reference_params(dataset, symbol, start, end, "csv", country, q))

    def reference_estimate(self, dataset: str, symbol: Optional[str] = None, start: Optional[str] = None,
                           end: Optional[str] = None, *, country: Optional[str] = None, q: Optional[str] = None,
                           format: str = "parquet") -> dict:
        """The rows and bytes a reference export would have; the bytes are an upper bound."""
        return self._json("/v3/reference/estimate", **self._reference_params(dataset, symbol, start, end, format, country, q))

    def reference_export(self, dataset: str, symbol: Optional[str] = None, start: Optional[str] = None,
                         end: Optional[str] = None, *, country: Optional[str] = None, q: Optional[str] = None,
                         format: str = "parquet", dest=None, progress: Optional[Callable] = None,
                         timeout: float = 1800) -> str:
        """Every native column of a collection, or of one of its series, as a file."""
        params = self._reference_params(dataset, symbol, start, end, format, country, q)
        label = re.sub(r"[^A-Za-z0-9._-]", "_", symbol or "all")[:120]
        filename = f"{label}_{dataset}_{start or 'first'}_{end or 'latest'}.{format}"
        return self._export("/v3/reference/export", params, filename, dest, progress, timeout)

    def reference_rows(self, dataset: str, symbol: Optional[str] = None, start: Optional[str] = None,
                       end: Optional[str] = None, *, country: Optional[str] = None, q: Optional[str] = None) -> List[dict]:
        """A reference selection as a list of dicts, held in memory."""
        params = self._reference_params(dataset, symbol, start, end, "csv", country, q)
        return self._csv_rows(self._fetch_rows_bytes("/v3/reference/export", params))
