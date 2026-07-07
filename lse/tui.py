"""
`lse browse`: the databank in your terminal.

A full screen browser over the vault: every asset class and reference dataset,
how many instruments and ticks each holds, how far back the history reaches,
and a download flow that pulls any slice as Parquet without leaving the
terminal. Built on the standard library's curses so it adds no dependency
(Windows needs `pip install windows-curses`).

Keys: arrows move, Enter opens, / searches, t cycles timeframe, q goes back.
"""

import curses
import time
from datetime import date, timedelta

# ── formatting ───────────────────────────────────────────────────────────────

def fnum(n) -> str:
    n = float(n or 0)
    for cut, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= cut:
            return f"{n / cut:.1f}{suf}"
    return f"{int(n)}"


def day(s) -> str:
    return str(s)[:10] if s else ""


# Reference dataset display names (numbers always come from the API).
REF_NAMES = {
    "insider_trades": "Insider trades", "dividends": "Dividends",
    "bond_yields": "Government bond yields", "economic_calendar": "Economic calendar",
    "financial_reports": "Financial statements", "stock_splits": "Stock splits",
    "cot": "COT positioning", "company_profiles": "Company profiles",
    "stock_fundamentals": "Fundamentals",
}


class Browser:
    def __init__(self, client):
        self.c = client
        self.rows = []          # full catalog
        self.refs = []          # reference datasets
        self.meta = {}
        self.classes = []       # [(name, n_symbols, ticks, earliest)]
        self.status = ""

    # ── data ────────────────────────────────────────────────────────────

    def load(self):
        self.meta = self.c.vault_meta()
        self.rows = self.c.datasets()
        try:
            self.refs = self.c.reference()
        except Exception:
            self.refs = []
        agg = {}
        for r in self.rows:
            a = agg.setdefault(r["dataset"], [0, 0, "9999"])
            a[0] += 1
            a[1] += int(r["ticks"] or 0)  # tolerate a class with no ticks yet
            f = day(r["first_tick"])
            if f and f < a[2]:
                a[2] = f
        self.classes = sorted(((k, v[0], v[1], v[2]) for k, v in agg.items()),
                              key=lambda x: -x[2])

    # ── curses main ─────────────────────────────────────────────────────

    def run(self, scr):
        curses.curs_set(0)
        scr.timeout(-1)
        self.home(scr)

    def head(self, scr, crumb):
        h, w = scr.getmaxyx()
        total = sum(c[2] for c in self.classes)
        scr.addnstr(0, 2, "LSE DATABANK", w - 4, curses.A_BOLD)
        scr.addnstr(0, 16, crumb, max(1, w - 18))
        line = (f"{len(self.rows):,} datasets | {fnum(total)} ticks | "
                f"{len(self.classes)} classes | {len(self.refs)} reference files")
        scr.addnstr(1, 2, line, w - 4, curses.A_DIM)
        scr.hline(2, 1, curses.ACS_HLINE, w - 2)
        if self.status:
            scr.addnstr(h - 1, 2, self.status[: w - 4], w - 4, curses.A_REVERSE)

    # ── home: classes + reference ───────────────────────────────────────

    def home(self, scr):
        idx = 0
        while True:
            items = ([("class",) + c for c in self.classes] +
                     [("ref", r["dataset"], r["rows"], r.get("first"), r.get("last"))
                      for r in self.refs])
            scr.erase()
            h, w = scr.getmaxyx()
            self.head(scr, "/ home")
            y = 4
            scr.addnstr(y - 1, 4, "ASSET CLASSES                  SYMBOLS        TICKS   SINCE",
                        w - 6, curses.A_DIM)
            for i, it in enumerate(items):
                if y >= h - 2:
                    break
                sel = curses.A_REVERSE if i == idx else 0
                if it[0] == "class":
                    _, name, nsym, ticks, first = it
                    scr.addnstr(y, 4, f"{name:<28}{nsym:>9,}   {fnum(ticks):>8}   {first[:4]}",
                                w - 6, sel)
                else:
                    if items[i - 1][0] == "class":
                        y += 1
                        scr.addnstr(y, 4, "REFERENCE DATASETS                ROWS   COVERAGE",
                                    w - 6, curses.A_DIM)
                        y += 1
                    _, name, rows, first, last = it
                    label = REF_NAMES.get(name, name)
                    span = f"{day(first)} to {day(last)}" if first else "snapshot"
                    scr.addnstr(y, 4, f"{label:<28}{fnum(rows):>10}   {span}", w - 6, sel)
                y += 1
            scr.addnstr(h - 1, w - 34, "enter open   q quit", 30, curses.A_DIM)
            scr.refresh()
            k = scr.getch()
            if k == ord("q"):
                return
            if k == curses.KEY_UP:
                idx = max(0, idx - 1)
            elif k == curses.KEY_DOWN:
                idx = min(len(items) - 1, idx + 1)
            elif k in (curses.KEY_ENTER, 10, 13) and items:
                it = items[idx]
                if it[0] == "class":
                    self.symbols(scr, it[1])
                else:
                    self.download(scr, dataset=it[1], symbol=None, ref=True)

    # ── symbol list for one class ───────────────────────────────────────

    def symbols(self, scr, cls):
        rows = sorted((r for r in self.rows if r["dataset"] == cls),
                      key=lambda r: -int(r["ticks"]))
        idx = top = 0
        query = ""
        while True:
            match = [r for r in rows if query.upper() in r["symbol"].upper()] if query else rows
            idx = min(idx, max(0, len(match) - 1))
            scr.erase()
            h, w = scr.getmaxyx()
            self.head(scr, f"/ {cls}")
            scr.addnstr(3, 4, f"search: {query}_" if query else
                        f"{len(match):,} symbols   (/ to search)", w - 6, curses.A_DIM)
            scr.addnstr(4, 4, "SYMBOL            TICKS     PER DAY     FIRST         LAST",
                        w - 6, curses.A_DIM)
            view_h = h - 7
            if idx < top:
                top = idx
            if idx >= top + view_h:
                top = idx - view_h + 1
            for i, r in enumerate(match[top: top + view_h]):
                j = top + i
                t0, t1 = day(r["first_tick"]), day(r["last_tick"])
                days = max(1, (date.fromisoformat(t1) - date.fromisoformat(t0)).days) if t0 and t1 else 1
                line = (f"{r['symbol']:<15}{fnum(r['ticks']):>8}  {fnum(int(r['ticks']) / days):>9}"
                        f"     {t0}    {t1}")
                scr.addnstr(5 + i, 4, line, w - 6, curses.A_REVERSE if j == idx else 0)
            scr.addnstr(h - 1, w - 44, "enter download   / search   q back", 40, curses.A_DIM)
            scr.refresh()
            k = scr.getch()
            if k == ord("q"):
                if query:
                    query = ""
                    continue
                return
            if k == ord("/"):
                query = ""
                curses.curs_set(1)
                while True:
                    k2 = scr.getch()
                    if k2 in (curses.KEY_ENTER, 10, 13, 27):
                        break
                    if k2 in (curses.KEY_BACKSPACE, 127, 8):
                        query = query[:-1]
                    elif 32 <= k2 < 127:
                        query += chr(k2)
                    scr.addnstr(3, 4, f"search: {query}_" + " " * 20, scr.getmaxyx()[1] - 6)
                    scr.refresh()
                curses.curs_set(0)
                idx = 0
            elif k == curses.KEY_UP:
                idx = max(0, idx - 1)
            elif k == curses.KEY_DOWN:
                idx = min(len(match) - 1, idx + 1)
            elif k == curses.KEY_NPAGE:
                idx = min(len(match) - 1, idx + view_h)
            elif k == curses.KEY_PPAGE:
                idx = max(0, idx - view_h)
            elif k in (curses.KEY_ENTER, 10, 13) and match:
                self.download(scr, dataset=cls, symbol=match[idx])

    # ── download pane ───────────────────────────────────────────────────

    def download(self, scr, dataset, symbol, ref=False):
        tfs = ["tick"] + (self.meta.get("timeframes", [])
                          if dataset in self.meta.get("candle_classes", []) else [])
        tfi = 0
        if ref:
            start = end = None
        else:
            last = day(symbol["last_tick"])
            first = day(symbol["first_tick"])
            # Raw ticks default to the last 30 days; candles to the full span.
            start, end = (max(first, str(date.fromisoformat(last) - timedelta(days=30))), last)
        stage = None
        while True:
            scr.erase()
            h, w = scr.getmaxyx()
            name = REF_NAMES.get(dataset, dataset) if ref else symbol["symbol"]
            self.head(scr, f"/ download / {name}")
            y = 4
            if ref:
                scr.addnstr(y, 4, f"{name}: the whole dataset as one Parquet file.", w - 6); y += 2
            else:
                scr.addnstr(y, 4, f"{symbol['symbol']}  ({dataset})  "
                            f"{fnum(symbol['ticks'])} ticks, {day(symbol['first_tick'])} to "
                            f"{day(symbol['last_tick'])}", w - 6); y += 2
                tf = tfs[tfi]
                scr.addnstr(y, 4, f"timeframe : {tf}   (t to cycle: {' '.join(tfs)})", w - 6); y += 1
                scr.addnstr(y, 4, f"range     : {start} to {end}   (full span: f, last month: m)",
                            w - 6); y += 1
            y += 1
            scr.addnstr(y, 4, "d download    q back", w - 6, curses.A_BOLD)
            if stage:
                scr.addnstr(y + 2, 4, stage, w - 6, curses.A_REVERSE)
            scr.refresh()
            if stage and stage.startswith(("Queued", "Building", "Preparing")):
                # Poll without blocking on a keypress.
                scr.timeout(200)
            k = scr.getch()
            scr.timeout(-1)
            if k == ord("q"):
                return
            if not ref and k == ord("t"):
                tfi = (tfi + 1) % len(tfs)
                # Candles default to the full recorded span, ticks to the last month.
                start = day(symbol["first_tick"]) if tfs[tfi] != "tick" else \
                    max(day(symbol["first_tick"]),
                        str(date.fromisoformat(day(symbol["last_tick"])) - timedelta(days=30)))
            elif not ref and k == ord("f"):
                start = day(symbol["first_tick"])
            elif not ref and k == ord("m"):
                start = max(day(symbol["first_tick"]),
                            str(date.fromisoformat(end) - timedelta(days=30)))
            elif k == ord("d") and not stage:
                try:
                    stage = "Preparing"
                    self._draw_stage(scr, stage)
                    if ref:
                        job = self.c._vault_call("/export", {"dataset": dataset,
                                                             "format": "parquet"})
                        fname = f"{dataset}.parquet"
                    else:
                        job = self.c._vault_call("/export", {
                            "dataset": dataset, "symbol": symbol["symbol"],
                            "timeframe": tfs[tfi], "start": start, "end": end,
                            "format": "parquet"})
                        fname = (f"{dataset}_{symbol['symbol'].replace('/', '_')}"
                                 f"_{tfs[tfi]}.parquet")
                    info = None
                    while True:
                        info = self.c._vault_call(f"/export/{job['job_id']}")
                        s = info.get("status")
                        if s == "ready":
                            break
                        if s in ("failed", "expired"):
                            raise RuntimeError(info.get("error") or s)
                        self._draw_stage(scr, "Building" if s == "running" else "Queued")
                        time.sleep(1.2)
                    self._draw_stage(scr, "Downloading")
                    path = self.c._vault_download(job["job_id"], fname, None, info)
                    stage = f"Saved {path} ({fnum(info.get('bytes') or 0)}B)"
                except Exception as e:
                    stage = f"Failed: {e}"[:120]

    def _draw_stage(self, scr, text):
        h, w = scr.getmaxyx()
        scr.addnstr(h - 3, 4, (text + "...").ljust(w - 8)[: w - 6], w - 6, curses.A_REVERSE)
        scr.refresh()


def browse(client):
    import os
    # Assemble arrow-key escape sequences fast instead of the 1s default.
    os.environ.setdefault("ESCDELAY", "25")
    b = Browser(client)
    print("Loading the vault catalog...")
    b.load()
    curses.wrapper(b.run)
