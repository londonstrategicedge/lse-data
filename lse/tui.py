"""
`lse browse`: the catalogue in your terminal.

A full screen browser over LSE Data 3.0: every market and reference
collection, how many instruments and stored rows each holds, how far back the
history reaches, and a download flow that saves any slice as Parquet without
leaving the terminal. Built on the standard library's curses so it adds no
dependency (Windows needs `pip install windows-curses`).

Keys: arrows move, Enter opens, / searches, t cycles timeframe, q goes back.
"""

import curses
from datetime import date, timedelta

from .data3 import TIMEFRAMES

# ── formatting ───────────────────────────────────────────────────────────────

def fnum(n) -> str:
    n = float(n or 0)
    for cut, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= cut:
            return f"{n / cut:.1f}{suf}"
    return f"{int(n)}"


def day(s) -> str:
    return str(s)[:10] if s else ""


# Reference collection display names (numbers always come from the API).
REF_NAMES = {
    "insider_trades": "Insider trades", "dividends": "Dividends",
    "bond_yields": "Government bond yields", "bond_yields_intraday": "Bond yields, intraday",
    "economic_calendar": "Economic calendar", "financial_reports": "Financial statements",
    "stock_splits": "Stock splits", "cot": "COT positioning", "company_profiles": "Company profiles",
    "stock_fundamentals": "Fundamentals", "sovereign_yields": "Sovereign yields",
    "corporate_bonds": "Corporate bonds", "bond_futures": "Bond futures", "credit_indices": "Credit indices",
    "currency_index": "Currency index", "fx_derivatives": "FX derivatives",
    "interest_rates": "Interest rates", "volatility": "Volatility",
}
# The two markets listed by underlying rather than by contract.
CONTRACT_MARKETS = ("us_options", "cme")


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
        self.meta = {"timeframes": [t for t in TIMEFRAMES if t != "tick"]}
        self.rows = self.c.datasets()
        try:
            self.refs = self.c.reference()
        except Exception:
            self.refs = []
        agg = {}
        for r in self.rows:
            a = agg.setdefault(r["dataset"], [0, 0, "9999"])
            a[0] += 1
            a[1] += int(r["ticks"] or 0)  # tolerate a market with no count yet
            f = day(r["first_tick"])
            if f and f < a[2]:
                a[2] = f
        self.classes = sorted(((k, v[0], v[1], v[2] if v[2] != "9999" else "") for k, v in agg.items()),
                              key=lambda x: -x[2])

    # ── curses main ─────────────────────────────────────────────────────

    def run(self, scr):
        curses.curs_set(0)
        scr.timeout(-1)
        self.home(scr)

    def head(self, scr, crumb):
        h, w = scr.getmaxyx()
        total = sum(c[2] for c in self.classes)
        scr.addnstr(0, 2, "LSE DATA 3.0", w - 4, curses.A_BOLD)
        scr.addnstr(0, 16, crumb, max(1, w - 18))
        line = (f"{len(self.rows):,} instruments | {fnum(total)} stored rows | "
                f"{len(self.classes)} markets | {len(self.refs)} reference collections")
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
            scr.addnstr(y - 1, 4, "MARKETS                        SYMBOLS         ROWS   SINCE",
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
                        scr.addnstr(y, 4, "REFERENCE COLLECTIONS             ROWS   COVERAGE",
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
                      key=lambda r: -int(r["ticks"] or 0))
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
            scr.addnstr(4, 4, "SYMBOL             ROWS     PER DAY     FIRST         LAST",
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
                ticks = int(r["ticks"] or 0)
                line = (f"{r['symbol']:<15}{fnum(ticks):>8}  {fnum(ticks / days):>9}"
                        f"     {t0 or '':<10}    {t1 or ''}")
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
        tfs = ["tick"] + self.meta.get("timeframes", [])
        tfi = 0
        contract_market = (not ref) and dataset in CONTRACT_MARKETS
        if ref:
            start = end = None
        else:
            last = day(symbol["last_tick"]) or str(date.today())
            first = day(symbol["first_tick"]) or "2000-01-01"
            # The trade slice defaults to the last 30 days; candles to the full span.
            start, end = (max(first, str(date.fromisoformat(last) - timedelta(days=30))), last)
        stage = None
        while True:
            scr.erase()
            h, w = scr.getmaxyx()
            name = REF_NAMES.get(dataset, dataset) if ref else symbol["symbol"]
            self.head(scr, f"/ download / {name}")
            y = 4
            if ref:
                scr.addnstr(y, 4, f"{name}: the whole collection as one Parquet file.", w - 6); y += 2
            elif contract_market:
                scr.addnstr(y, 4, f"{symbol['symbol']}  ({dataset}) is an underlying with "
                            f"{symbol.get('contracts') or ''} contracts.", w - 6); y += 1
                scr.addnstr(y, 4, "Download one contract with client.export(market, contract, ...); "
                            "client.pairs(market, underlying=...) lists them.", w - 6); y += 2
            else:
                scr.addnstr(y, 4, f"{symbol['symbol']}  ({dataset})  "
                            f"{fnum(symbol['ticks'])} rows, {day(symbol['first_tick']) or '?'} to "
                            f"{day(symbol['last_tick']) or '?'}", w - 6); y += 2
                tf = tfs[tfi]
                scr.addnstr(y, 4, f"timeframe : {tf}   (t to cycle: {' '.join(tfs)})", w - 6); y += 1
                scr.addnstr(y, 4, f"range     : {start} to {end}   (full span: f, last month: m)",
                            w - 6); y += 1
            y += 1
            scr.addnstr(y, 4, "q back" if contract_market else "d download    q back", w - 6, curses.A_BOLD)
            if stage:
                scr.addnstr(y + 2, 4, stage, w - 6, curses.A_REVERSE)
            scr.refresh()
            k = scr.getch()
            if k == ord("q"):
                return
            if contract_market:
                continue
            if not ref and k == ord("t"):
                tfi = (tfi + 1) % len(tfs)
                first = day(symbol["first_tick"]) or "2000-01-01"
                start = first if tfs[tfi] != "tick" else \
                    max(first, str(date.fromisoformat(end) - timedelta(days=30)))
            elif not ref and k == ord("f"):
                start = day(symbol["first_tick"]) or "2000-01-01"
            elif not ref and k == ord("m"):
                start = max(day(symbol["first_tick"]) or "2000-01-01",
                            str(date.fromisoformat(end) - timedelta(days=30)))
            elif k == ord("d") and not stage:
                try:
                    stage = "Preparing"
                    self._draw_stage(scr, stage)

                    def progress(info):
                        phase = info.get("phase")
                        if phase in ("building", "queued"):
                            self._draw_stage(scr, "Building")
                        elif phase == "downloading":
                            self._draw_stage(scr, f"Downloading {fnum(info.get('bytes') or 0)}B")
                    if ref:
                        path = self.c.dataset(dataset, dataframe=False, progress=progress)
                    else:
                        path = self.c.history(symbol["symbol"], market=dataset, timeframe=tfs[tfi],
                                              start=start, end=end, dataframe=False, progress=progress)
                    import os
                    stage = f"Saved {path} ({fnum(os.path.getsize(path))}B)"
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
    print("Loading the catalogue...")
    b.load()
    curses.wrapper(b.run)
