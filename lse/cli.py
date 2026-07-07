"""
Command-line interface for lse-data.

Usage:
    lse auth lse_live_xxxxxxxxxxxx
    lse stream BTC/USD ETH/USD AAPL
    lse browse          (or just `lse`: the databank browser in your terminal)
    lse datasets        (plain table of everything in the vault)
"""

import argparse
import json
import os
import sys
from pathlib import Path

from lse import LSE, __version__


# API key is stored in ~/.lse/config.json so users don't have to paste it
# on every command. Also respects LSE_API_KEY env var, which wins if set.
CONFIG_DIR = Path.home() / ".lse"
CONFIG_PATH = CONFIG_DIR / "config.json"


def load_api_key() -> str:
    env = os.environ.get("LSE_API_KEY")
    if env:
        return env
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text()).get("api_key", "")
        except Exception:
            pass
    return ""


def save_api_key(key: str):
    CONFIG_DIR.mkdir(exist_ok=True)
    CONFIG_PATH.write_text(json.dumps({"api_key": key}, indent=2))
    CONFIG_PATH.chmod(0o600)


def require_key() -> str:
    key = load_api_key()
    if not key:
        sys.exit(
            "No API key found. Set one with:\n"
            "  lse auth lse_live_xxxxxxxxxxxx\n"
            "Or set the LSE_API_KEY environment variable.\n"
            "Get a key at https://londonstrategicedge.com/data"
        )
    return key


def cmd_auth(args):
    save_api_key(args.key)
    print(f"Saved API key to {CONFIG_PATH}")


def cmd_stream(args):
    client = LSE(api_key=require_key())
    try:
        for tick in client.stream(args.symbols):
            bid = f"bid={tick.bid}" if tick.bid else ""
            ask = f"ask={tick.ask}" if tick.ask else ""
            print(f"{tick.symbol:<12} {tick.price:>12,.4f}  {bid}  {ask}")
    except KeyboardInterrupt:
        print("\nStopped.")


def cmd_browse(args):
    try:
        import curses  # noqa: F401  (absent on native Windows)
    except ImportError:
        sys.exit("The browser needs curses. On Windows: pip install windows-curses")
    from .tui import browse
    try:
        browse(LSE(api_key=require_key()))
    except curses.error:
        # No usable terminal (piped output, unset TERM, CI). Exit with a plain
        # message instead of a curses traceback; bare `lse` lands here too.
        sys.exit("lse browse needs an interactive terminal. "
                 "For a plain listing that works anywhere, run: lse datasets")
    except KeyboardInterrupt:
        pass


def cmd_datasets(args):
    # Plain, pipeable table (the browser's data without the browser). One row per
    # class plus the reference files, so `lse datasets` answers "what is there and
    # how much" in one screen.
    from .tui import fnum, REF_NAMES
    client = LSE(api_key=require_key())
    rows = client.datasets()
    agg = {}
    for r in rows:
        a = agg.setdefault(r["dataset"], [0, 0, "9999"])
        a[0] += 1
        a[1] += int(r["ticks"] or 0)  # a class with no ticks yet must not crash the table
        f = str(r["first_tick"])[:10]
        if f < a[2]:
            a[2] = f
    print(f"{'CLASS':<18}{'SYMBOLS':>9}{'TICKS':>10}   SINCE")
    for name, (n, t, first) in sorted(agg.items(), key=lambda x: -x[1][1]):
        print(f"{name:<18}{n:>9,}{fnum(t):>10}   {first[:4]}")
    try:
        refs = client.reference()
    except Exception:
        refs = []
    if refs:
        print(f"\n{'REFERENCE':<26}{'ROWS':>10}   COVERAGE")
        for r in refs:
            span = f"{r['first']} to {r['last']}" if r.get("first") else "snapshot"
            print(f"{REF_NAMES.get(r['dataset'], r['dataset']):<26}{fnum(r['rows']):>10}   {span}")


def main(argv=None):
    p = argparse.ArgumentParser(prog="lse", description="London Strategic Edge market data")
    p.add_argument("--version", action="version", version=f"lse-data {__version__}")
    sub = p.add_subparsers(dest="cmd", required=False)

    pa = sub.add_parser("auth", help="Save your API key locally")
    pa.add_argument("key", help="Your lse_live_* API key")
    pa.set_defaults(func=cmd_auth)

    ps = sub.add_parser("stream", help="Stream live ticks via WebSocket")
    ps.add_argument("symbols", nargs="+", help="One or more symbols to subscribe to")
    ps.set_defaults(func=cmd_stream)

    pb = sub.add_parser("browse", help="Browse and download the databank in your terminal")
    pb.set_defaults(func=cmd_browse)

    pd = sub.add_parser("datasets", help="Print everything in the vault as a plain table")
    pd.set_defaults(func=cmd_datasets)

    args = p.parse_args(argv)
    # Bare `lse` opens the browser: the two seconds from install to seeing the whole
    # databank is the product.
    if not args.cmd:
        return cmd_browse(args)
    args.func(args)


if __name__ == "__main__":
    main()
