# historical_export.py
# Robust OHLCV exporter for Coinalyze.

import argparse
import datetime as dt
import json
import os
import sys
from typing import Optional

from coinalyze_api import get_future_markets, get_ohlcv_history

UTC = dt.timezone.utc

# --- Helpers ---
def parse_date(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)

def ensure_symbol(symbol: str) -> str:
    fut = get_future_markets()
    syms = [m["symbol"] for m in fut]
    if symbol not in syms:
        raise ValueError(f"Invalid symbol: {symbol}, available: {syms[:10]}...")
    return symbol

# --- Main Export ---
def export(symbol: str, interval: str, start: dt.datetime, end: dt.datetime, out_file: str):
    print(f"[INFO] Exporting {symbol} {interval} {start} → {end} into {out_file}")
    data = get_ohlcv_history(symbol, interval, int(start.timestamp()), int(end.timestamp()))
    pack = {
        "meta": {
            "symbol": symbol,
            "interval": interval,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "count": len(data),
        },
        "data": data,
    }
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(pack, f)
    print(f"[DONE] Wrote {len(data)} candles → {out_file}")

# --- Entry ---
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Coinalyze Historical Exporter")
    p.add_argument("--symbol", required=True, help="e.g. BTCUSDT_PERP.A")
    p.add_argument("--interval", required=True, help="e.g. 1min, 5min, 1h")

    # Modes: single date, explicit range, or whole month
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="Single day (YYYY-MM-DD)")
    g.add_argument("--from_to", nargs=2, metavar=("FROM", "TO"), help="Explicit range (YYYY-MM-DD YYYY-MM-DD)")
    g.add_argument("--month", help="Whole month (YYYY-MM)")

    p.add_argument("--out", required=True, help="Output JSON file")

    args = p.parse_args()
    symbol = ensure_symbol(args.symbol)

    if args.date:
        start = parse_date(args.date)
        end = start + dt.timedelta(days=1)
    elif args.from_to:
        start, end = map(parse_date, args.from_to)
    elif args.month:
        start = dt.datetime.strptime(args.month, "%Y-%m").replace(tzinfo=UTC)
        next_month = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        end = next_month
    else:
        sys.exit("No valid time range provided.")

    export(symbol, args.interval, start, end, args.out)
