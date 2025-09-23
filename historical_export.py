# historical_export.py
"""
Robust OHLCV exporter for Coinalyze.

- Validates symbol against /future-markets
- Modes: --date YYYY-MM-DD  OR  --from YYYY-MM-DD --to YYYY-MM-DD  OR  --month YYYY-MM
- Parses Coinalyze response (list of {"symbol": "...", "history": [...]})
- Writes flat JSONL: one candle per line, with fields: symbol, interval, ts, o,h,l,c,v,bv (when provided)
- Verbose logging for Railway
"""

import argparse
import datetime as dt
import json
import os
import sys
from typing import List, Dict

from coinalyze_api import get_future_markets, get_ohlcv_history

UTC = dt.timezone.utc


# ---------------- Time helpers ----------------
def parse_date(s: str) -> dt.datetime:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        # Allow YYYYMMDD
        return dt.datetime.strptime(s, "%Y%m%d").replace(tzinfo=UTC)


def month_bounds(s: str) -> (dt.datetime, dt.datetime):
    """s = 'YYYY-MM' -> (start_utc, end_utc_exclusive)"""
    start = dt.datetime.strptime(s, "%Y-%m").replace(tzinfo=UTC)
    # next month
    if start.month == 12:
        nxt = dt.datetime(start.year + 1, 1, 1, tzinfo=UTC)
    else:
        nxt = dt.datetime(start.year, start.month + 1, 1, tzinfo=UTC)
    return start, nxt


# ---------------- Symbol validation ----------------
def validate_symbol(symbol: str) -> None:
    fut = get_future_markets()
    symbols = [m.get("symbol") for m in fut if m.get("symbol")]
    if symbol not in symbols:
        # Offer a few hints
        hints = [s for s in symbols if s and s.startswith(symbol[:3])][:20]
        raise SystemExit(
            f"[ERROR] Symbol '{symbol}' not found on Coinalyze.\n"
            f"Try one of: {hints}\n"
            f"(Tip: run inspect_symbols.py to list exchange-specific names.)"
        )


# ---------------- Writer ----------------
def ensure_parent(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def write_jsonl_rows(out_path: str, rows: List[Dict]) -> int:
    ensure_parent(out_path)
    with open(out_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(rows)


# ---------------- Flatten Coinalyze payload ----------------
def flatten_ohlcv_payload(payload, interval: str) -> List[Dict]:
    """
    Coinalyze /ohlcv-history returns:
      [
        { "symbol": "BTCUSDT_PERP.A", "history": [ { ...candle... }, ... ] },
        ...
      ]
    We flatten to one JSON per candle with keys: symbol, interval, ts,o,h,l,c,v,bv (+ pass-through extras).
    """
    out = []
    if not isinstance(payload, list):
        print(f"[WARN] Unexpected payload type: {type(payload).__name__}")
        return out

    for entry in payload:
        if not isinstance(entry, dict):
            continue
        sym = entry.get("symbol")
        hist = entry.get("history") or []
        for c in hist:
            # Allow for different key naming; map commonly used keys if present.
            row = {
                "symbol": sym,
                "interval": interval,
                "ts": c.get("t") or c.get("ts") or c.get("time"),
                "o":  c.get("o") or c.get("open"),
                "h":  c.get("h") or c.get("high"),
                "l":  c.get("l") or c.get("low"),
                "c":  c.get("c") or c.get("close"),
                "v":  c.get("v") or c.get("volume"),
                "bv": c.get("bv") or c.get("buy_volume") or c.get("taker_buy_volume"),
            }
            # keep any extra fields
            for k, v in c.items():
                if k not in row:
                    row[k] = v
            out.append(row)
    return out


# ---------------- Export core ----------------
def export_span(symbol: str, interval: str, start: dt.datetime, end: dt.datetime, out_file: str) -> int:
    print(f"[INFO] Request: symbol={symbol} interval={interval}  from={start}  to={end}")
    payload = get_ohlcv_history(symbol, interval, int(start.timestamp()), int(end.timestamp()))
    rows = flatten_ohlcv_payload(payload, interval)
    n = write_jsonl_rows(out_file, rows)
    print(f"[DONE] Wrote {n} candles → {out_file}")
    if n == 0:
        print("[WARN] Zero candles returned. Check symbol spelling or date coverage.")
    return n


# ---------------- CLI ----------------
def main():
    p = argparse.ArgumentParser(description="Coinalyze OHLCV exporter")
    p.add_argument("--symbol", required=True, help="e.g. BTCUSDT_PERP.A")
    p.add_argument("--interval", required=True, help="e.g. 1min, 5min, 15min, 1h")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="YYYY-MM-DD or YYYYMMDD")
    g.add_argument("--from", dest="from_date", help="YYYY-MM-DD or YYYYMMDD")
    g.add_argument("--month", help="YYYY-MM")  # whole month
    p.add_argument("--to", dest="to_date", help="YYYY-MM-DD or YYYYMMDD (use with --from)")
    p.add_argument("--out", required=True, help="Output JSONL path")
    args = p.parse_args()

    # Validate symbol early
    validate_symbol(args.symbol)

    if args.date:
        start = parse_date(args.date)
        end = start + dt.timedelta(days=1)
    elif args.from_date and args.to_date:
        start = parse_date(args.from_date)
        end = parse_date(args.to_date) + dt.timedelta(days=1)  # inclusive end
    elif args.month:
        start, end = month_bounds(args.month)
    else:
        p.error("Provide one of --date, (--from and --to), or --month.")

    export_span(args.symbol, args.interval, start, end, args.out)


if __name__ == "__main__":
    main()
