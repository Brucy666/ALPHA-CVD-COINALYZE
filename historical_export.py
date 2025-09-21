# historical_export.py
"""
Robust OHLCV exporter for Coinalyze.

Features
- Symbol validation (fails fast if the symbol name is wrong)
- Single-day (--date), explicit range (--from --to), or whole month (--month YYYY-MM)
- JSONL output with clear metadata (symbol, interval, from, to, count)
- Verbose logging (so Railway logs show exactly what was requested and returned)
- Timezone-aware (UTC)

Examples
--------
Single day:
  python3 historical_export.py --symbol BTCUSDT_PERP.A --interval 1min \
    --date 2025-08-01 --out /data/exports/btc_1m_2025-08-01.jsonl

Explicit range:
  python3 historical_export.py --symbol BTCUSDT_PERP.A --interval 1min \
    --from 2025-08-01 --to 2025-08-07 \
    --out /data/exports/btc_1m_2025-08-01_to_2025-08-07.jsonl

Whole month (one file per day):
  python3 historical_export.py --symbol BTCUSDT_PERP.A --interval 1min \
    --month 2025-08 --out-dir /data/exports
"""

import argparse
import datetime as dt
import json
import os
import sys
from typing import Optional, Tuple, List

# Our API client (with built-in retries)
from coinalyze_api import get_future_markets, get_ohlcv_history

UTC = dt.timezone.utc


# ---------- Time utils ----------
def parse_date(s: str) -> dt.datetime:
    """
    Accepts 'YYYY-MM-DD' or 'YYYYMMDD'. Returns aware datetime at 00:00:00 UTC.
    """
    s = s.strip()
    if s.isdigit() and len(s) == 8:
        d = dt.datetime.strptime(s, "%Y%m%d")
    else:
        d = dt.datetime.strptime(s, "%Y-%m-%d")
    return d.replace(tzinfo=UTC)


def day_bounds(d: dt.datetime) -> Tuple[int, int]:
    """
    Given aware date at 00:00 UTC, return (start_ts, end_ts) for that UTC day.
    """
    start = dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=UTC)
    end = start + dt.timedelta(days=1) - dt.timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def month_days(year: int, month: int) -> List[dt.datetime]:
    """
    Return list of all UTC midnights for a given month.
    """
    first = dt.datetime(year, month, 1, tzinfo=UTC)
    if month == 12:
        nxt = dt.datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        nxt = dt.datetime(year, month + 1, 1, tzinfo=UTC)
    days = []
    cur = first
    while cur < nxt:
        days.append(cur)
        cur += dt.timedelta(days=1)
    return days


# ---------- IO ----------
def ensure_parent(path: str) -> None:
    p = os.path.dirname(path)
    if p and not os.path.exists(p):
        os.makedirs(p, exist_ok=True)


def write_jsonl(path: str, obj: dict) -> None:
    ensure_parent(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# ---------- Symbol validation ----------
def validate_symbol(symbol: str) -> None:
    markets = get_future_markets()
    symbols = {m.get("symbol") for m in markets if m.get("symbol")}
    if symbol not in symbols:
        # Try to help the user with suggestions
        suggestions = sorted([s for s in symbols if s and s.startswith(symbol[:3])])[:20]
        raise SystemExit(
            f"[ERROR] Symbol '{symbol}' not found at Coinalyze.\n"
            f"Try one of: {suggestions}\n"
            f"(Tip: run the helper script to list BTC symbols)"
        )


# ---------- Exporters ----------
def export_span(symbol: str, interval: str, start_ts: int, end_ts: int, out_path: str) -> int:
    """
    Fetch OHLCV for [start_ts, end_ts] and append to JSONL.
    Returns number of records (len of payload) written in this block.
    """
    print(f"[INFO] Request: symbol={symbol} interval={interval} "
          f"from={dt.datetime.fromtimestamp(start_ts, UTC)} "
          f"to={dt.datetime.fromtimestamp(end_ts, UTC)}")
    payload = get_ohlcv_history(symbol, interval, start_ts, end_ts)

    count = 0
    # many APIs return list; we’ll detect length
    if isinstance(payload, list):
        count = len(payload)
    elif isinstance(payload, dict):
        # if dict with 'data' or similar nesting
        if "data" in payload and isinstance(payload["data"], list):
            count = len(payload["data"])
        else:
            # unknown shape; we still store
            count = 1
    else:
        payload = payload  # None or unknown

    block = {
        "symbol": symbol,
        "interval": interval,
        "from": start_ts,
        "to": end_ts,
        "count": count,
        "payload": payload
    }
    write_jsonl(out_path, block)
    print(f"[INFO] Wrote block: count={count} → {out_path}")
    if count == 0:
        print("[WARN] Empty payload. (Likely wrong symbol or no data for this window.)")
    return count


def export_single_day(symbol: str, interval: str, day_utc: dt.datetime, out_path: str) -> int:
    start_ts, end_ts = day_bounds(day_utc)
    return export_span(symbol, interval, start_ts, end_ts, out_path)


def export_range(symbol: str, interval: str, start_utc: dt.datetime, end_utc: dt.datetime, out_path: str) -> int:
    """
    Export whole [start_utc, end_utc] inclusive.
    """
    start_ts = int(start_utc.timestamp())
    # inclusive end: add day-1s if end_utc provided at midnight
    end_ts = int((end_utc + dt.timedelta(days=1) - dt.timedelta(seconds=1)).timestamp())
    return export_span(symbol, interval, start_ts, end_ts, out_path)


def export_month(symbol: str, interval: str, y: int, m: int, out_dir: str, out_pattern: Optional[str]) -> None:
    """
    Export every day in a month. If out_pattern is provided, write into one file (append).
    Otherwise, one file per day in out_dir named {symbol}_{interval}_{YYYY-MM-DD}.jsonl
    """
    days = month_days(y, m)
    if out_pattern:
        # single file mode
        out_path = out_pattern
        if not out_path.endswith(".jsonl"):
            out_path += ".jsonl"
        print(f"[INFO] Month export (single file): {out_path}")
        for d in days:
            export_single_day(symbol, interval, d, out_path)
    else:
        # per-day files in out_dir
        if not out_dir:
            out_dir = "./"
        os.makedirs(out_dir, exist_ok=True)
        print(f"[INFO] Month export → directory: {out_dir}")
        for d in days:
            name = f"{symbol}_{interval}_{d.date()}.jsonl"
            out_path = os.path.join(out_dir, name)
            export_single_day(symbol, interval, d, out_path)


# ---------- CLI ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True, help="e.g. BTCUSDT_PERP.A (validate with inspect_symbols.py)")
    p.add_argument("--interval", default="1min")
    p.add_argument("--date", help="single day YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--from", dest="from_date", help="start date YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--to", dest="to_date", help="end date YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--month", help="month string YYYY-MM (exports all days)")
    p.add_argument("--out", help="output JSONL (for --date or --from/--to). If omitted in --month, writes per-day files.")
    p.add_argument("--out-dir", help="directory for per-day files (used with --month if --out not set)")
    args = p.parse_args()

    # 1) validate symbol early (fast fail)
    try:
        validate_symbol(args.symbol)
    except SystemExit as e:
        print(str(e))
        sys.exit(2)

    # 2) choose mode
    if args.date:
        d = parse_date(args.date)
        out_path = args.out or f"./{args.symbol}_{args.interval}_{d.date()}.jsonl"
        export_single_day(args.symbol, args.interval, d, out_path)
    elif args.from_date and args.to_date:
        start = parse_date(args.from_date)
        end = parse_date(args.to_date)
        out_path = args.out or f"./{args.symbol}_{args.interval}_{start.date()}_to_{end.date()}.jsonl"
        export_range(args.symbol, args.interval, start, end, out_path)
    elif args.month:
        y, m = map(int, args.month.split("-"))
        # If --out supplied with --month → single file; else per-day files.
        export_month(
            symbol=args.symbol,
            interval=args.interval,
            y=y, m=m,
            out_dir=args.out_dir or "./",
            out_pattern=args.out
        )
    else:
        print("[ERROR] Require one of: --date, (--from & --to), or --month YYYY-MM")
        sys.exit(2)

    print("[DONE] Export complete.")

if __name__ == "__main__":
    main()
