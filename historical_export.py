# historical_export.py
"""
CLI tool to export OHLCV history for a single day (or date range) to JSONL.
Usage examples:
  python3 historical_export.py --symbol BTCUSDT_PERP.A --interval 1min --date 20250801 --out btc_1min_20250801.jsonl
  python3 historical_export.py --symbol BTCUSDT_PERP.A --interval 5min --from 2025-08-01 --to 2025-08-07 --out week_aug.jsonl
Notes:
 - Date format: YYYYMMDD or YYYY-MM-DD (both allowed)
 - File is JSONL: one JSON object per fetch block (returned API content)
"""

import argparse
import time
import datetime
import json
from typing import Optional
from coinalyze_api import get_ohlcv_history

def parse_date_arg(s: str) -> int:
    s = s.strip()
    fmt = "%Y%m%d" if s.isdigit() and len(s) == 8 else "%Y-%m-%d"
    dt = datetime.datetime.strptime(s, fmt)
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())

def day_start_end_from_date(ts: int):
    # return day's start (00:00 UTC) and end (23:59:59)
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    start = datetime.datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())

def write_jsonl_line(path: str, obj):
    with open(path, "a", encoding="utf8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def export_one_day(symbol: str, interval: str, date_ts: int, out_path: str, chunk_hours: int = 24):
    # fetch start/end for the date
    start_ts, end_ts = day_start_end_from_date(date_ts)
    # call API (some providers return arrays; we store the raw reply into JSONL)
    print(f"Fetching {symbol} {interval} from {datetime.datetime.utcfromtimestamp(start_ts)} to {datetime.datetime.utcfromtimestamp(end_ts)}")
    data = get_ohlcv_history(symbol, interval, start_ts, end_ts)
    if not data:
        print("No data returned for that day.")
    write_jsonl_line(out_path, {"symbol": symbol, "interval": interval, "from": start_ts, "to": end_ts, "payload": data})
    print("Wrote:", out_path)

def export_range(symbol: str, interval: str, from_ts: int, to_ts: int, out_path: str):
    # simple single-call (API supports from/to)
    print(f"Fetching range {symbol} {interval} {from_ts}->{to_ts}")
    data = get_ohlcv_history(symbol, interval, from_ts, to_ts)
    write_jsonl_line(out_path, {"symbol": symbol, "interval": interval, "from": from_ts, "to": to_ts, "payload": data})
    print("Wrote:", out_path)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--interval", default="1min")
    p.add_argument("--date", help="single date YYYYMMDD or YYYY-MM-DD")
    p.add_argument("--from", dest="from_date", help="start date YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--to", dest="to_date", help="end date YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--out", required=True, help="output JSONL path")
    args = p.parse_args()

    if args.date:
        ts = parse_date_arg(args.date)
        export_one_day(args.symbol, args.interval, ts, args.out)
    elif args.from_date and args.to_date:
        from_ts = parse_date_arg(args.from_date)
        to_ts = parse_date_arg(args.to_date)
        # to_ts should be end-of-day
        to_ts = to_ts + 24*3600 - 1
        export_range(args.symbol, args.interval, from_ts, to_ts, args.out)
    else:
        p.error("Either --date or both --from and --to required.")

if __name__ == "__main__":
    main()
