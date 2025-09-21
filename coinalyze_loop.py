# coinalyze_loop.py
"""
Live polling loop that gathers snapshots + histories and writes snapshot + stream.
Supports computing a simple CVD from buy/sell endpoint if available.
Environment configuration:
  SYMBOL (optional) or pass via CLI
  INTERVAL (e.g. 1min, 5min)
  WINDOW_HOURS
  SLEEP_SECONDS
  PRINT_JSON
Run:
  python3 coinalyze_loop.py --symbol BTCUSDT_PERP.A
"""

import os
import time
import json
import argparse
import random
import signal
from typing import Optional

from coinalyze_api import (
    get_open_interest, get_funding_rate,
    get_open_interest_history, get_funding_rate_history,
    get_predicted_funding_rate_history, get_liquidation_history,
    get_long_short_ratio_history, get_ohlcv_history, get_buy_sell_history
)
from data_sink import write_snapshot, append_jsonl, retention_cleanup
from discord_poster import post_summary, build_embed

shutdown = False
def _sigterm(*_):
    global shutdown
    shutdown = True

signal.signal(signal.SIGINT, _sigterm)
signal.signal(signal.SIGTERM, _sigterm)

def now_ts(): return int(time.time())

def sleep_with_jitter(sec):
    time.sleep(max(0, sec + random.uniform(0, 0.25*sec)))

def compute_cvd_from_taker(taker_history_payload) -> Optional[float]:
    """
    Expect taker_history_payload to be list of {ts, buy_volume, sell_volume} or similar.
    Returns cumulative buy - sell (CVD) for available range; returns None if not computable.
    """
    if not taker_history_payload:
        return None
    cvd = 0.0
    # attempt to navigate common shapes
    # payload might be {"data": [{...}, ...]} or a list directly
    rows = taker_history_payload.get("data") if isinstance(taker_history_payload, dict) and "data" in taker_history_payload else taker_history_payload
    if not isinstance(rows, list):
        return None
    for r in rows:
        buy = r.get("buy_volume") or r.get("taker_buy") or r.get("buy") or 0
        sell = r.get("sell_volume") or r.get("taker_sell") or r.get("sell") or 0
        try:
            cvd += float(buy) - float(sell)
        except Exception:
            continue
    return cvd

def fetch_block(symbol: str, interval: str, window_hr: int):
    t1 = now_ts()
    t0 = t1 - window_hr*3600
    # snapshots
    oi = get_open_interest(symbol)
    fr = get_funding_rate(symbol)
    # histories
    oi_hist = get_open_interest_history(symbol, interval, t0, t1)
    fr_hist = get_funding_rate_history(symbol, interval, t0, t1)
    pfr_hist = get_predicted_funding_rate_history(symbol, interval, t0, t1)
    liq_hist = get_liquidation_history(symbol, interval, t0, t1)
    ls_hist = get_long_short_ratio_history(symbol, interval, t0, t1)
    ohlcv = get_ohlcv_history(symbol, interval, t0, t1)

    # attempt taker history for CVD
    try:
        taker = get_buy_sell_history(symbol, interval, t0, t1)
        cvd = compute_cvd_from_taker(taker)
    except NotImplementedError:
        cvd = None
    except Exception:
        cvd = None

    return {
        "symbol": symbol,
        "interval": interval,
        "window_hours": window_hr,
        "snapshots": {"open_interest": oi, "funding_rate": fr},
        "history": {
            "open_interest": oi_hist,
            "funding_rate": fr_hist,
            "predicted_funding_rate": pfr_hist,
            "liquidations": liq_hist,
            "long_short_ratio": ls_hist,
            "ohlcv": ohlcv,
            # include taker if present (raw)
            "taker": taker if 'taker' in locals() else None
        },
        "computed": {"cvd": cvd},
        "fetched_at": t1
    }

def main_loop(symbol: str, interval: str, window_hr: int, sleep_sec: int, print_json: bool):
    print(f"=== AlphaOps • Coinalyze Live ===")
    print(f"Symbol: {symbol} | Interval: {interval} | Window(h): {window_hr}")
    print("Ctrl+C to stop.\n")

    backoff = sleep_sec
    cycle = 0
    while not shutdown:
        t0 = time.time()
        try:
            pack = fetch_block(symbol, interval, window_hr)

            # persist
            snapshot_path = write_snapshot(symbol, interval, pack)
            stream_path = append_jsonl(symbol, interval, pack)

            # terminal summary
            oi_now = (pack["snapshots"].get("open_interest") or [{}])[0]
            fr_now = (pack["snapshots"].get("funding_rate") or [{}])[0]
            cvd = pack["computed"].get("cvd")
            ohlcv_len = len(pack["history"].get("ohlcv") or [])
            liq_len = len(pack["history"].get("liquidations") or [])
            ls_len = len(pack["history"].get("long_short_ratio") or [])
            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"TF:{interval} OI:{oi_now.get('value','?')} FR:{fr_now.get('value','?')} "
                  f"Candles:{ohlcv_len} LIQ:{liq_len} LS:{ls_len} CVD:{cvd if cvd is not None else 'NA'} "
                  f"Saved:{snapshot_path.split('/')[-1]}  Dur:{round(time.time()-t0,2)}s")

            if print_json:
                s = json.dumps(pack, separators=(",", ":"), ensure_ascii=False)
                print(s[:800] + ("..." if len(s) > 800 else ""))

            # discord
            try:
                post_summary(f"Coinalyze • {symbol} • {interval}", build_embed(symbol, interval, pack))
            except Exception as e:
                print("Discord post error:", repr(e))

            # periodic retention
            cycle += 1
            if cycle % 60 == 0:
                retention_cleanup()

            backoff = sleep_sec
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ERROR: {repr(e)} | backoff:{backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 600)
            continue

        sleep_with_jitter(sleep_sec)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="symbol override (e.g. BTCUSDT_PERP.A)")
    parser.add_argument("--interval", default=os.getenv("INTERVAL", "5min"))
    parser.add_argument("--window", type=int, default=int(os.getenv("WINDOW_HOURS", "6")))
    parser.add_argument("--sleep", type=int, default=int(os.getenv("SLEEP_SECONDS", "60")))
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()

    symbol = args.symbol or os.getenv("SYMBOL")
    if not symbol:
        raise SystemExit("Symbol required (env SYMBOL or --symbol).")

    main_loop(symbol, args.interval, args.window, args.sleep, args.print_json)
