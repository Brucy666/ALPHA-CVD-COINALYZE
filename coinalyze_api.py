# coinalyze_api.py
"""
Robust Coinalyze API client.
Set COINALYZE_API_KEY in env.
Functions accept symbols as either a single string or list.
All responses returned as parsed JSON.
"""

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Union, List

API_KEY = os.getenv("COINALYZE_API_KEY") or os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing COINALYZE_API_KEY env var.")

BASE = os.getenv("COINALYZE_BASE", "https://api.coinalyze.net/v1")
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "alphaops-coinalyze/1.0"
}

# Session with retries
_session = requests.Session()
retries = Retry(
    total=6,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
_session.mount("https://", HTTPAdapter(max_retries=retries))


def _ensure_symbols(symbols: Union[str, List[str]]) -> str:
    if isinstance(symbols, (list, tuple)):
        return ",".join(s.strip() for s in symbols)
    return str(symbols)


def _get(path: str, params: dict = None, timeout: int = 20):
    url = BASE.rstrip("/") + path
    try:
        resp = _session.get(url, headers=HEADERS, params=params or {}, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        # Bubble up a descriptive error.
        raise RuntimeError(f"Coinalyze API request failed: {e} (url={url}, params={params})")


# --- discovery ---
def get_exchanges():
    return _get("/exchanges")


def get_future_markets():
    return _get("/future-markets")


def get_spot_markets():
    return _get("/spot-markets")


# --- current snapshots (require symbol(s)) ---
def get_open_interest(symbols: Union[str, List[str]], convert_to_usd: bool = False):
    return _get("/open-interest", {"symbols": _ensure_symbols(symbols), "convert_to_usd": str(convert_to_usd).lower()})


def get_funding_rate(symbols: Union[str, List[str]]):
    return _get("/funding-rate", {"symbols": _ensure_symbols(symbols)})


def get_predicted_funding_rate(symbols: Union[str, List[str]]):
    return _get("/predicted-funding-rate", {"symbols": _ensure_symbols(symbols)})


# --- histories (require symbols, interval, from, to) ---
def get_open_interest_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int, convert_to_usd: bool = False):
    return _get("/open-interest-history", {
        "symbols": _ensure_symbols(symbols),
        "interval": interval,
        "from": start_ts,
        "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })


def get_funding_rate_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int):
    return _get("/funding-rate-history", {
        "symbols": _ensure_symbols(symbols),
        "interval": interval,
        "from": start_ts,
        "to": end_ts
    })


def get_predicted_funding_rate_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int):
    return _get("/predicted-funding-rate-history", {
        "symbols": _ensure_symbols(symbols),
        "interval": interval,
        "from": start_ts,
        "to": end_ts
    })


def get_liquidation_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int, convert_to_usd: bool = False):
    return _get("/liquidation-history", {
        "symbols": _ensure_symbols(symbols),
        "interval": interval,
        "from": start_ts,
        "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })


def get_long_short_ratio_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int):
    return _get("/long-short-ratio-history", {
        "symbols": _ensure_symbols(symbols),
        "interval": interval,
        "from": start_ts,
        "to": end_ts
    })


def get_ohlcv_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int):
    return _get("/ohlcv-history", {
        "symbols": _ensure_symbols(symbols),
        "interval": interval,
        "from": start_ts,
        "to": end_ts
    })


# Optional: try to call taker/buy-sell endpoint if it exists (best-effort)
def get_buy_sell_history(symbols: Union[str, List[str]], interval: str, start_ts: int, end_ts: int):
    """
    Best-effort: call an endpoint that may provide taker volume (buy/sell) useful for CVD.
    If Coinalyze doesn't provide it under this name, caller should catch RuntimeError.
    """
    try:
        return _get("/taker-volume-history", {
            "symbols": _ensure_symbols(symbols),
            "interval": interval,
            "from": start_ts,
            "to": end_ts
        })
    except RuntimeError:
        # Fallback: not implemented upstream
        raise NotImplementedError("No taker/buy-sell history endpoint available (try different endpoint name or provide sample JSON).")
