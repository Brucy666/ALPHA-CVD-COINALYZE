# discord_poster.py
import os
import requests
import json

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK", os.getenv("WEBHOOK_URL", "")).strip()

def post_summary(text: str, embed: dict = None) -> bool:
    """Post a compact message to Discord webhook if configured."""
    if not WEBHOOK_URL:
        return False
    payload = {"content": text}
    if embed:
        payload["embeds"] = [embed]
    r = requests.post(WEBHOOK_URL, json=payload, timeout=10)
    r.raise_for_status()
    return True

def build_embed(symbol: str, interval: str, pack: dict) -> dict:
    oi = (pack.get("snapshots",{}).get("open_interest") or [{}])[0]
    fr = (pack.get("snapshots",{}).get("funding_rate") or [{}])[0]
    cvd = pack.get("computed", {}).get("cvd")
    fields = []
    if oi: fields.append({"name":"Open Interest", "value": str(oi.get("value","?")), "inline": True})
    if fr: fields.append({"name":"Funding", "value": str(fr.get("value","?")), "inline": True})
    fields.append({"name":"Candles", "value": str(len(pack.get("history",{}).get("ohlcv",[]))), "inline": True})
    fields.append({"name":"LIQ", "value": str(len(pack.get("history",{}).get("liquidations",[]))), "inline": True})
    if cvd is not None:
        fields.append({"name":"CVD", "value": f"{cvd}", "inline": True})

    return {
        "title": f"Coinalyze • {symbol} • {interval}",
        "description": f"Live snapshot • fetched_at {pack.get('fetched_at')}",
        "fields": fields
    }
