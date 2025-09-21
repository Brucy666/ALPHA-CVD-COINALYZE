# data_sink.py
"""
Small helpers to persist snapshots and append streaming JSONL lines.
Adjust as needed for your Railway volume or S3 path.
"""

import os
import json
import time
from glob import glob

BASE_DIR = os.getenv("DATA_DIR", "./data")
SNAPSHOT_DIR = os.path.join(BASE_DIR, "snapshots")
STREAM_DIR = os.path.join(BASE_DIR, "streams")
os.makedirs(SNAPSHOT_DIR, exist_ok=True)
os.makedirs(STREAM_DIR, exist_ok=True)

def write_snapshot(symbol: str, interval: str, pack: dict) -> str:
    ts = pack.get("fetched_at", int(time.time()))
    filename = f"{symbol.replace('/','_')}_{interval}_{ts}.json"
    path = os.path.join(SNAPSHOT_DIR, filename)
    with open(path, "w", encoding="utf8") as f:
        json.dump(pack, f, ensure_ascii=False, indent=2)
    return path

def append_jsonl(symbol: str, interval: str, pack: dict) -> str:
    filename = f"{symbol.replace('/','_')}_{interval}.jsonl"
    path = os.path.join(STREAM_DIR, filename)
    with open(path, "a", encoding="utf8") as f:
        f.write(json.dumps(pack, ensure_ascii=False) + "\n")
    return path

def retention_cleanup(max_snapshots: int = 1000, max_streams_bytes: int = 200 * 1024 * 1024):
    """Keep snapshot dir trimmed, keep streams under bytes (best-effort)."""
    try:
        snaps = sorted(glob(os.path.join(SNAPSHOT_DIR, "*.json")), key=os.path.getmtime)
        if len(snaps) > max_snapshots:
            to_delete = snaps[:len(snaps)-max_snapshots]
            for p in to_delete:
                try:
                    os.remove(p)
                except Exception:
                    pass

        # Ensure total streams size under limit (delete oldest whole files)
        streams = sorted(glob(os.path.join(STREAM_DIR, "*.jsonl")), key=os.path.getmtime)
        total = sum(os.path.getsize(p) for p in streams)
        while total > max_streams_bytes and streams:
            p = streams.pop(0)
            try:
                total -= os.path.getsize(p)
                os.remove(p)
            except Exception:
                pass
    except Exception:
        pass
