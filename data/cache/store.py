from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def _cache_path(base_dir: Path, key: str) -> Path:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
    return base_dir / "data" / "cache" / f"{h}.json"


def get_cached(base_dir: Path, key: str, candle_ts: str) -> Any | None:
    path = _cache_path(base_dir, key)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            row = json.load(f)
        if row.get("candle_ts") == candle_ts:
            return row.get("payload")
    except Exception:
        return None
    return None


def set_cached(base_dir: Path, key: str, candle_ts: str, payload: Any) -> None:
    path = _cache_path(base_dir, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"candle_ts": candle_ts, "payload": payload}, f, ensure_ascii=True)
