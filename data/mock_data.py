from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _seed(ts: datetime, key: str) -> int:
    return abs(hash(f"{ts.isoformat()}::{key}")) % 100000


def _price_from_seed(base: float, s: int, scale: float) -> float:
    return round(base + ((s % 2000) - 1000) * scale, 4)


def make_mock_snapshot(timestamp: datetime, config: dict[str, Any]) -> dict[str, Any]:
    ts = timestamp.replace(second=0, microsecond=0, tzinfo=timezone.utc)

    stock_symbols = config["stock"]["symbols"]
    crypto_symbols = config["crypto"]["symbols"]

    stock_market: dict[str, Any] = {}
    for sym in stock_symbols:
        s = _seed(ts, sym)
        close = _price_from_seed(180.0 if sym == "AAPL" else 390.0, s, 0.02)
        atr = round(max(0.3, close * 0.01), 4)
        stock_market[sym] = {
            "timestamp": ts.isoformat(),
            "open": round(close * 0.998, 4),
            "high": round(close * 1.005, 4),
            "low": round(close * 0.995, 4),
            "close": close,
            "atr": atr,
            "momentum": ((s % 200) - 100) / 100,
            "orderbook": {"top3_ratio": 0.12 + ((s % 6) * 0.01), "spread_pct": 0.08 + ((s % 4) * 0.05)},
        }

    spy_seed = _seed(ts, "SPY")
    vix_seed = _seed(ts, "VIX")
    spy_close = _price_from_seed(520.0, spy_seed, 0.03)
    vix_value = _price_from_seed(18.0, vix_seed, 0.01)

    crypto_market: dict[str, Any] = {}
    for sym in crypto_symbols:
        s = _seed(ts, sym)
        base = 65000.0 if sym == "BTCUSDT" else 3200.0
        close = _price_from_seed(base, s, 0.6 if sym == "BTCUSDT" else 0.08)
        crypto_market[sym] = {
            "timestamp": ts.isoformat(),
            "close": close,
            "atr": round(max(5.0, close * 0.012), 4),
            "volume": 1000 + (s % 500),
            "avg_volume": 1000,
            "breakout_level": round(close * 0.997, 4),
            "breakdown_level": round(close * 1.003, 4),
            "orderbook": {"top3_ratio": 0.1 + ((s % 8) * 0.01), "spread_pct": 0.12 + ((s % 3) * 0.07)},
        }

    btc_close = crypto_market["BTCUSDT"]["close"] if "BTCUSDT" in crypto_market else 65000.0

    return {
        "timestamp": ts.isoformat(),
        "stock": {
            "timestamp": ts.isoformat(),
            "market": stock_market,
            "spy": {"close": spy_close, "sma50": round(spy_close * 0.995, 4)},
            "vix": {"value": vix_value},
        },
        "crypto": {
            "timestamp": ts.isoformat(),
            "market": crypto_market,
            "btc": {"close": btc_close, "dma200": round(btc_close * 0.99, 4)},
        },
    }


def validate_snapshot_sync(snapshot: dict[str, Any]) -> bool:
    master = snapshot["timestamp"]
    if snapshot["stock"]["timestamp"] != master:
        return False
    if snapshot["crypto"]["timestamp"] != master:
        return False
    for payload in snapshot["stock"]["market"].values():
        if payload["timestamp"] != master:
            return False
    for payload in snapshot["crypto"]["market"].values():
        if payload["timestamp"] != master:
            return False
    return True
