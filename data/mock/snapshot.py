from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _seed(ts: datetime, key: str) -> int:
    return abs(hash(f"{ts.isoformat()}::{key}")) % 100000


def _price(base: float, s: int, scale: float) -> float:
    return round(base + ((s % 2000) - 1000) * scale, 4)


def make_mock_snapshot(now_ts: datetime, stock_symbols: list[str], crypto_symbols: list[str]) -> dict[str, Any]:
    ts = now_ts.replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()

    stock_market: dict[str, Any] = {}
    for sym in stock_symbols:
        s = _seed(now_ts, sym)
        close = _price(180.0 if sym != "MSFT" else 390.0, s, 0.03)
        atr = max(0.1, close * 0.01)
        stock_market[sym] = {
            "timestamp": ts,
            "open": round(close * 0.998, 4),
            "high": round(close * 1.005, 4),
            "low": round(close * 0.995, 4),
            "close": close,
            "atr": round(atr, 4),
            "momentum": round(((s % 200) - 100) / 100, 6),
            "dma50": round(close * 0.985, 4),
            "dma200": round(close * 0.96, 4),
            "ret5": round(((s % 40) - 20) / 1000, 6),
            "recent_high20": round(close * 1.02, 4),
            "recent_low20": round(close * 0.98, 4),
            "volume": 1_000_000 + (s % 500_000),
            "avg_volume": 900_000 + (s % 300_000),
            "orderbook": {"top3_ratio": 0.12, "spread_pct": 0.1},
        }

    spy_close = _price(520.0, _seed(now_ts, "SPY"), 0.03)
    vix_val = _price(18.0, _seed(now_ts, "VIX"), 0.02)

    crypto_market: dict[str, Any] = {}
    for sym in crypto_symbols:
        s = _seed(now_ts, sym)
        base = 65000.0 if "BTC" in sym else 3200.0
        close = _price(base, s, 0.7 if "BTC" in sym else 0.09)
        atr = max(0.5, close * 0.012)
        prev_close = round(close * 0.998, 4)
        crypto_market[sym] = {
            "timestamp": ts,
            "close": round(close, 4),
            "atr": round(atr, 4),
            "volume": 1000 + (s % 400),
            "avg_volume": 1000,
            "breakout_level": round(close * 0.997, 4),
            "breakdown_level": round(close * 1.003, 4),
            "ema20": round(close * 0.996, 4),
            "prev_close": prev_close,
            "candle_change_pct": round(((close - prev_close) / prev_close) * 100, 4),
            "above_breakout_count": 3,
            "orderbook": {"top3_ratio": 0.1, "spread_pct": 0.1},
        }

    btc_close = crypto_market.get("BTCUSDT", {"close": 65000.0})["close"]

    return {
        "timestamp": ts,
        "stock": {
            "timestamp": ts,
            "market": stock_market,
            "spy": {"close": spy_close, "sma50": round(spy_close * 0.995, 4), "sma200": round(spy_close * 0.97, 4), "momentum": 0.01},
            "vix": {"value": vix_val, "prev": round(vix_val + 1.5, 4)},
        },
        "crypto": {"timestamp": ts, "market": crypto_market, "btc": {"close": btc_close, "dma200": round(btc_close * 0.99, 4)}},
    }


def validate_snapshot_sync(snapshot: dict[str, Any]) -> bool:
    master = snapshot.get("timestamp")
    if snapshot.get("stock", {}).get("timestamp") != master:
        return False
    if snapshot.get("crypto", {}).get("timestamp") != master:
        return False
    for row in snapshot.get("stock", {}).get("market", {}).values():
        if row.get("timestamp") != master:
            return False
    for row in snapshot.get("crypto", {}).get("market", {}).values():
        if row.get("timestamp") != master:
            return False
    return True
