from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from engines.session_guard import is_stock_market_open


NY = ZoneInfo("America/New_York")
LEADER_SET = {"NVDA", "STX"}
BLOCKED_SET = {"DUOL"}
CORE_SET = {"SPY"}


def _is_eod(ts_iso: str) -> bool:
    ts = datetime.fromisoformat(ts_iso).astimezone(NY)
    return ts.hour == 16 and ts.minute == 0


def _common_entry_ok(row: dict[str, Any]) -> bool:
    close = float(row.get("close", 0.0))
    dma50 = float(row.get("dma50", 0.0))
    dma200 = float(row.get("dma200", 0.0))
    spread = float(row.get("orderbook", {}).get("spread_pct", 99.0))
    vol = float(row.get("volume", 0.0))
    avg_vol = float(row.get("avg_volume", 0.0))
    return close > dma50 and dma50 > dma200 and vol > avg_vol and spread <= 0.5


def run_stock_engine(snapshot: dict[str, Any], config: dict[str, Any], active_symbols: list[str]) -> list[dict[str, Any]]:
    if not bool(config.get("stock", {}).get("enabled", True)):
        return []

    ts = snapshot["timestamp"]
    if not is_stock_market_open(ts):
        return []
    if not _is_eod(ts):
        return []

    spy = snapshot["stock"]["spy"]
    vix = snapshot["stock"]["vix"]
    regime_on = float(spy.get("close", 0.0)) > float(spy.get("sma200", 0.0)) and float(vix.get("value", 99.0)) < 35.0
    if not regime_on:
        return []

    signals: list[dict[str, Any]] = []

    # SPY Core entry only
    spy_row = snapshot["stock"]["market"].get("SPY")
    if spy_row and _common_entry_ok(spy_row):
        pullback = float(spy_row.get("ret5", 0.0)) <= -0.03
        breakout = float(spy_row.get("close", 0.0)) > float(spy_row.get("recent_high20", 10**12))
        vix_comp = float(vix.get("prev", 0.0)) > 25.0 and float(vix.get("value", 0.0)) < 20.0
        if pullback or breakout or vix_comp:
            signals.append(
                {
                    "timestamp": ts,
                    "engine": "stock",
                    "strategy": "stock_core_spy",
                    "asset": "SPY",
                    "action": "enter",
                    "side": "long",
                    "signal_type": "core_entry",
                    "role": "core",
                    "target_r": 1.0,
                    "regime": "risk_on",
                    "score": 1.0,
                    "atr": float(spy_row.get("atr", 1.0)),
                    "price": float(spy_row.get("close", 0.0)),
                    "stop_price": round(float(spy_row.get("close", 0.0)) * 0.92, 4),
                    "reason": "SPY core signal",
                    "orderbook": spy_row.get("orderbook", {}),
                }
            )

    # Alpha scanner (single)
    alpha_candidates: list[tuple[float, dict[str, Any]]] = []
    spy_mom = float(spy.get("momentum", 0.0))
    for sym in active_symbols:
        if sym in CORE_SET or sym in LEADER_SET or sym in BLOCKED_SET:
            continue
        row = snapshot["stock"]["market"].get(sym)
        if not row or not _common_entry_ok(row):
            continue
        rs = float(row.get("momentum", 0.0)) - spy_mom
        if rs <= 0:
            continue
        score = (rs * 100.0) + (float(row.get("volume", 0.0)) / max(float(row.get("avg_volume", 1.0)), 1.0))
        alpha_candidates.append(
            (
                score,
                {
                    "timestamp": ts,
                    "engine": "stock",
                    "strategy": "stock_alpha_scanner",
                    "asset": sym,
                    "action": "enter",
                    "side": "long",
                    "signal_type": "alpha_entry",
                    "role": "alpha",
                    "target_r": 0.5,
                    "regime": "risk_on",
                    "score": round(score, 4),
                    "atr": float(row.get("atr", 1.0)),
                    "price": float(row.get("close", 0.0)),
                    "stop_price": round(float(row.get("close", 0.0)) * 0.92, 4),
                    "reason": "stock alpha selected",
                    "orderbook": row.get("orderbook", {}),
                },
            )
        )
    if alpha_candidates:
        alpha_candidates.sort(key=lambda x: x[0], reverse=True)
        signals.append(alpha_candidates[0][1])

    return signals
