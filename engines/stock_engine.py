from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any


def _is_blocked_opening_window(ts_iso: str, no_trade_minutes: int) -> bool:
    ts = datetime.fromisoformat(ts_iso).astimezone(ZoneInfo("America/New_York"))
    market_open = ts.replace(hour=9, minute=30, second=0, microsecond=0)
    delta_min = (ts - market_open).total_seconds() / 60
    return 0 <= delta_min < no_trade_minutes


def run_stock_engine(snapshot: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if not config["stock"]["enabled"]:
        return signals

    ts = snapshot["timestamp"]
    spy = snapshot["stock"]["spy"]
    vix = snapshot["stock"]["vix"]["value"]
    regime = "risk_on" if spy["close"] > spy["sma50"] else "risk_off"

    blocked_open = _is_blocked_opening_window(ts, int(config["stock"]["no_trade_minutes_after_open"]))

    for asset, bar in snapshot["stock"]["market"].items():
        reason_parts = [f"regime={regime}", f"vix={vix:.2f}", f"mom={bar['momentum']:.3f}"]
        score = 0.0

        if regime == "risk_on":
            score += 0.5
        if vix <= float(config["stock"]["max_vix"]):
            score += 0.25
        if float(bar["momentum"]) > 0.15:
            score += 0.25

        if blocked_open:
            reason_parts.append("blocked:first_30min")
            continue

        if score >= 0.75:
            signals.append(
                {
                    "timestamp": ts,
                    "engine": "stock",
                    "strategy": "stock_regime_momentum",
                    "asset": asset,
                    "action": "enter",
                    "side": "long",
                    "signal_type": "momentum",
                    "regime": regime,
                    "score": round(score, 4),
                    "atr": bar["atr"],
                    "price": bar["close"],
                    "stop_price": round(bar["close"] - (1.5 * bar["atr"]), 4),
                    "reason": ";".join(reason_parts),
                    "orderbook": bar["orderbook"],
                }
            )
    return signals
