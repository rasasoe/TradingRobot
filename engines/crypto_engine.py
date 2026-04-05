from __future__ import annotations

from typing import Any


def run_crypto_engine(snapshot: dict[str, Any], config: dict[str, Any], disabled: list[str] | None = None) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if not config["crypto"]["enabled"]:
        return signals

    disabled = disabled or []
    if "crypto_breakout" in disabled:
        return signals

    ts = snapshot["timestamp"]
    btc = snapshot["crypto"]["btc"]
    regime = "bull" if btc["close"] > btc["dma200"] else "bear"

    for asset, bar in snapshot["crypto"]["market"].items():
        vol_ratio = float(bar["volume"]) / float(bar["avg_volume"]) if bar["avg_volume"] else 0.0
        long_breakout = bar["close"] > bar["breakout_level"] and vol_ratio > 1.05
        short_breakdown = bar["close"] < bar["breakdown_level"] and vol_ratio > 1.05

        if regime == "bull" and long_breakout:
            signals.append(
                {
                    "timestamp": ts,
                    "engine": "crypto",
                    "strategy": "crypto_breakout",
                    "asset": asset,
                    "action": "enter",
                    "side": "long",
                    "signal_type": "breakout",
                    "regime": regime,
                    "score": 0.9,
                    "atr": bar["atr"],
                    "price": bar["close"],
                    "stop_price": round(bar["close"] - (2.0 * bar["atr"]), 4),
                    "reason": f"regime={regime};vol_ratio={vol_ratio:.2f}",
                    "orderbook": bar["orderbook"],
                }
            )

        if regime == "bear" and config["crypto"]["allow_short"] and short_breakdown:
            signals.append(
                {
                    "timestamp": ts,
                    "engine": "crypto",
                    "strategy": "crypto_breakout",
                    "asset": asset,
                    "action": "enter",
                    "side": "short",
                    "signal_type": "breakdown",
                    "regime": regime,
                    "score": 0.85,
                    "atr": bar["atr"],
                    "price": bar["close"],
                    "stop_price": round(bar["close"] + (2.0 * bar["atr"]), 4),
                    "reason": f"regime={regime};vol_ratio={vol_ratio:.2f}",
                    "orderbook": bar["orderbook"],
                }
            )

    return signals
