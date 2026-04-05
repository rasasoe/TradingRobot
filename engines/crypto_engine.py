from __future__ import annotations

from typing import Any


def run_crypto_engine(
    snapshot: dict[str, Any],
    config: dict[str, Any],
    active_symbols: list[str],
    disabled: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not bool(config.get("crypto", {}).get("enabled", True)):
        return []
    disabled = disabled or []
    if "crypto_breakout" in disabled:
        return []

    btc = snapshot["crypto"]["btc"]
    regime_on = float(btc.get("close", 0.0)) > float(btc.get("dma200", 0.0))
    if not regime_on:
        return []

    alpha_candidates: list[tuple[float, dict[str, Any]]] = []
    for sym in active_symbols:
        if sym in {"BTCUSDT", "SOLUSDT"}:
            continue
        row = snapshot["crypto"]["market"].get(sym)
        if not row:
            continue

        spread = float(row.get("orderbook", {}).get("spread_pct", 99.0))
        top3 = float(row.get("orderbook", {}).get("top3_ratio", 99.0))
        vol_ratio = float(row.get("volume", 0.0)) / max(float(row.get("avg_volume", 1.0)), 1.0)
        breakout = float(row.get("close", 0.0)) > float(row.get("breakout_level", 10**12))
        hold_ok = int(row.get("above_breakout_count", 0)) >= 2
        overheat = abs(float(row.get("candle_change_pct", 0.0))) > 4.0
        liquid_ok = spread <= 0.5 and top3 <= 0.2

        if not (breakout and hold_ok and vol_ratio > 1.05 and liquid_ok and not overheat):
            continue

        score = (vol_ratio * 50.0) - spread - abs(float(row.get("candle_change_pct", 0.0)))
        alpha_candidates.append(
            (
                score,
                {
                    "timestamp": snapshot["timestamp"],
                    "engine": "crypto",
                    "strategy": "crypto_alpha_breakout",
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
                    "reason": "crypto alpha selected",
                    "orderbook": row.get("orderbook", {}),
                },
            )
        )

    if not alpha_candidates:
        return []
    alpha_candidates.sort(key=lambda x: x[0], reverse=True)
    return [alpha_candidates[0][1]]
