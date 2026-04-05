from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _window_stats(trades: list[dict[str, Any]]) -> tuple[float, float, float]:
    if not trades:
        return 0.0, 0.0, 0.0
    wins = sum(1 for t in trades if float(t.get("r", 0.0)) > 0)
    avg_r = sum(float(t.get("r", 0.0)) for t in trades) / len(trades)
    avg_vol = sum(float(t.get("volatility", 0.0)) for t in trades) / len(trades)
    return wins / len(trades), avg_r, avg_vol


def evaluate_drift(
    now_ts: datetime,
    trades: list[dict[str, Any]],
    config: dict[str, Any],
    system_state: dict[str, Any],
) -> dict[str, Any]:
    try:
        now = now_ts.replace(tzinfo=timezone.utc)
        last_30_cut = now - timedelta(days=30)
        prev_60_cut = now - timedelta(days=60)

        last_30: list[dict[str, Any]] = []
        prev_30: list[dict[str, Any]] = []

        for t in trades:
            ts = datetime.fromisoformat(t["timestamp"]).astimezone(timezone.utc)
            if ts >= last_30_cut:
                last_30.append(t)
            elif ts >= prev_60_cut:
                prev_30.append(t)

        w1, r1, v1 = _window_stats(last_30)
        w0, r0, v0 = _window_stats(prev_30)

        thresholds = config["drift"]
        winrate_drop = (w0 - w1) if prev_30 else 0.0
        avg_r_drop = ((r0 - r1) / abs(r0)) if prev_30 and abs(r0) > 1e-9 else 0.0
        vol_change = abs((v1 - v0) / v0) if prev_30 and abs(v0) > 1e-9 else 0.0

        drift = (
            winrate_drop >= float(thresholds["winrate_drop_threshold"])
            or avg_r_drop >= float(thresholds["avg_r_drop_threshold"])
            or vol_change >= float(thresholds["vol_change_threshold"])
        )

        if drift:
            system_state["r_multiplier"] = float(thresholds["r_reduction_factor"])
            system_state["disabled_strategies"] = list(thresholds["disable_strategies_on_drift"])
            warning = "drift_detected"
        else:
            system_state["r_multiplier"] = 1.0
            system_state["disabled_strategies"] = []
            warning = "ok"

        return {
            "checked": True,
            "warning": warning,
            "drift": drift,
            "metrics": {
                "last30_winrate": w1,
                "prev30_winrate": w0,
                "last30_avg_r": r1,
                "prev30_avg_r": r0,
                "vol_change": vol_change,
            },
        }
    except Exception as exc:  # fail-closed trigger
        return {"checked": False, "warning": f"drift_check_failed:{exc}", "drift": False, "metrics": {}}
