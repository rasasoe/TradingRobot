from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
from typing import Any

from data.providers import yahoo


def select_stock_watchlist(config: dict[str, Any]) -> list[str]:
    timeout = float(config.get("data", {}).get("api", {}).get("request_timeout_sec", 8.0))
    universe = list(config.get("selector", {}).get("stock_universe", []))
    limit = int(config.get("watchlist", {}).get("stock_max_active", 8))

    ranked: list[tuple[float, str]] = []
    for sym in universe:
        try:
            bars = yahoo.bars_from_chart(yahoo.get_chart(sym, "1d", "6mo", timeout))
            if len(bars) < 30:
                continue
            closes = [b["close"] for b in bars]
            vols = [b["volume"] for b in bars]
            ret20 = (closes[-1] - closes[-20]) / closes[-20] if closes[-20] else 0.0
            vol20 = mean([abs(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(len(closes) - 19, len(closes)) if closes[i - 1]])
            avg_vol = mean(vols[-20:]) if vols else 0.0
            score = (ret20 * 100.0) + (vol20 * 10.0) + (avg_vol / 1_000_000.0)
            ranked.append((score, sym))
        except Exception:
            continue

    ranked.sort(reverse=True)
    picks = [sym for _, sym in ranked[:limit]]
    return picks


def should_run_stock_selector(system_state: dict[str, Any], config: dict[str, Any], now_ts: datetime) -> bool:
    last = system_state.get("selector_stock_last_run")
    if not last:
        return True
    last_dt = datetime.fromisoformat(str(last)).astimezone(timezone.utc)
    interval_hours = float(config.get("watchlist", {}).get("stock_selector_hours", 24))
    return (now_ts - last_dt).total_seconds() >= interval_hours * 3600
