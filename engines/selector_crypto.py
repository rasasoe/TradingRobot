from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from data.providers import binance

_EXCLUDED_BASES = {
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USD1",
    "USDE",
}


def _base_asset(symbol: str, quote: str) -> str:
    return symbol[: -len(quote)] if symbol.endswith(quote) else symbol


def _spread_pct(symbol: str, timeout: float) -> float:
    book = binance.book_ticker(symbol, timeout)
    bid = float(book.get("bidPrice", 0.0) or 0.0)
    ask = float(book.get("askPrice", 0.0) or 0.0)
    if bid <= 0 or ask <= 0:
        return 100.0
    mid = (bid + ask) / 2
    return ((ask - bid) / mid) * 100


def select_crypto_watchlist(config: dict[str, Any]) -> list[str]:
    timeout = float(config.get("data", {}).get("api", {}).get("request_timeout_sec", 8.0))
    quote = str(config.get("selector", {}).get("crypto_quote", "USDT"))
    min_qv = float(config.get("selector", {}).get("crypto_min_quote_volume", 50_000_000))
    limit = int(config.get("watchlist", {}).get("crypto_max_active", 8))

    rows = binance.ticker_24h(timeout)
    ranked: list[tuple[float, str]] = []

    for row in rows:
        symbol = str(row.get("symbol", ""))
        if not symbol.endswith(quote):
            continue
        base = _base_asset(symbol, quote)
        if base.upper() in _EXCLUDED_BASES:
            continue
        qv = float(row.get("quoteVolume", 0.0) or 0.0)
        if qv < min_qv:
            continue
        spread = _spread_pct(symbol, timeout)
        if spread > float(config.get("execution", {}).get("spread_limit_pct", 0.5)):
            continue
        change = abs(float(row.get("priceChangePercent", 0.0) or 0.0))
        score = (qv / 1_000_000.0) + change - spread
        ranked.append((score, symbol))

    ranked.sort(reverse=True)
    picks = [sym for _, sym in ranked[:limit]]
    return picks


def should_run_crypto_selector(system_state: dict[str, Any], config: dict[str, Any], now_ts: datetime) -> bool:
    last = system_state.get("selector_crypto_last_run")
    if not last:
        return True
    last_dt = datetime.fromisoformat(str(last)).astimezone(timezone.utc)
    interval_hours = float(config.get("watchlist", {}).get("crypto_selector_hours", 4))
    return (now_ts - last_dt).total_seconds() >= interval_hours * 3600
