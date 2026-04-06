from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen


def _http_json(url: str, timeout: float) -> Any:
    req = Request(url, headers={"User-Agent": "TradingRobot/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def klines(symbol: str, interval: str, limit: int, timeout: float) -> list[list[Any]]:
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = _http_json(url, timeout)
    return data if isinstance(data, list) else []


def ticker_24h(timeout: float) -> list[dict[str, Any]]:
    url = "https://api.binance.com/api/v3/ticker/24hr"
    data = _http_json(url, timeout)
    return data if isinstance(data, list) else []


def book_ticker(symbol: str, timeout: float) -> dict[str, Any]:
    url = f"https://api.binance.com/api/v3/ticker/bookTicker?symbol={symbol}"
    data = _http_json(url, timeout)
    return data if isinstance(data, dict) else {}


def orderbook_liquidity(symbol: str, timeout: float, limit: int = 20) -> dict[str, float]:
    import ccxt

    ex = ccxt.binance({"enableRateLimit": True, "timeout": int(timeout * 1000)})
    ob = ex.fetch_order_book(symbol, limit=max(20, limit))
    bids = ob.get("bids", []) or []
    asks = ob.get("asks", []) or []
    if not bids or not asks:
        return {
            "best_bid": 0.0,
            "best_ask": 0.0,
            "midpoint": 0.0,
            "spread_pct": 100.0,
            "top3_ratio": 1.0,
        }
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    midpoint = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else 0.0
    spread_pct = ((best_ask - best_bid) / midpoint) * 100 if midpoint > 0 else 100.0
    n = min(max(20, limit), len(bids), len(asks))
    top3_sum = sum(float(x[1]) for x in bids[:3]) + sum(float(x[1]) for x in asks[:3])
    topn_sum = sum(float(x[1]) for x in bids[:n]) + sum(float(x[1]) for x in asks[:n])
    top3_ratio = (top3_sum / topn_sum) if topn_sum > 0 else 1.0
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": midpoint,
        "spread_pct": spread_pct,
        "top3_ratio": top3_ratio,
    }
