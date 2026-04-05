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
