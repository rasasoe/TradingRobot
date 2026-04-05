from __future__ import annotations

import json
from statistics import mean
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen


def _http_json(url: str, timeout: float) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "TradingRobot/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_chart(symbol: str, interval: str, range_str: str, timeout: float) -> dict[str, Any]:
    s = quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{s}?interval={interval}&range={range_str}"
    return _http_json(url, timeout)


def bars_from_chart(payload: dict[str, Any]) -> list[dict[str, float]]:
    result = payload.get("chart", {}).get("result", [])
    if not result:
        return []
    q = result[0].get("indicators", {}).get("quote", [{}])[0]
    bars: list[dict[str, float]] = []
    for o, h, l, c, v in zip(q.get("open", []), q.get("high", []), q.get("low", []), q.get("close", []), q.get("volume", [])):
        if o is None or h is None or l is None or c is None:
            continue
        bars.append({"open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v or 0.0)})
    return bars


def sma(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    chunk = values[-period:] if len(values) >= period else values
    return float(mean(chunk))


def atr(bars: list[dict[str, float]], period: int = 14) -> float:
    if not bars:
        return 0.0
    trs = [max(0.0, b["high"] - b["low"]) for b in bars[-period:]]
    return float(mean(trs)) if trs else 0.0
