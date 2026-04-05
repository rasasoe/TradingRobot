from __future__ import annotations

import json
from datetime import datetime, timezone
from statistics import mean
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen


def _http_json(url: str, timeout: float) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "TradingRobot/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _yahoo_chart(symbol: str, interval: str, range_str: str, timeout: float) -> dict[str, Any]:
    s = quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{s}?interval={interval}&range={range_str}"
    return _http_json(url, timeout)


def _extract_yahoo_bars(payload: dict[str, Any]) -> list[dict[str, float]]:
    result = payload.get("chart", {}).get("result", [])
    if not result:
        return []
    data = result[0]
    quote_data = data.get("indicators", {}).get("quote", [{}])[0]
    opens = quote_data.get("open", [])
    highs = quote_data.get("high", [])
    lows = quote_data.get("low", [])
    closes = quote_data.get("close", [])
    bars: list[dict[str, float]] = []
    for o, h, l, c in zip(opens, highs, lows, closes):
        if o is None or h is None or l is None or c is None:
            continue
        bars.append({"open": float(o), "high": float(h), "low": float(l), "close": float(c)})
    return bars


def _atr_from_bars(bars: list[dict[str, float]], period: int = 14) -> float:
    if not bars:
        return 0.0
    trs = [max(0.0, b["high"] - b["low"]) for b in bars[-period:]]
    return float(mean(trs)) if trs else 0.0


def _momentum_from_bars(bars: list[dict[str, float]]) -> float:
    if len(bars) < 2:
        return 0.0
    prev_close = bars[-2]["close"]
    cur_close = bars[-1]["close"]
    if prev_close == 0:
        return 0.0
    return (cur_close - prev_close) / prev_close


def _binance_klines(symbol: str, interval: str, limit: int, timeout: float) -> list[list[Any]]:
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = _http_json(url, timeout)
    if isinstance(data, list):
        return data
    return []


def _build_stock_market(symbols: list[str], master_ts: str, timeout: float) -> dict[str, Any]:
    market: dict[str, Any] = {}
    for sym in symbols:
        bars = _extract_yahoo_bars(_yahoo_chart(sym, "1m", "5d", timeout))
        if not bars:
            continue
        last = bars[-1]
        atr = max(0.01, _atr_from_bars(bars, 14))
        momentum = _momentum_from_bars(bars)
        market[sym] = {
            "timestamp": master_ts,
            "open": round(last["open"], 4),
            "high": round(last["high"], 4),
            "low": round(last["low"], 4),
            "close": round(last["close"], 4),
            "atr": round(atr, 4),
            "momentum": round(momentum, 6),
            "orderbook": {"top3_ratio": 0.12, "spread_pct": 0.1},
        }
    return market


def _build_spy_vix(master_ts: str, timeout: float) -> dict[str, Any]:
    spy_min_bars = _extract_yahoo_bars(_yahoo_chart("SPY", "1m", "5d", timeout))
    spy_day_bars = _extract_yahoo_bars(_yahoo_chart("SPY", "1d", "1y", timeout))
    vix_bars = _extract_yahoo_bars(_yahoo_chart("^VIX", "1d", "1mo", timeout))

    if not spy_min_bars or not spy_day_bars or not vix_bars:
        raise ValueError("failed_to_fetch_spy_vix")

    spy_close = spy_min_bars[-1]["close"]
    closes = [b["close"] for b in spy_day_bars[-50:]]
    sma50 = mean(closes) if closes else spy_close
    vix_value = vix_bars[-1]["close"]

    return {
        "timestamp": master_ts,
        "spy": {"close": round(spy_close, 4), "sma50": round(float(sma50), 4)},
        "vix": {"value": round(float(vix_value), 4)},
    }


def _build_crypto_market(symbols: list[str], master_ts: str, timeout: float) -> dict[str, Any]:
    market: dict[str, Any] = {}
    for sym in symbols:
        kl = _binance_klines(sym, "1m", 240, timeout)
        if not kl:
            continue
        closes = [float(x[4]) for x in kl]
        highs = [float(x[2]) for x in kl]
        lows = [float(x[3]) for x in kl]
        vols = [float(x[5]) for x in kl]

        close = closes[-1]
        atr = mean([max(0.0, h - l) for h, l in zip(highs[-14:], lows[-14:])]) if len(highs) >= 14 else max(1.0, close * 0.005)
        avg_vol = mean(vols[-20:]) if len(vols) >= 20 else (vols[-1] if vols else 1.0)
        breakout = max(highs[-20:-1]) if len(highs) > 20 else max(highs[:-1] or highs)
        breakdown = min(lows[-20:-1]) if len(lows) > 20 else min(lows[:-1] or lows)

        market[sym] = {
            "timestamp": master_ts,
            "close": round(close, 4),
            "atr": round(float(max(0.01, atr)), 4),
            "volume": round(float(vols[-1]), 4) if vols else 0.0,
            "avg_volume": round(float(max(1e-9, avg_vol)), 4),
            "breakout_level": round(float(breakout), 4),
            "breakdown_level": round(float(breakdown), 4),
            "orderbook": {"top3_ratio": 0.1, "spread_pct": 0.1},
        }
    return market


def _build_btc_regime(master_ts: str, timeout: float) -> dict[str, Any]:
    day = _binance_klines("BTCUSDT", "1d", 220, timeout)
    if not day:
        raise ValueError("failed_to_fetch_btc_daily")
    closes = [float(x[4]) for x in day]
    close = closes[-1]
    dma200 = mean(closes[-200:]) if len(closes) >= 200 else mean(closes)
    return {"timestamp": master_ts, "btc": {"close": round(close, 4), "dma200": round(float(dma200), 4)}}


def make_api_snapshot(timestamp: datetime, config: dict[str, Any]) -> dict[str, Any]:
    ts = timestamp.replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
    timeout = float(config.get("data", {}).get("api", {}).get("request_timeout_sec", 8.0))

    stock_symbols = list(config.get("stock", {}).get("symbols", []))
    crypto_symbols = list(config.get("crypto", {}).get("symbols", []))

    stock_market = _build_stock_market(stock_symbols, ts, timeout)
    spy_vix = _build_spy_vix(ts, timeout)
    crypto_market = _build_crypto_market(crypto_symbols, ts, timeout)
    btc_regime = _build_btc_regime(ts, timeout)

    if len(stock_market) != len(stock_symbols):
        missing = [s for s in stock_symbols if s not in stock_market]
        raise ValueError(f"missing_stock_data:{missing}")
    if len(crypto_market) != len(crypto_symbols):
        missing = [s for s in crypto_symbols if s not in crypto_market]
        raise ValueError(f"missing_crypto_data:{missing}")

    return {
        "timestamp": ts,
        "stock": {
            "timestamp": ts,
            "market": stock_market,
            "spy": spy_vix["spy"],
            "vix": spy_vix["vix"],
        },
        "crypto": {
            "timestamp": ts,
            "market": crypto_market,
            "btc": btc_regime["btc"],
        },
    }
