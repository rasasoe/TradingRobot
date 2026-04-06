from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
from typing import Any

from data.cache.store import get_cached, set_cached
from data.providers import binance, yahoo


def _momentum(bars: list[dict[str, float]]) -> float:
    if len(bars) < 2:
        return 0.0
    prev_c = bars[-2]["close"]
    cur_c = bars[-1]["close"]
    if prev_c == 0:
        return 0.0
    return (cur_c - prev_c) / prev_c


def _safe_ratio(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return a / b


def _base_snapshot(ts: str) -> dict[str, Any]:
    return {
        "timestamp": ts,
        "stock": {
            "timestamp": ts,
            "market": {},
            "spy": {"close": 0.0, "sma50": 0.0, "sma200": 0.0, "momentum": 0.0},
            "vix": {"value": 0.0, "prev": 0.0},
        },
        "crypto": {"timestamp": ts, "market": {}, "btc": {"close": 0.0, "dma200": 0.0}},
    }


def build_api_snapshot(base_dir, now_ts: datetime, config: dict[str, Any], stock_symbols: list[str], crypto_symbols: list[str]) -> dict[str, Any]:
    ts = now_ts.replace(second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
    timeout = float(config.get("data", {}).get("api", {}).get("request_timeout_sec", 8.0))

    snap = _base_snapshot(ts)

    # Stock market data
    for sym in stock_symbols:
        key_1m = f"stock:{sym}:1m"
        bars = get_cached(base_dir, key_1m, ts)
        if bars is None:
            bars = yahoo.bars_from_chart(yahoo.get_chart(sym, "1m", "5d", timeout))
            set_cached(base_dir, key_1m, ts, bars)
        key_1d = f"stock:{sym}:1d"
        day_bars = get_cached(base_dir, key_1d, ts)
        if day_bars is None:
            day_bars = yahoo.bars_from_chart(yahoo.get_chart(sym, "1d", "1y", timeout))
            set_cached(base_dir, key_1d, ts, day_bars)
        if not bars:
            raise ValueError(f"missing_stock:{sym}")
        if not day_bars or len(day_bars) < 210:
            raise ValueError(f"missing_stock_daily:{sym}")
        last = bars[-1]
        atr = max(0.01, yahoo.atr(bars, 14))
        day_closes = [x["close"] for x in day_bars]
        day_vols = [x["volume"] for x in day_bars]
        dma50 = yahoo.sma(day_closes, 50)
        dma200 = yahoo.sma(day_closes, 200)
        ret5 = _safe_ratio(day_closes[-1] - day_closes[-6], day_closes[-6]) if len(day_closes) >= 6 else 0.0
        recent_high20 = max(day_closes[-21:-1]) if len(day_closes) >= 21 else max(day_closes)
        recent_low20 = min(day_closes[-21:-1]) if len(day_closes) >= 21 else min(day_closes)
        volume = day_vols[-1] if day_vols else 0.0
        avg_volume = mean(day_vols[-20:]) if len(day_vols) >= 20 else max(volume, 1.0)
        snap["stock"]["market"][sym] = {
            "timestamp": ts,
            "open": round(last["open"], 4),
            "high": round(last["high"], 4),
            "low": round(last["low"], 4),
            "close": round(last["close"], 4),
            "atr": round(atr, 4),
            "momentum": round(_momentum(bars), 6),
            "dma50": round(dma50, 4),
            "dma200": round(dma200, 4),
            "ret5": round(ret5, 6),
            "recent_high20": round(recent_high20, 4),
            "recent_low20": round(recent_low20, 4),
            "volume": round(volume, 4),
            "avg_volume": round(float(max(avg_volume, 1e-9)), 4),
            "orderbook": {"top3_ratio": 0.12, "spread_pct": 0.1},
        }

    # SPY + VIX regime
    spy_min = yahoo.bars_from_chart(yahoo.get_chart("SPY", "1m", "5d", timeout))
    spy_day = yahoo.bars_from_chart(yahoo.get_chart("SPY", "1d", "1y", timeout))
    vix_day = yahoo.bars_from_chart(yahoo.get_chart("^VIX", "1d", "1mo", timeout))
    if not spy_min or not spy_day or not vix_day:
        raise ValueError("missing_spy_vix")
    spy_close = spy_min[-1]["close"]
    spy_sma50 = yahoo.sma([b["close"] for b in spy_day], 50)
    spy_sma200 = yahoo.sma([b["close"] for b in spy_day], 200)
    vix_val = vix_day[-1]["close"]
    vix_prev = vix_day[-2]["close"] if len(vix_day) >= 2 else vix_val
    snap["stock"]["spy"] = {
        "close": round(spy_close, 4),
        "sma50": round(spy_sma50, 4),
        "sma200": round(spy_sma200, 4),
        "momentum": round(_momentum(spy_min), 6),
    }
    snap["stock"]["vix"] = {"value": round(vix_val, 4), "prev": round(vix_prev, 4)}

    # Crypto market data
    breakout_lookback = int(config.get("backfill", {}).get("breakout_lookback_bars", 20))
    for sym in crypto_symbols:
        key = f"crypto:{sym}:1m"
        kl = get_cached(base_dir, key, ts)
        if kl is None:
            kl = binance.klines(sym, "1m", 240, timeout)
            set_cached(base_dir, key, ts, kl)
        if not kl:
            raise ValueError(f"missing_crypto:{sym}")
        if len(kl) < 25:
            raise ValueError(f"insufficient_crypto:{sym}")

        # Closed-candle only: drop the latest forming candle.
        closed = kl[:-1]
        closes = [float(x[4]) for x in closed]
        highs = [float(x[2]) for x in closed]
        lows = [float(x[3]) for x in closed]
        vols = [float(x[5]) for x in closed]
        close_time = int(closed[-1][6])

        close = closes[-1]
        atr = mean([max(0.0, h - l) for h, l in zip(highs[-14:], lows[-14:])]) if len(highs) >= 14 else max(1.0, close * 0.01)
        avg_vol = mean(vols[-20:]) if len(vols) >= 20 else vols[-1]
        lb = max(2, breakout_lookback)
        breakout = max(highs[-lb:-1]) if len(highs) > lb else max(highs)
        breakdown = min(lows[-lb:-1]) if len(lows) > lb else min(lows)
        ema20 = closes[0]
        alpha = 2.0 / 21.0
        for c in closes[1:]:
            ema20 = (c * alpha) + (ema20 * (1 - alpha))
        prev_close = closes[-2] if len(closes) >= 2 else closes[-1]
        candle_chg = _safe_ratio(closes[-1] - prev_close, prev_close) if prev_close else 0.0
        above_breakout_count = 0
        for c in closes[-4:-1]:
            if c > breakout:
                above_breakout_count += 1

        liq = binance.orderbook_liquidity(sym, timeout, limit=20)
        snap["crypto"]["market"][sym] = {
            "timestamp": ts,
            "close_time": close_time,
            "close": round(close, 4),
            "atr": round(float(max(0.01, atr)), 4),
            "volume": round(float(vols[-1]), 4),
            "avg_volume": round(float(max(avg_vol, 1e-9)), 4),
            "breakout_level": round(float(breakout), 4),
            "breakdown_level": round(float(breakdown), 4),
            "ema20": round(float(ema20), 4),
            "prev_close": round(float(prev_close), 4),
            "candle_change_pct": round(float(candle_chg * 100), 4),
            "above_breakout_count": int(above_breakout_count),
            "orderbook": {
                "best_bid": round(float(liq["best_bid"]), 8),
                "best_ask": round(float(liq["best_ask"]), 8),
                "midpoint": round(float(liq["midpoint"]), 8),
                "spread_pct": round(float(liq["spread_pct"]), 6),
                "top3_ratio": round(float(liq["top3_ratio"]), 6),
            },
        }

    btc_daily = binance.klines("BTCUSDT", "1d", int(config.get("backfill", {}).get("crypto_dma_days", 220)), timeout)
    if not btc_daily:
        raise ValueError("missing_btc_daily")
    if len(btc_daily) < 205:
        raise ValueError("insufficient_btc_daily")
    # Closed-candle only for regime.
    btc_closed = btc_daily[:-1]
    btc_closes = [float(x[4]) for x in btc_closed]
    btc_close = btc_closes[-1]
    btc_close_time = int(btc_closed[-1][6])
    dma_len = min(200, len(btc_closes))
    btc_dma = mean(btc_closes[-dma_len:])
    snap["crypto"]["btc"] = {"close": round(btc_close, 4), "dma200": round(float(btc_dma), 4), "close_time": btc_close_time}

    return snap
