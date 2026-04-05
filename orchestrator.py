from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.settings import load_config
from data.mock.snapshot import make_mock_snapshot, validate_snapshot_sync
from data.providers.market_data import build_api_snapshot
from engines.crypto_engine import run_crypto_engine
from engines.drift import evaluate_drift
from engines.selector_crypto import select_crypto_watchlist, should_run_crypto_selector
from engines.selector_stock import select_stock_watchlist, should_run_stock_selector
from engines.stock_engine import run_stock_engine
from execution.executor import execute_orders
from notifications.router import NotificationRouter
from ops.log_rotate import rotate_logs
from risk.enforcement import evaluate_enforcement
from risk.safe_mode import can_enter_new_position, update_safe_mode
from state.store import (
    append_jsonl,
    ensure_state_files,
    load_alert_idempotency,
    load_capital_events,
    load_idempotency,
    load_pending_orders,
    load_positions,
    load_system_state,
    load_trades,
    load_json_state,
    save_alert_idempotency,
    save_idempotency,
    save_pending_orders,
    save_portfolio,
    save_positions,
    save_system_state,
    save_trades,
    save_json_state,
)


def acquire_lock(base_dir: Path, stale_seconds: int) -> int:
    lock_path = base_dir / "state" / "run.lock"
    if lock_path.exists():
        age = datetime.now(timezone.utc).timestamp() - lock_path.stat().st_mtime
        if age > stale_seconds:
            lock_path.unlink(missing_ok=True)
    return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)


def release_lock(base_dir: Path, fd: int) -> None:
    os.close(fd)
    (base_dir / "state" / "run.lock").unlink(missing_ok=True)


def _collect_prices(snapshot: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for k, v in snapshot["stock"]["market"].items():
        out[k] = float(v["close"])
    for k, v in snapshot["crypto"]["market"].items():
        out[k] = float(v["close"])
    return out


def _build_portfolio(ts: str, positions: dict[str, Any], prices: dict[str, float]) -> dict[str, Any]:
    stock: list[dict[str, Any]] = []
    crypto: list[dict[str, Any]] = []
    for asset, pos in positions.items():
        avg = float(pos.get("avg_price", 0.0))
        qty = float(pos.get("qty", 0.0))
        px = float(prices.get(asset, avg))
        upnl = (px - avg) * qty
        if pos.get("side") == "short":
            upnl = -upnl
        row = {
            "asset": asset,
            "side": pos.get("side", "long"),
            "qty": qty,
            "avg_price": avg,
            "mark_price": px,
            "unrealized_pnl": round(upnl, 4),
            "stop_price": pos.get("stop_price", 0.0),
            "status": pos.get("status", "open"),
            "signal_type": pos.get("signal_type", "unknown"),
        }
        if pos.get("engine") == "stock":
            stock.append(row)
        else:
            crypto.append(row)
    return {"timestamp": ts, "stock": stock, "crypto": crypto, "total_positions": len(stock) + len(crypto)}


def _write_pnl_log(base_dir: Path, ts: str, positions: dict[str, Any], prices: dict[str, float]) -> bool:
    try:
        grouped: dict[tuple[str, str, str], float] = {}
        for asset, pos in positions.items():
            qty = float(pos.get("qty", 0.0))
            if qty == 0:
                continue
            avg = float(pos.get("avg_price", 0.0))
            px = float(prices.get(asset, avg))
            pnl = (px - avg) * qty
            if pos.get("side") == "short":
                pnl = -pnl
            key = (str(pos.get("engine", "unknown")), str(asset), str(pos.get("signal_type", "unknown")))
            grouped[key] = grouped.get(key, 0.0) + pnl

        if not grouped:
            append_jsonl(base_dir / "logs" / "pnl.log", {"timestamp": ts, "engine": "none", "asset": "none", "signal_type": "none", "pnl": 0.0})
            return True

        for (engine, asset, signal_type), pnl in grouped.items():
            append_jsonl(base_dir / "logs" / "pnl.log", {"timestamp": ts, "engine": engine, "asset": asset, "signal_type": signal_type, "pnl": round(pnl, 4)})
        return True
    except Exception:
        return False


def _write_pnl_log_retry(base_dir: Path, ts: str, positions: dict[str, Any], prices: dict[str, float]) -> bool:
    if _write_pnl_log(base_dir, ts, positions, prices):
        return True
    return _write_pnl_log(base_dir, ts, positions, prices)


def _performance_snapshot(base_dir: Path, ts: str, config: dict[str, Any], system_state: dict[str, Any], positions: dict[str, Any], prices: dict[str, float], trades: list[dict[str, Any]]) -> dict[str, Any]:
    initial_cash = float(config.get("capital", {}).get("initial_cash", 100000.0))
    cash = float(system_state.get("cash", initial_cash))
    realized = round(sum(float(t.get("pnl", 0.0)) for t in trades), 4)

    unrealized = 0.0
    for asset, pos in positions.items():
        qty = float(pos.get("qty", 0.0))
        avg = float(pos.get("avg_price", 0.0))
        px = float(prices.get(asset, avg))
        pnl = (px - avg) * qty
        if pos.get("side") == "short":
            pnl = -pnl
        unrealized += pnl
    unrealized = round(unrealized, 4)
    total_pnl = round(realized + unrealized, 4)
    equity = round(initial_cash + total_pnl, 4)
    ret = round((total_pnl / initial_cash) * 100, 4) if initial_cash else 0.0

    row = {
        "timestamp": ts,
        "initial_cash": initial_cash,
        "cash": round(cash, 4),
        "equity": equity,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "total_pnl": total_pnl,
        "return_pct": ret,
    }
    append_jsonl(base_dir / "logs" / "performance.log", row)
    return row


def _generate_management_signals(snapshot: dict[str, Any], positions: dict[str, Any], prices: dict[str, float]) -> list[dict[str, Any]]:
    ts = snapshot["timestamp"]
    spy = snapshot.get("stock", {}).get("spy", {})
    vix = snapshot.get("stock", {}).get("vix", {})
    stock_regime_on = float(spy.get("close", 0.0)) > float(spy.get("sma200", 0.0)) and float(vix.get("value", 99.0)) < 35.0
    btc = snapshot.get("crypto", {}).get("btc", {})
    crypto_regime_on = float(btc.get("close", 0.0)) > float(btc.get("dma200", 0.0))

    out: list[dict[str, Any]] = []
    for asset, pos in positions.items():
        px = prices.get(asset)
        if px is None:
            continue
        role = str(pos.get("role", "core")).lower()
        if role == "quarantine" or asset == "DUOL":
            continue
        side = str(pos.get("side", "long"))
        stop = float(pos.get("stop_price", 0.0))
        should_exit = (side == "long" and px <= stop) or (side == "short" and px >= stop)
        if should_exit:
            out.append(
                {
                    "timestamp": ts,
                    "engine": pos.get("engine", "unknown"),
                    "strategy": "risk_stop",
                    "asset": asset,
                    "action": "exit",
                    "side": side,
                    "signal_type": pos.get("signal_type", "risk"),
                    "regime": "risk_control",
                    "score": 1.0,
                    "atr": 0.0,
                    "price": float(px),
                    "stop_price": stop,
                    "reason": "stop_loss_triggered",
                    "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                }
            )
            continue

        avg = float(pos.get("avg_price", px))
        pnl_pct = ((px - avg) / avg) if avg else 0.0

        if asset == "NVDA" and pnl_pct <= -0.10:
            out.append(
                {
                    "timestamp": ts,
                    "engine": "stock",
                    "strategy": "leader_stop",
                    "asset": asset,
                    "action": "exit",
                    "side": side,
                    "signal_type": pos.get("signal_type", "leader"),
                    "regime": "risk_control",
                    "score": 1.0,
                    "atr": 0.0,
                    "price": float(px),
                    "stop_price": stop,
                    "reason": "NVDA -10% stop",
                    "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                }
            )
        if asset == "STX":
            if pnl_pct <= -0.10:
                out.append(
                    {
                        "timestamp": ts,
                        "engine": "stock",
                        "strategy": "leader_stop",
                        "asset": asset,
                        "action": "exit",
                        "side": side,
                        "signal_type": pos.get("signal_type", "leader"),
                        "regime": "risk_control",
                        "score": 1.0,
                        "atr": 0.0,
                        "price": float(px),
                        "stop_price": stop,
                        "reason": "STX -10% stop",
                        "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                    }
                )
            elif pnl_pct >= 0.25:
                out.append(
                    {
                        "timestamp": ts,
                        "engine": "stock",
                        "strategy": "leader_take_profit",
                        "asset": asset,
                        "action": "exit",
                        "side": side,
                        "signal_type": pos.get("signal_type", "leader"),
                        "regime": "risk_control",
                        "score": 1.0,
                        "atr": 0.0,
                        "price": float(px),
                        "stop_price": stop,
                        "reason": "STX +25% take profit",
                        "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                    }
                )

        if pos.get("engine") == "stock":
            row = snapshot.get("stock", {}).get("market", {}).get(asset, {})
            dma50 = float(row.get("dma50", 0.0))
            if not stock_regime_on and role == "leader":
                out.append(
                    {
                        "timestamp": ts,
                        "engine": "stock",
                        "strategy": "risk_off_reduce",
                        "asset": asset,
                        "action": "reduce",
                        "reduce_fraction": 0.5,
                        "side": side,
                        "signal_type": pos.get("signal_type", "leader"),
                        "regime": "risk_off",
                        "score": 1.0,
                        "atr": 0.0,
                        "price": float(px),
                        "stop_price": stop,
                        "reason": "stock risk-off leader reduce 50%",
                        "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                    }
                )
            if dma50 > 0 and px < dma50:
                if role == "leader":
                    frac = 0.5
                elif role == "core":
                    frac = 0.25
                else:
                    frac = 0.0
                if frac > 0:
                    out.append(
                        {
                            "timestamp": ts,
                            "engine": "stock",
                            "strategy": "dma50_break_reduce",
                            "asset": asset,
                            "action": "reduce",
                            "reduce_fraction": frac,
                            "side": side,
                            "signal_type": pos.get("signal_type", role),
                            "regime": "risk_control",
                            "score": 1.0,
                            "atr": 0.0,
                            "price": float(px),
                            "stop_price": stop,
                            "reason": f"{asset} below 50DMA reduce {int(frac*100)}%",
                            "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                        }
                    )
        else:
            row = snapshot.get("crypto", {}).get("market", {}).get(asset, {})
            ema20 = float(row.get("ema20", 0.0))
            if pnl_pct <= -0.08:
                out.append(
                    {
                        "timestamp": ts,
                        "engine": "crypto",
                        "strategy": "crypto_stop_8pct",
                        "asset": asset,
                        "action": "exit",
                        "side": side,
                        "signal_type": pos.get("signal_type", "crypto"),
                        "regime": "risk_control",
                        "score": 1.0,
                        "atr": 0.0,
                        "price": float(px),
                        "stop_price": stop,
                        "reason": "crypto -8% stop",
                        "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                    }
                )
            if asset == "SOLUSDT" and not crypto_regime_on:
                out.append(
                    {
                        "timestamp": ts,
                        "engine": "crypto",
                        "strategy": "btc_regime_off_reduce",
                        "asset": asset,
                        "action": "reduce",
                        "reduce_fraction": 0.5,
                        "side": side,
                        "signal_type": pos.get("signal_type", "core_crypto"),
                        "regime": "risk_off",
                        "score": 1.0,
                        "atr": 0.0,
                        "price": float(px),
                        "stop_price": stop,
                        "reason": "BTC<=200DMA SOL reduce 50%",
                        "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                    }
                )
            if ema20 > 0 and px < ema20:
                out.append(
                    {
                        "timestamp": ts,
                        "engine": "crypto",
                        "strategy": "ema20_break_reduce",
                        "asset": asset,
                        "action": "reduce",
                        "reduce_fraction": 0.5,
                        "side": side,
                        "signal_type": pos.get("signal_type", "crypto"),
                        "regime": "risk_control",
                        "score": 1.0,
                        "atr": 0.0,
                        "price": float(px),
                        "stop_price": stop,
                        "reason": "20EMA break reduce 50%",
                        "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                    }
                )
    return out


def _should_recalc_drift(system_state: dict[str, Any], config: dict[str, Any], now_ts: datetime, trades_len: int) -> bool:
    drift_cfg = config.get("drift", {})
    interval_hours = float(drift_cfg.get("check_interval_hours", 24))
    every_n = int(drift_cfg.get("check_every_n_new_trades", 20))

    last = system_state.get("drift_last_checked")
    if not last:
        return True
    last_dt = datetime.fromisoformat(str(last)).astimezone(timezone.utc)
    by_time = (now_ts - last_dt).total_seconds() >= interval_hours * 3600
    by_trades = (trades_len - int(system_state.get("drift_last_trade_count", 0))) >= every_n
    return by_time or by_trades


def _drift_state(now_ts: datetime, config: dict[str, Any], trades: list[dict[str, Any]], system_state: dict[str, Any]) -> dict[str, Any]:
    if _should_recalc_drift(system_state, config, now_ts, len(trades)):
        result = evaluate_drift(now_ts, trades, config, system_state)
        if result.get("checked", False):
            system_state["drift_last_checked"] = now_ts.isoformat()
            system_state["drift_last_warning"] = result.get("warning", "unknown")
            system_state["drift_last_trade_count"] = len(trades)
        return result

    max_age_h = float(config.get("drift", {}).get("max_status_age_hours", 48))
    last = system_state.get("drift_last_checked")
    if not last:
        return {"checked": False, "warning": "drift_not_initialized", "drift": False, "metrics": {}}
    last_dt = datetime.fromisoformat(str(last)).astimezone(timezone.utc)
    checked = (now_ts - last_dt).total_seconds() <= max_age_h * 3600
    return {
        "checked": checked,
        "warning": str(system_state.get("drift_last_warning", "unknown")),
        "drift": str(system_state.get("drift_last_warning", "")) == "drift_detected",
        "metrics": {},
    }


def _run_selectors(base_dir: Path, config: dict[str, Any], now_ts: datetime, system_state: dict[str, Any]) -> None:
    apply_mode = str(config.get("watchlist", {}).get("apply_mode", "auto"))

    if should_run_stock_selector(system_state, config, now_ts):
        picks = select_stock_watchlist(config)
        save_json_state(base_dir, "watchlist_stock_auto.json", {"timestamp": now_ts.isoformat(), "symbols": picks})
        system_state["selector_stock_last_run"] = now_ts.isoformat()
        if apply_mode == "auto":
            static = list(config.get("static_symbols_stock", []))
            merged = list(dict.fromkeys(static + picks))[: int(config.get("watchlist", {}).get("stock_max_active", 8))]
            save_json_state(base_dir, "watchlist_stock_active.json", {"timestamp": now_ts.isoformat(), "symbols": merged})

    if should_run_crypto_selector(system_state, config, now_ts):
        picks = select_crypto_watchlist(config)
        save_json_state(base_dir, "watchlist_crypto_auto.json", {"timestamp": now_ts.isoformat(), "symbols": picks})
        system_state["selector_crypto_last_run"] = now_ts.isoformat()
        if apply_mode == "auto":
            static = list(config.get("static_symbols_crypto", []))
            merged = list(dict.fromkeys(static + picks))[: int(config.get("watchlist", {}).get("crypto_max_active", 8))]
            save_json_state(base_dir, "watchlist_crypto_active.json", {"timestamp": now_ts.isoformat(), "symbols": merged})


def _active_symbols(base_dir: Path, config: dict[str, Any]) -> tuple[list[str], list[str]]:
    stock_active = load_json_state(base_dir, "watchlist_stock_active.json", {"symbols": []})
    crypto_active = load_json_state(base_dir, "watchlist_crypto_active.json", {"symbols": []})
    stock_symbols = list(stock_active.get("symbols", []))
    crypto_symbols = list(crypto_active.get("symbols", []))

    if not stock_symbols:
        stock_symbols = list(config.get("static_symbols_stock", []))[: int(config.get("watchlist", {}).get("stock_max_active", 8))]
    if not crypto_symbols:
        crypto_symbols = list(config.get("static_symbols_crypto", []))[: int(config.get("watchlist", {}).get("crypto_max_active", 8))]

    return stock_symbols, crypto_symbols


def _capital_event_block(ts: str, events: list[dict[str, Any]]) -> bool:
    key = ts[:16]
    return any(str(e.get("timestamp", ""))[:16] == key for e in events)


def _signal_alert_text(sig: dict[str, Any], status: str) -> str:
    action = sig.get("action", "unknown")
    side = sig.get("side", "unknown")
    action_kr = "매수" if action == "enter" and side == "long" else "매도" if action == "exit" else action
    return (
        "[신호 알림]\n"
        f"자산: {sig.get('asset','UNKNOWN')}\n"
        f"행동: {action_kr} ({action} {side})\n"
        f"전략: {sig.get('strategy', sig.get('signal_type','unknown'))}\n"
        f"점수: {sig.get('score',0.0)}\n"
        f"상태: {status}"
    )


def _portfolio_summary_text(portfolio: dict[str, Any], performance: dict[str, Any]) -> str:
    return (
        "[포트폴리오 현황]\n"
        f"총 포지션: {portfolio.get('total_positions',0)} (주식 {len(portfolio.get('stock', []))} / 코인 {len(portfolio.get('crypto', []))})\n"
        f"누적수익률: {performance.get('return_pct', 0.0)}%\n"
        f"총손익: {performance.get('total_pnl', 0.0)}\n"
        f"실현손익: {performance.get('realized_pnl', 0.0)}\n"
        f"미실현손익: {performance.get('unrealized_pnl', 0.0)}"
    )


def _format_qam_signal(
    mode_status: str,
    stock_regime: str,
    crypto_regime: str,
    drift_on: bool,
    portfolio: dict[str, Any],
    performance: dict[str, Any],
    allow_orders: bool,
    why: str,
) -> str:
    stock_assets = ", ".join([f"{x['asset']}:{round(float(x['qty']), 6)}" for x in portfolio.get("stock", [])]) or "NONE"
    crypto_assets = ", ".join([f"{x['asset']}:{round(float(x['qty']), 6)}" for x in portfolio.get("crypto", [])]) or "NONE"
    return (
        "[QAM SIGNAL]\n\n"
        "[STATUS]\n"
        f"MODE: {mode_status}\n"
        f"STOCK_REGIME: {stock_regime}\n"
        f"CRYPTO_REGIME: {crypto_regime}\n"
        f"DRIFT: {'ON' if drift_on else 'OFF'}\n\n"
        "[PORTFOLIO]\n"
        f"STOCK_POSITIONS: {stock_assets}\n"
        f"CRYPTO_POSITIONS: {crypto_assets}\n"
        f"TOTAL_POSITIONS: {portfolio.get('total_positions', 0)}\n"
        f"RETURN_PCT: {performance.get('return_pct', 0.0)}%\n"
        f"TOTAL_PNL: {performance.get('total_pnl', 0.0)}\n\n"
        "[EXECUTION]\n"
        f"ORDERS_ALLOWED: {'YES' if allow_orders else 'NO'}\n"
        f"WHY: {why}"
    )


def _notify(
    config: dict[str, Any],
    ts: str,
    signals: list[dict[str, Any]],
    allow_new_entries: bool,
    block_reason: str,
    reasons: list[str],
    drift_warning: str,
    alert_state: dict[str, Any],
    portfolio: dict[str, Any],
    performance: dict[str, Any],
) -> int:
    router = NotificationRouter(config)
    notif_cfg = config.get("notifications", {}).get("telegram", {})
    sent = 0
    seen = set(alert_state.get("seen", []))

    if reasons and bool(notif_cfg.get("send_system_alerts", True)):
        if router.send("[시스템 경고]\n사유: " + "|".join(reasons)):
            sent += 1

    if bool(notif_cfg.get("send_signal_alerts", True)):
        for sig in signals:
            candle_ts = str(sig.get("timestamp", ts))[:16]
            strategy = str(sig.get("strategy", sig.get("signal_type", "unknown")))
            key = f"{sig.get('asset','')}|{sig.get('side','')}|{candle_ts}|{strategy}"
            h = hashlib.sha256(key.encode("utf-8")).hexdigest()
            if h in seen:
                continue
            status = "candidate" if allow_new_entries or sig.get("action") == "exit" else f"blocked:{block_reason or 'fail_closed'}"
            if router.send(_signal_alert_text(sig, status)):
                sent += 1
                seen.add(h)

    if bool(notif_cfg.get("send_risk_alerts", True)):
        prev = str(alert_state.get("last_risk_warning_state", ""))
        cur = str(drift_warning)
        if cur != prev:
            if router.send(f"[리스크 경고]\n상태: {cur}"):
                sent += 1
            alert_state["last_risk_warning_state"] = cur

    if bool(notif_cfg.get("send_portfolio_summary", True)):
        interval_min = int(notif_cfg.get("portfolio_summary_interval_minutes", 60))
        last_iso = alert_state.get("last_portfolio_summary_ts")
        should_send = True
        if last_iso:
            last_dt = datetime.fromisoformat(str(last_iso)).astimezone(timezone.utc)
            now_dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
            should_send = (now_dt - last_dt).total_seconds() >= interval_min * 60
        if should_send:
            if router.send(_portfolio_summary_text(portfolio, performance)):
                sent += 1
                alert_state["last_portfolio_summary_ts"] = ts

    alert_state["seen"] = list(seen)
    return sent


def run_once(base_dir: Path, config_rel_path: str = "config/config.yaml", emit_signals: bool = False) -> dict[str, Any]:
    ensure_state_files(base_dir)
    config = load_config(base_dir, config_rel_path)

    # low I/O log rotation
    rotate_logs(base_dir, int(config.get("logs", {}).get("rotate_max_mb", 3)), int(config.get("logs", {}).get("rotate_keep", 3)))

    try:
        fd = acquire_lock(base_dir, int(config.get("system", {}).get("lock_stale_seconds", 180)))
    except FileExistsError:
        return {"skipped": True, "reason": "lock_busy"}

    try:
        now_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        positions = load_positions(base_dir)
        idempotency = load_idempotency(base_dir)
        alert_state = load_alert_idempotency(base_dir)
        system_state = load_system_state(base_dir)
        trades = load_trades(base_dir)
        pending_orders = load_pending_orders(base_dir)

        _run_selectors(base_dir, config, now_ts, system_state)
        stock_symbols, crypto_symbols = _active_symbols(base_dir, config)

        data_source = str(config.get("data", {}).get("source", "mock")).lower()
        snapshot: dict[str, Any]
        if data_source == "api":
            try:
                snapshot = build_api_snapshot(base_dir, now_ts, config, stock_symbols, crypto_symbols)
                data_source = "api"
            except Exception:
                if not bool(config.get("data", {}).get("fallback_to_mock_on_error", True)):
                    raise
                snapshot = make_mock_snapshot(now_ts, stock_symbols, crypto_symbols)
                data_source = "mock_fallback"
        else:
            snapshot = make_mock_snapshot(now_ts, stock_symbols, crypto_symbols)
            data_source = "mock"

        ts = snapshot["timestamp"]
        prices = _collect_prices(snapshot)

        stock_signals = run_stock_engine(snapshot, config, stock_symbols)
        crypto_signals = run_crypto_engine(snapshot, config, crypto_symbols, system_state.get("disabled_strategies", []))
        management_signals = _generate_management_signals(snapshot, positions, prices)

        drift = _drift_state(now_ts, config, trades, system_state)

        pnl_ok = _write_pnl_log_retry(base_dir, ts, positions, prices)
        if pnl_ok:
            system_state["pnl_log_fail_streak"] = 0
        else:
            system_state["pnl_log_fail_streak"] = int(system_state.get("pnl_log_fail_streak", 0)) + 1

        time_sync_ok = validate_snapshot_sync(snapshot)
        capital_event_block = _capital_event_block(ts, load_capital_events(base_dir))

        allow_from_enforcement, block_reasons, violation_reasons = evaluate_enforcement(
            time_sync_ok=time_sync_ok,
            pnl_ok=pnl_ok,
            drift_checked=bool(drift.get("checked", False)),
            capital_event_block=capital_event_block,
            pnl_fail_streak=int(system_state.get("pnl_log_fail_streak", 0)),
        )

        update_safe_mode(system_state, violation_reasons, int(config.get("system", {}).get("safe_mode_violation_streak", 3)))
        if violation_reasons:
            append_jsonl(base_dir / "logs" / "violations.log", {"timestamp": ts, "reasons": violation_reasons})

        allow_new_entries = can_enter_new_position(system_state, allow_from_enforcement)
        block_reason = "|".join(block_reasons) if block_reasons else ("SAFE_MODE" if system_state.get("safe_mode", False) else "")

        # Alpha mutual exclusion: stock alpha and crypto alpha cannot open together
        stock_alpha = [s for s in stock_signals if str(s.get("role", "")).lower() == "alpha" and s.get("action") == "enter"]
        crypto_alpha = [s for s in crypto_signals if str(s.get("role", "")).lower() == "alpha" and s.get("action") == "enter"]
        if stock_alpha and crypto_alpha:
            crypto_signals = [s for s in crypto_signals if s not in crypto_alpha]

        all_signals = stock_signals + crypto_signals + management_signals

        if not all_signals:
            append_jsonl(
                base_dir / "logs" / "decisions.log",
                {
                    "timestamp": ts,
                    "asset": "NONE",
                    "regime": "NO_ACTION",
                    "score": 0.0,
                    "ATR": 0.0,
                    "reason": "no_signal",
                    "executed": False,
                    "blocked_reason": "",
                },
            )

        positions, idempotency, trades, newly_filled = execute_orders(
            base_dir=base_dir,
            mode=str(config.get("mode", "paper")),
            signals=all_signals,
            positions=positions,
            idempotency=idempotency,
            trades=trades,
            system_state=system_state,
            prices=prices,
            config=config,
            allow_new_entries=allow_new_entries,
            block_reason=block_reason,
        )
        pending_orders.extend(newly_filled)

        system_state["last_heartbeat"] = ts

        portfolio = _build_portfolio(ts, positions, prices)
        perf = _performance_snapshot(base_dir, ts, config, system_state, positions, prices, trades)
        stock_regime = "RISK-ON" if float(snapshot.get("stock", {}).get("spy", {}).get("close", 0.0)) > float(snapshot.get("stock", {}).get("spy", {}).get("sma200", 0.0)) and float(snapshot.get("stock", {}).get("vix", {}).get("value", 99.0)) < 35.0 else "RISK-OFF"
        crypto_regime = "RISK-ON" if float(snapshot.get("crypto", {}).get("btc", {}).get("close", 0.0)) > float(snapshot.get("crypto", {}).get("btc", {}).get("dma200", 0.0)) else "RISK-OFF"
        mode_status = "SAFE MODE" if bool(system_state.get("safe_mode", False)) else ("FAIL-CLOSED" if not allow_new_entries else "NORMAL")
        qam_text = _format_qam_signal(
            mode_status=mode_status,
            stock_regime=stock_regime,
            crypto_regime=crypto_regime,
            drift_on=str(drift.get("warning", "")) == "drift_detected",
            portfolio=portfolio,
            performance=perf,
            allow_orders=allow_new_entries,
            why=block_reason or "rule_matched",
        )
        if emit_signals and all_signals:
            print(qam_text)

        telegram_sent = _notify(
            config=config,
            ts=ts,
            signals=all_signals,
            allow_new_entries=allow_new_entries,
            block_reason=block_reason,
            reasons=block_reasons,
            drift_warning=str(drift.get("warning", "unknown")),
            alert_state=alert_state,
            portfolio=portfolio,
            performance=perf,
        )

        save_positions(base_dir, positions)
        save_idempotency(base_dir, idempotency)
        save_alert_idempotency(base_dir, alert_state)
        save_trades(base_dir, trades)
        save_pending_orders(base_dir, pending_orders)
        save_portfolio(base_dir, portfolio)
        save_system_state(base_dir, system_state)

        return {
            "timestamp": ts,
            "data_source": data_source,
            "allow_new_entries": allow_new_entries,
            "safe_mode": bool(system_state.get("safe_mode", False)),
            "violation_streak": int(system_state.get("violation_streak", 0)),
            "signals": len(all_signals),
            "telegram_sent": telegram_sent,
            "drift_warning": drift.get("warning", "unknown"),
            "portfolio": portfolio,
            "performance": perf,
            "watchlist": {"stock": stock_symbols, "crypto": crypto_symbols},
            "qam_signal": qam_text if all_signals else "",
        }
    finally:
        release_lock(base_dir, fd)


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingRobot main orchestrator")
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--emit-signals", action="store_true")
    args = parser.parse_args()

    result = run_once(Path(args.base_dir).resolve(), args.config, emit_signals=args.emit_signals)
    print(json.dumps(result, ensure_ascii=False))
    if not result.get("skipped"):
        print(
            f"[요약] 신규진입허용={result['allow_new_entries']} 안전모드={result['safe_mode']} "
            f"포트폴리오수={result['portfolio']['total_positions']} 누적수익률={result['performance']['return_pct']}%"
        )


if __name__ == "__main__":
    main()
