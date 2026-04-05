from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import load_config
from data.mock.snapshot import make_mock_snapshot
from data.providers.market_data import build_api_snapshot
from execution.executor import execute_orders
from notifications.router import NotificationRouter
from state.store import (
    ensure_state_files,
    load_alert_idempotency,
    load_idempotency,
    load_pending_orders,
    load_positions,
    load_system_state,
    load_trades,
    save_alert_idempotency,
    save_idempotency,
    save_pending_orders,
    save_portfolio,
    save_positions,
    save_system_state,
    save_trades,
)


def acquire_fast_lock(base_dir: Path, stale_seconds: int) -> int:
    lock_path = base_dir / "state" / "fast.lock"
    if lock_path.exists():
        age = datetime.now(timezone.utc).timestamp() - lock_path.stat().st_mtime
        if age > stale_seconds:
            lock_path.unlink(missing_ok=True)
    return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)


def release_fast_lock(base_dir: Path, fd: int) -> None:
    os.close(fd)
    (base_dir / "state" / "fast.lock").unlink(missing_ok=True)


def _collect_prices(snapshot: dict[str, Any]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for asset, row in snapshot.get("stock", {}).get("market", {}).items():
        prices[asset] = float(row["close"])
    for asset, row in snapshot.get("crypto", {}).get("market", {}).items():
        prices[asset] = float(row["close"])
    return prices


def _build_portfolio(ts: str, positions: dict[str, Any], prices: dict[str, float]) -> dict[str, Any]:
    stock: list[dict[str, Any]] = []
    crypto: list[dict[str, Any]] = []

    for asset, pos in positions.items():
        qty = float(pos.get("qty", 0.0))
        avg = float(pos.get("avg_price", 0.0))
        mark = float(prices.get(asset, avg))
        pnl = (mark - avg) * qty
        if pos.get("side") == "short":
            pnl = -pnl

        row = {
            "asset": asset,
            "side": pos.get("side", "long"),
            "qty": qty,
            "avg_price": avg,
            "mark_price": mark,
            "unrealized_pnl": round(pnl, 4),
            "stop_price": pos.get("stop_price", 0.0),
            "status": pos.get("status", "open"),
            "signal_type": pos.get("signal_type", "unknown"),
        }
        if pos.get("engine") == "stock":
            stock.append(row)
        else:
            crypto.append(row)

    return {
        "timestamp": ts,
        "stock": stock,
        "crypto": crypto,
        "total_positions": len(stock) + len(crypto),
    }


def _management_signals(ts: str, positions: dict[str, Any], prices: dict[str, float]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for asset, pos in positions.items():
        mark = prices.get(asset)
        if mark is None:
            continue
        side = str(pos.get("side", "long"))
        stop = float(pos.get("stop_price", 0.0))
        hit = (side == "long" and mark <= stop) or (side == "short" and mark >= stop)
        if not hit:
            continue
        signals.append(
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
                "price": float(mark),
                "stop_price": stop,
                "reason": "fast_monitor_stop_loss",
                "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
            }
        )
    return signals


def _signal_alert_text(sig: dict[str, Any], status: str) -> str:
    return (
        "[긴급 신호]\n"
        f"자산: {sig.get('asset', 'UNKNOWN')}\n"
        f"행동: {sig.get('action', 'unknown')} {sig.get('side', 'unknown')}\n"
        f"전략: {sig.get('strategy', sig.get('signal_type', 'unknown'))}\n"
        f"상태: {status}"
    )


def _notify_fast(
    config: dict[str, Any],
    ts: str,
    signals: list[dict[str, Any]],
    alert_state: dict[str, Any],
    system_state: dict[str, Any],
) -> int:
    router = NotificationRouter(config)
    notif = config.get("notifications", {}).get("telegram", {})
    sent = 0
    seen = set(alert_state.get("seen", []))

    if bool(notif.get("send_signal_alerts", True)):
        for sig in signals:
            candle_ts = str(sig.get("timestamp", ts))[:16]
            strategy = str(sig.get("strategy", sig.get("signal_type", "risk_stop")))
            raw = f"{sig.get('asset','')}|{sig.get('side','')}|{candle_ts}|{strategy}"
            key = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            if key in seen:
                continue
            if router.send(_signal_alert_text(sig, "fast_exit")):
                sent += 1
                seen.add(key)

    if bool(notif.get("send_system_alerts", True)):
        stale_limit = int(config.get("system", {}).get("main_stale_seconds", 180))
        hb = system_state.get("last_heartbeat")
        if hb:
            now_dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
            hb_dt = datetime.fromisoformat(str(hb)).astimezone(timezone.utc)
            stale = (now_dt - hb_dt).total_seconds() > stale_limit
            last_stale = str(alert_state.get("last_main_stale_alert_ts", ""))
            if stale and (not last_stale or last_stale[:16] != ts[:16]):
                if router.send("[시스템 경고]\n메인 루프 heartbeat stale 감지"):
                    sent += 1
                    alert_state["last_main_stale_alert_ts"] = ts

    alert_state["seen"] = list(seen)
    return sent


def run_fast_once(base_dir: Path, config_rel_path: str = "config/config.yaml") -> dict[str, Any]:
    ensure_state_files(base_dir)
    config = load_config(base_dir, config_rel_path)

    try:
        fd = acquire_fast_lock(base_dir, int(config.get("system", {}).get("lock_stale_seconds", 180)))
    except FileExistsError:
        return {"skipped": True, "reason": "fast_lock_busy"}

    try:
        now_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        system_state = load_system_state(base_dir)
        positions = load_positions(base_dir)
        idempotency = load_idempotency(base_dir)
        alert_state = load_alert_idempotency(base_dir)
        trades = load_trades(base_dir)
        pending_orders = load_pending_orders(base_dir)

        stock_symbols = [a for a, p in positions.items() if p.get("engine") == "stock"]
        crypto_symbols = [a for a, p in positions.items() if p.get("engine") == "crypto"]

        data_source = str(config.get("data", {}).get("source", "mock")).lower()
        if data_source == "api" and (stock_symbols or crypto_symbols):
            try:
                snapshot = build_api_snapshot(base_dir, now_ts, config, stock_symbols, crypto_symbols)
                data_source = "api"
            except Exception:
                snapshot = make_mock_snapshot(now_ts, stock_symbols, crypto_symbols)
                data_source = "mock_fallback"
        else:
            snapshot = make_mock_snapshot(now_ts, stock_symbols, crypto_symbols)
            data_source = "mock"

        ts = snapshot["timestamp"]
        prices = _collect_prices(snapshot)
        signals = _management_signals(ts, positions, prices)

        positions, idempotency, trades, new_orders = execute_orders(
            base_dir=base_dir,
            mode=str(config.get("mode", "paper")),
            signals=signals,
            positions=positions,
            idempotency=idempotency,
            trades=trades,
            system_state=system_state,
            prices=prices,
            config=config,
            allow_new_entries=False,
            block_reason="FAST_MONITOR_ONLY",
        )
        pending_orders.extend(new_orders)

        system_state["fast_heartbeat"] = ts
        portfolio = _build_portfolio(ts, positions, prices)
        alert_sent = _notify_fast(config, ts, signals, alert_state, system_state)

        save_positions(base_dir, positions)
        save_portfolio(base_dir, portfolio)
        save_idempotency(base_dir, idempotency)
        save_alert_idempotency(base_dir, alert_state)
        save_trades(base_dir, trades)
        save_pending_orders(base_dir, pending_orders)
        save_system_state(base_dir, system_state)

        return {
            "timestamp": ts,
            "data_source": data_source,
            "fast_signals": len(signals),
            "alerts": alert_sent,
            "positions": portfolio.get("total_positions", 0),
        }
    finally:
        release_fast_lock(base_dir, fd)


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingRobot fast monitor")
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    print(json.dumps(run_fast_once(Path(args.base_dir).resolve(), args.config), ensure_ascii=False))


if __name__ == "__main__":
    main()
