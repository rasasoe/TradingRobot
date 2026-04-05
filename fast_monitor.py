from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import load_config
from data.live_data import make_api_snapshot
from data.mock_data import make_mock_snapshot
from execution.notifier import format_signal_alert, format_system_alert, send_telegram_message
from execution.runner import execute_orders
from orchestrator import _build_performance_snapshot, _build_portfolio_snapshot
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


def acquire_fast_lock(base_dir: Path) -> int:
    lock_path = base_dir / "state" / "run.lock"
    return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)


def release_fast_lock(base_dir: Path, fd: int) -> None:
    os.close(fd)
    (base_dir / "state" / "run.lock").unlink(missing_ok=True)


def _update_data(now_ts: datetime, config: dict[str, Any]) -> dict[str, Any]:
    data_cfg = config.get("data", {})
    source = str(data_cfg.get("source", "api")).lower()
    fallback = bool(data_cfg.get("fallback_to_mock_on_error", True))
    if source == "api":
        try:
            return make_api_snapshot(now_ts, config)
        except Exception:
            if not fallback:
                raise
    return make_mock_snapshot(now_ts, config)


def _collect_prices(snapshot: dict[str, Any]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for asset, bar in snapshot["stock"]["market"].items():
        prices[asset] = float(bar["close"])
    for asset, bar in snapshot["crypto"]["market"].items():
        prices[asset] = float(bar["close"])
    return prices


def _generate_stop_signals(ts: str, positions: dict[str, Any], prices: dict[str, float]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for asset, pos in positions.items():
        px = prices.get(asset)
        if px is None:
            continue
        side = str(pos.get("side", "long"))
        stop = float(pos.get("stop_price", 0.0))
        should_exit = (side == "long" and px <= stop) or (side == "short" and px >= stop)
        if not should_exit:
            continue
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
                "price": px,
                "stop_price": stop,
                "reason": "fast_monitor_stop_loss",
                "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
            }
        )
    return out


def _send_fast_alerts(
    ts: str,
    config: dict[str, Any],
    stop_signals: list[dict[str, Any]],
    alert_state: dict[str, Any],
    system_state: dict[str, Any],
) -> int:
    notif = config.get("notifications", {}).get("telegram", {})
    if not bool(notif.get("enabled", False)):
        return 0
    token = str(notif.get("bot_token", ""))
    chat_id = str(notif.get("chat_id", ""))
    if not token or not chat_id:
        return 0

    sent = 0
    seen = set(alert_state.get("seen", []))
    for sig in stop_signals:
        candle_ts = str(sig.get("timestamp", ts))[:16]
        key = f"{sig.get('asset','')}|{sig.get('side','')}|{candle_ts}|{sig.get('strategy','risk_stop')}"
        h = hashlib.sha256(key.encode("utf-8")).hexdigest()
        if h in seen:
            continue
        if send_telegram_message(token, chat_id, format_signal_alert(sig, "fast_stop_exit")):
            sent += 1
            seen.add(h)

    stale_limit = int(config.get("system", {}).get("main_stale_seconds", 180))
    last_main = system_state.get("last_heartbeat")
    if last_main:
        main_ts = datetime.fromisoformat(str(last_main)).astimezone(timezone.utc)
        now_dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
        stale = (now_dt - main_ts).total_seconds() > stale_limit
        last_stale_alert = str(alert_state.get("last_main_stale_alert_ts", ""))
        should_send_stale = stale and (not last_stale_alert or last_stale_alert[:16] != ts[:16])
        if should_send_stale:
            if send_telegram_message(token, chat_id, format_system_alert(["MAIN_LOOP_STALE"])):
                sent += 1
                alert_state["last_main_stale_alert_ts"] = ts

    alert_state["seen"] = list(seen)
    return sent


def run_fast_once(base_dir: Path, config_rel_path: str = "config/config.yaml") -> dict[str, Any]:
    ensure_state_files(base_dir)
    config = load_config(base_dir, config_rel_path)

    try:
        fd = acquire_fast_lock(base_dir)
    except FileExistsError:
        return {"skipped": True, "reason": "lock_busy"}
    try:
        now_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        snapshot = _update_data(now_ts, config)
        ts = snapshot["timestamp"]

        positions = load_positions(base_dir)
        idempotency = load_idempotency(base_dir)
        alert_state = load_alert_idempotency(base_dir)
        trades = load_trades(base_dir)
        pending_orders = load_pending_orders(base_dir)
        system_state = load_system_state(base_dir)

        prices = _collect_prices(snapshot)
        stop_signals = _generate_stop_signals(ts, positions, prices)

        positions, idempotency, trades, newly_filled = execute_orders(
            base_dir=base_dir,
            signals=stop_signals,
            positions=positions,
            idempotency=idempotency,
            trades=trades,
            system_state=system_state,
            prices=prices,
            config=config,
            allow_new_entries=False,
            block_reason="FAST_MONITOR_ONLY",
        )

        pending_orders.extend(newly_filled)
        system_state["fast_heartbeat"] = ts

        portfolio = _build_portfolio_snapshot(ts, positions, prices)
        _ = _build_performance_snapshot(base_dir, ts, config, system_state, positions, prices, trades)
        alert_sent = _send_fast_alerts(ts, config, stop_signals, alert_state, system_state)

        save_positions(base_dir, positions)
        save_portfolio(base_dir, portfolio)
        save_idempotency(base_dir, idempotency)
        save_alert_idempotency(base_dir, alert_state)
        save_trades(base_dir, trades)
        save_pending_orders(base_dir, pending_orders)
        save_system_state(base_dir, system_state)

        return {"timestamp": ts, "fast_signals": len(stop_signals), "alerts": alert_sent}
    finally:
        release_fast_lock(base_dir, fd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast monitor for stop-loss and health")
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    print(run_fast_once(Path(args.base_dir).resolve(), args.config))


if __name__ == "__main__":
    main()
