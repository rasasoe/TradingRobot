from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import load_config
from data.mock_data import make_mock_snapshot, validate_snapshot_sync
from engines.crypto_engine import run_crypto_engine
from engines.stock_engine import run_stock_engine
from execution.runner import execute_orders
from risk.drift import evaluate_drift
from state.store import (
    append_jsonl,
    ensure_state_files,
    load_capital_events,
    load_idempotency,
    load_pending_orders,
    load_positions,
    load_system_state,
    load_trades,
    save_idempotency,
    save_pending_orders,
    save_positions,
    save_system_state,
    save_trades,
)


def acquire_lock(base_dir: Path) -> int:
    lock_path = base_dir / "state" / "run.lock"
    return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)


def release_lock(base_dir: Path, fd: int) -> None:
    os.close(fd)
    (base_dir / "state" / "run.lock").unlink(missing_ok=True)


def update_data(now_ts: datetime, config: dict[str, Any]) -> dict[str, Any]:
    return make_mock_snapshot(now_ts, config)


def validate_data(snapshot: dict[str, Any]) -> bool:
    return validate_snapshot_sync(snapshot)


def _collect_prices(snapshot: dict[str, Any]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for asset, bar in snapshot["stock"]["market"].items():
        prices[asset] = float(bar["close"])
    for asset, bar in snapshot["crypto"]["market"].items():
        prices[asset] = float(bar["close"])
    return prices


def _generate_management_signals(ts: str, positions: dict[str, Any], prices: dict[str, float]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for asset, pos in positions.items():
        px = prices.get(asset)
        if px is None:
            continue
        side = pos.get("side", "long")
        stop = float(pos.get("stop_price", 0.0))
        should_exit = (side == "long" and px <= stop) or (side == "short" and px >= stop)
        if should_exit:
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
                    "price": px,
                    "stop_price": stop,
                    "reason": "stop_loss_triggered",
                    "orderbook": {"top3_ratio": 0.0, "spread_pct": 0.0},
                }
            )
    return signals


def _capital_event_blocks_entry(ts: str, events: list[dict[str, Any]]) -> bool:
    candle_key = ts[:16]
    return any(e.get("timestamp", "")[:16] == candle_key for e in events)


def _write_pnl_log(base_dir: Path, ts: str, positions: dict[str, Any], prices: dict[str, float]) -> bool:
    try:
        grouped: dict[tuple[str, str, str], float] = {}
        for asset, pos in positions.items():
            qty = float(pos.get("qty", 0.0))
            if qty == 0:
                continue
            px = prices.get(asset, float(pos.get("avg_price", 0.0)))
            avg = float(pos.get("avg_price", 0.0))
            pnl = (px - avg) * qty
            if pos.get("side") == "short":
                pnl = -pnl
            key = (pos.get("engine", "unknown"), asset, pos.get("signal_type", "unknown"))
            grouped[key] = grouped.get(key, 0.0) + pnl

        for (engine, asset, signal_type), pnl in grouped.items():
            append_jsonl(
                base_dir / "logs" / "pnl.log",
                {
                    "timestamp": ts,
                    "engine": engine,
                    "asset": asset,
                    "signal_type": signal_type,
                    "pnl": round(pnl, 4),
                },
            )
        if not grouped:
            append_jsonl(
                base_dir / "logs" / "pnl.log",
                {
                    "timestamp": ts,
                    "engine": "none",
                    "asset": "none",
                    "signal_type": "none",
                    "pnl": 0.0,
                },
            )
        return True
    except Exception:
        return False


def _record_violation(base_dir: Path, ts: str, reasons: list[str]) -> None:
    append_jsonl(base_dir / "logs" / "violations.log", {"timestamp": ts, "reasons": reasons})


def run_once(base_dir: Path, config_rel_path: str = "config/config.yaml") -> dict[str, Any]:
    ensure_state_files(base_dir)
    config = load_config(base_dir, config_rel_path)

    fd = acquire_lock(base_dir)
    try:
        now_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        snapshot = update_data(now_ts, config)
        ts = snapshot["timestamp"]

        positions = load_positions(base_dir)
        idempotency = load_idempotency(base_dir)
        trades = load_trades(base_dir)
        pending_orders = load_pending_orders(base_dir)
        system_state = load_system_state(base_dir)

        prices = _collect_prices(snapshot)
        management_signals = _generate_management_signals(ts, positions, prices)

        stock_signals = run_stock_engine(snapshot, config)
        crypto_signals = run_crypto_engine(snapshot, config, system_state.get("disabled_strategies", []))
        entry_signals = stock_signals + crypto_signals

        drift = evaluate_drift(now_ts, trades, config, system_state)
        pnl_ok = _write_pnl_log(base_dir, ts, positions, prices)
        time_sync_ok = validate_data(snapshot)

        reasons: list[str] = []
        if not time_sync_ok:
            reasons.append("TIME_SYNC_UNMATCHED")
        if not pnl_ok:
            reasons.append("PNL_LOG_FAILED")
        if not drift.get("checked", False):
            reasons.append("DRIFT_STATUS_UNCHECKED")

        if _capital_event_blocks_entry(ts, load_capital_events(base_dir)):
            reasons.append("CAPITAL_EVENT_CANDLE")

        if reasons:
            system_state["violation_streak"] = int(system_state.get("violation_streak", 0)) + 1
            if system_state["violation_streak"] >= int(config["system"]["safe_mode_violation_streak"]):
                system_state["safe_mode"] = True
            _record_violation(base_dir, ts, reasons)
            allow_new_entries = False
            block_reason = "|".join(reasons)
        else:
            system_state["violation_streak"] = 0
            allow_new_entries = not bool(system_state.get("safe_mode", False))
            block_reason = "SAFE_MODE" if system_state.get("safe_mode", False) else ""

        all_signals = entry_signals + management_signals
        positions, idempotency, trades, newly_filled = execute_orders(
            base_dir=base_dir,
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

        save_positions(base_dir, positions)
        save_idempotency(base_dir, idempotency)
        save_trades(base_dir, trades)
        save_pending_orders(base_dir, pending_orders)
        save_system_state(base_dir, system_state)

        return {
            "timestamp": ts,
            "allow_new_entries": allow_new_entries,
            "violation_streak": system_state["violation_streak"],
            "safe_mode": bool(system_state.get("safe_mode", False)),
            "signals": len(all_signals),
            "drift_warning": drift.get("warning", "unknown"),
        }
    finally:
        release_lock(base_dir, fd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-asset separated trading orchestrator")
    parser.add_argument("--base-dir", default=".", help="Project base directory")
    parser.add_argument("--config", default="config/config.yaml", help="Config file path")
    args = parser.parse_args()

    result = run_once(Path(args.base_dir).resolve(), args.config)
    print(result)


if __name__ == "__main__":
    main()
