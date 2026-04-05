from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import load_config
from data.live_data import make_api_snapshot
from data.mock_data import make_mock_snapshot, validate_snapshot_sync
from engines.crypto_engine import run_crypto_engine
from engines.stock_engine import run_stock_engine
from execution.notifier import (
    format_portfolio_summary,
    format_risk_alert,
    format_signal_alert,
    format_system_alert,
    send_telegram_message,
)
from execution.runner import execute_orders
from risk.drift import evaluate_drift
from state.store import (
    append_jsonl,
    ensure_state_files,
    load_alert_idempotency,
    load_capital_events,
    load_idempotency,
    load_pending_orders,
    load_portfolio,
    load_positions,
    load_system_state,
    load_trades,
    save_idempotency,
    save_pending_orders,
    save_portfolio,
    save_positions,
    save_alert_idempotency,
    save_system_state,
    save_trades,
)


def acquire_lock(base_dir: Path) -> int:
    lock_path = base_dir / "state" / "run.lock"
    return os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)


def release_lock(base_dir: Path, fd: int) -> None:
    os.close(fd)
    (base_dir / "state" / "run.lock").unlink(missing_ok=True)


def update_data(now_ts: datetime, config: dict[str, Any]) -> tuple[dict[str, Any], str]:
    data_cfg = config.get("data", {})
    source = str(data_cfg.get("source", "mock")).lower()
    fallback = bool(data_cfg.get("fallback_to_mock_on_error", True))
    if source == "api":
        try:
            return make_api_snapshot(now_ts, config), "api"
        except Exception:
            if not fallback:
                raise
            return make_mock_snapshot(now_ts, config), "mock_fallback"
    return make_mock_snapshot(now_ts, config), "mock"


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

def _build_portfolio_snapshot(ts: str, positions: dict[str, Any], prices: dict[str, float]) -> dict[str, Any]:
    stock_items: list[dict[str, Any]] = []
    crypto_items: list[dict[str, Any]] = []
    for asset, pos in positions.items():
        qty = float(pos.get("qty", 0.0))
        avg = float(pos.get("avg_price", 0.0))
        px = float(prices.get(asset, avg))
        unrealized = (px - avg) * qty
        if pos.get("side") == "short":
            unrealized = -unrealized
        item = {
            "asset": asset,
            "side": pos.get("side", "long"),
            "qty": qty,
            "avg_price": avg,
            "mark_price": px,
            "unrealized_pnl": round(unrealized, 4),
            "stop_price": pos.get("stop_price", 0.0),
            "status": pos.get("status", "open"),
            "signal_type": pos.get("signal_type", "unknown"),
        }
        if pos.get("engine") == "stock":
            stock_items.append(item)
        else:
            crypto_items.append(item)
    return {
        "timestamp": ts,
        "stock": stock_items,
        "crypto": crypto_items,
        "total_positions": len(stock_items) + len(crypto_items),
    }


def _build_performance_snapshot(
    base_dir: Path,
    ts: str,
    config: dict[str, Any],
    system_state: dict[str, Any],
    positions: dict[str, Any],
    prices: dict[str, float],
    trades: list[dict[str, Any]],
) -> dict[str, Any]:
    initial_cash = float(config["capital"]["initial_cash"])
    cash = float(system_state.get("cash", initial_cash))
    realized = round(sum(float(t.get("pnl", 0.0)) for t in trades), 4)
    unrealized_val = 0.0
    for asset, pos in positions.items():
        qty = float(pos.get("qty", 0.0))
        avg = float(pos.get("avg_price", 0.0))
        px = float(prices.get(asset, avg))
        pnl = (px - avg) * qty
        if pos.get("side") == "short":
            pnl = -pnl
        unrealized_val += pnl
    unrealized = round(unrealized_val, 4)
    total_pnl = round(realized + unrealized, 4)
    equity = round(initial_cash + total_pnl, 4)
    return_pct = round((total_pnl / initial_cash) * 100, 4) if initial_cash > 0 else 0.0
    snapshot = {
        "timestamp": ts,
        "initial_cash": initial_cash,
        "cash": round(cash, 4),
        "equity": round(equity, 4),
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "total_pnl": total_pnl,
        "return_pct": return_pct,
    }
    append_jsonl(base_dir / "logs" / "performance.log", snapshot)
    return snapshot


def _publish_signals(
    base_dir: Path,
    timestamp: str,
    signals: list[dict[str, Any]],
    allow_new_entries: bool,
    block_reason: str,
    config: dict[str, Any],
    emit_console_override: bool,
) -> int:
    output_cfg = config.get("signal_output", {})
    emit_log = bool(output_cfg.get("emit_log", True))
    emit_console = bool(output_cfg.get("emit_console", False) or emit_console_override)

    emitted = 0
    for sig in signals:
        status = "candidate"
        if sig.get("action") == "enter" and not allow_new_entries:
            status = f"blocked:{block_reason or 'fail_closed'}"
        payload = {
            "timestamp": timestamp,
            "engine": sig.get("engine", "unknown"),
            "asset": sig.get("asset", "unknown"),
            "action": sig.get("action", "unknown"),
            "side": sig.get("side", "unknown"),
            "signal_type": sig.get("signal_type", "unknown"),
            "price": sig.get("price", 0.0),
            "score": sig.get("score", 0.0),
            "reason": sig.get("reason", ""),
            "status": status,
        }
        if emit_log:
            append_jsonl(base_dir / "logs" / "signals.log", payload)
        if emit_console:
            print(json.dumps(payload, ensure_ascii=True))
        emitted += 1
    return emitted


def _notify_telegram(
    config: dict[str, Any],
    signals: list[dict[str, Any]],
    allow_new_entries: bool,
    block_reason: str,
    reasons: list[str],
    drift_warning: str,
    alert_idempotency: dict[str, Any],
    portfolio: dict[str, Any],
    performance: dict[str, Any],
) -> dict[str, Any]:
    notif = config.get("notifications", {}).get("telegram", {})
    if not bool(notif.get("enabled", False)):
        return {"sent": 0, "seen": alert_idempotency.get("seen", [])}

    bot_token = str(notif.get("bot_token", ""))
    chat_id = str(notif.get("chat_id", ""))
    seen = set(alert_idempotency.get("seen", []))
    sent = 0

    if reasons and bool(notif.get("send_system_alerts", True)):
        if send_telegram_message(bot_token, chat_id, format_system_alert(reasons)):
            sent += 1

    if drift_warning == "drift_detected" and bool(notif.get("send_risk_alerts", True)):
        if send_telegram_message(bot_token, chat_id, format_risk_alert(drift_warning)):
            sent += 1

    if bool(notif.get("send_signal_alerts", True)):
        for sig in signals:
            base = (
                f"{sig.get('timestamp','')}|{sig.get('engine','')}|{sig.get('asset','')}|"
                f"{sig.get('action','')}|{sig.get('side','')}|{sig.get('signal_type','')}|{sig.get('price',0)}"
            )
            sig_hash = hashlib.sha256(base.encode("utf-8")).hexdigest()
            if sig_hash in seen:
                continue
            status = "candidate"
            if sig.get("action") == "enter" and not allow_new_entries:
                status = f"blocked:{block_reason or 'fail_closed'}"
            if send_telegram_message(bot_token, chat_id, format_signal_alert(sig, status)):
                sent += 1
                seen.add(sig_hash)

    if bool(notif.get("send_portfolio_summary", True)):
        if send_telegram_message(bot_token, chat_id, format_portfolio_summary(portfolio, performance)):
            sent += 1

    alert_idempotency["seen"] = list(seen)
    return {"sent": sent, "seen": alert_idempotency["seen"]}


def run_once(base_dir: Path, config_rel_path: str = "config/config.yaml", emit_signals: bool = False) -> dict[str, Any]:
    ensure_state_files(base_dir)
    config = load_config(base_dir, config_rel_path)

    fd = acquire_lock(base_dir)
    try:
        now_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        snapshot, data_source = update_data(now_ts, config)
        ts = snapshot["timestamp"]

        positions = load_positions(base_dir)
        _ = load_portfolio(base_dir)
        idempotency = load_idempotency(base_dir)
        alert_idempotency = load_alert_idempotency(base_dir)
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
        emitted_signals = _publish_signals(
            base_dir=base_dir,
            timestamp=ts,
            signals=all_signals,
            allow_new_entries=allow_new_entries,
            block_reason=block_reason,
            config=config,
            emit_console_override=emit_signals,
        )
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

        portfolio = _build_portfolio_snapshot(ts, positions, prices)
        performance = _build_performance_snapshot(
            base_dir=base_dir,
            ts=ts,
            config=config,
            system_state=system_state,
            positions=positions,
            prices=prices,
            trades=trades,
        )
        notify_result = _notify_telegram(
            config=config,
            signals=all_signals,
            allow_new_entries=allow_new_entries,
            block_reason=block_reason,
            reasons=reasons,
            drift_warning=drift.get("warning", "unknown"),
            alert_idempotency=alert_idempotency,
            portfolio=portfolio,
            performance=performance,
        )
        save_positions(base_dir, positions)
        save_portfolio(base_dir, portfolio)
        save_idempotency(base_dir, idempotency)
        save_alert_idempotency(base_dir, alert_idempotency)
        save_trades(base_dir, trades)
        save_pending_orders(base_dir, pending_orders)
        save_system_state(base_dir, system_state)

        return {
            "timestamp": ts,
            "allow_new_entries": allow_new_entries,
            "violation_streak": system_state["violation_streak"],
            "safe_mode": bool(system_state.get("safe_mode", False)),
            "signals": len(all_signals),
            "signals_emitted": emitted_signals,
            "telegram_sent": int(notify_result.get("sent", 0)),
            "drift_warning": drift.get("warning", "unknown"),
            "data_source": data_source,
            "portfolio": portfolio,
            "performance": performance,
        }
    finally:
        release_lock(base_dir, fd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-asset separated trading orchestrator")
    parser.add_argument("--base-dir", default=".", help="Project base directory")
    parser.add_argument("--config", default="config/config.yaml", help="Config file path")
    parser.add_argument("--emit-signals", action="store_true", help="Print generated signals to stdout")
    args = parser.parse_args()

    result = run_once(Path(args.base_dir).resolve(), args.config, emit_signals=args.emit_signals)
    print(json.dumps(result, ensure_ascii=False))
    print(
        f"[요약] 신규진입허용={result['allow_new_entries']} "
        f"안전모드={result['safe_mode']} "
        f"포트폴리오수={result['portfolio']['total_positions']} "
        f"텔레그램전송={result['telegram_sent']} "
        f"누적수익률={result['performance']['return_pct']}%"
    )


if __name__ == "__main__":
    main()
