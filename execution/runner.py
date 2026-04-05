from __future__ import annotations

import hashlib
from typing import Any

from risk.risk_manager import (
    calculate_exposure,
    calculate_total_equity,
    can_enter_position,
    position_size_by_risk,
)
from state.store import append_jsonl


def signal_hash(signal: dict[str, Any]) -> str:
    candle_ts = str(signal.get("timestamp", ""))[:16]
    strategy_name = str(signal.get("strategy", signal.get("signal_type", "unknown")))
    base = (
        f"{signal.get('asset','')}|{signal.get('side','')}|{candle_ts}|{strategy_name}|{signal.get('action','')}"
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _passes_orderbook(signal: dict[str, Any], config: dict[str, Any]) -> tuple[bool, str]:
    ob = signal.get("orderbook", {})
    top3 = float(ob.get("top3_ratio", 1.0))
    spread = float(ob.get("spread_pct", 99.0))

    if top3 > float(config["execution"]["top3_orderbook_ratio_limit"]):
        return False, "blocked:orderbook_top3_ratio"
    if spread > float(config["execution"]["spread_limit_pct"]):
        return False, "blocked:spread_limit"
    return True, "ok"


def _decision_row(signal: dict[str, Any], executed: bool, blocked_reason: str = "") -> dict[str, Any]:
    return {
        "timestamp": signal["timestamp"],
        "asset": signal["asset"],
        "regime": signal.get("regime", "n/a"),
        "score": signal.get("score", 0.0),
        "ATR": signal.get("atr", 0.0),
        "reason": signal.get("reason", ""),
        "executed": executed,
        "blocked_reason": blocked_reason,
    }


def execute_orders(
    base_dir,
    signals: list[dict[str, Any]],
    positions: dict[str, Any],
    idempotency: dict[str, Any],
    trades: list[dict[str, Any]],
    system_state: dict[str, Any],
    prices: dict[str, float],
    config: dict[str, Any],
    allow_new_entries: bool,
    block_reason: str = "",
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    seen = set(idempotency.get("seen", []))
    pending_orders: list[dict[str, Any]] = []

    cash = float(system_state.get("cash", config["capital"]["initial_cash"]))
    equity = calculate_total_equity(cash, positions, prices)
    exposure = calculate_exposure(positions, prices)

    for signal in signals:
        s_hash = signal_hash(signal)

        if s_hash in seen:
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, False, "blocked:idempotent"))
            continue

        if signal["action"] == "enter" and not allow_new_entries:
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, False, block_reason or "blocked:fail_closed"))
            seen.add(s_hash)
            continue

        pass_ob, ob_reason = _passes_orderbook(signal, config)
        if signal["action"] == "enter" and not pass_ob:
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, False, ob_reason))
            seen.add(s_hash)
            continue

        asset = signal["asset"]
        side = signal["side"]
        price = float(signal["price"])
        stop_price = float(signal["stop_price"])

        if signal["action"] == "enter":
            qty = position_size_by_risk(
                equity=equity,
                entry_price=price,
                stop_price=stop_price,
                base_risk=float(config["capital"]["base_risk_per_trade"]),
                r_multiplier=float(system_state.get("r_multiplier", 1.0)),
            )
            max_single_notional = equity * float(config["capital"]["max_single_position_equity_pct"])
            if price > 0:
                qty = min(qty, max_single_notional / price)
            notional = qty * price
            if qty <= 0 or not can_enter_position(equity, exposure, notional, config):
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, False, "blocked:risk_limit"))
                seen.add(s_hash)
                continue

            signed_qty = qty if side == "long" else -qty
            positions[asset] = {
                "engine": signal["engine"],
                "signal_type": signal["signal_type"],
                "side": side,
                "qty": signed_qty,
                "avg_price": price,
                "stop_price": stop_price,
                "status": "open",
            }
            cash -= abs(notional)
            exposure += abs(notional)
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, True))

        elif signal["action"] == "exit":
            pos = positions.get(asset)
            if not pos or float(pos.get("qty", 0.0)) == 0:
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, False, "blocked:no_position"))
                seen.add(s_hash)
                continue

            qty = float(pos["qty"])
            pnl = (price - float(pos["avg_price"])) * qty
            if pos.get("side") == "short":
                pnl = -pnl
            cash += abs(qty * price) + pnl
            trades.append(
                {
                    "timestamp": signal["timestamp"],
                    "engine": pos.get("engine", "unknown"),
                    "asset": asset,
                    "signal_type": pos.get("signal_type", "unknown"),
                    "r": pnl / max(1e-9, abs(float(pos["avg_price"]) - float(pos["stop_price"])) * abs(qty)),
                    "pnl": pnl,
                    "volatility": signal.get("atr", 0.0),
                }
            )
            positions.pop(asset, None)
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(signal, True))

        seen.add(s_hash)
        pending_orders.append({"signal_hash": s_hash, "asset": asset, "status": "filled"})

    idempotency["seen"] = list(seen)
    system_state["cash"] = round(cash, 4)
    return positions, idempotency, trades, pending_orders
