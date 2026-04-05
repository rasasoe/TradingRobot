from __future__ import annotations

import hashlib
from typing import Any

from execution.order_router import route_order
from risk.risk_manager import (
    calculate_exposure,
    calculate_total_equity,
    can_enter_position,
    position_size_by_risk,
)
from state.store import append_jsonl


def idempotency_key(signal: dict[str, Any]) -> str:
    candle_ts = str(signal.get("timestamp", ""))[:16]
    strategy = str(signal.get("strategy", signal.get("signal_type", "unknown")))
    base = f"{signal.get('asset','')}|{signal.get('side','')}|{candle_ts}|{strategy}|{signal.get('action','')}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _passes_orderbook(signal: dict[str, Any], config: dict[str, Any]) -> tuple[bool, str]:
    ob = signal.get("orderbook", {})
    top3 = float(ob.get("top3_ratio", 1.0))
    spread = float(ob.get("spread_pct", 99.0))
    if top3 > float(config.get("execution", {}).get("top3_orderbook_ratio_limit", 0.2)):
        return False, "blocked:orderbook_top3_ratio"
    if spread > float(config.get("execution", {}).get("spread_limit_pct", 0.5)):
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
    mode: str,
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

    initial_cash = float(config.get("capital", {}).get("initial_cash", 100000.0))
    if system_state.get("cash") is None:
        system_state["cash"] = initial_cash
    cash = float(system_state["cash"])

    equity = calculate_total_equity(cash, positions, prices)
    exposure = calculate_exposure(positions, prices)

    role_r_limit = {"core": 5.0, "leader": 3.0, "alpha": 2.0, "quarantine": 0.0}
    role_crypto_cap = {"BTCUSDT": 0.10, "SOLUSDT": 0.15}

    def current_role_r(role: str) -> float:
        total = 0.0
        for a, p in positions.items():
            if str(p.get("role", "")).lower() != role:
                continue
            qty = abs(float(p.get("qty", 0.0)))
            px = float(prices.get(a, float(p.get("avg_price", 0.0))))
            stop = float(p.get("stop_price", p.get("avg_price", px)))
            total += (qty * abs(px - stop)) / max(1e-9, equity * 0.01)
        return total

    alpha_open = any(str(p.get("role", "")).lower() == "alpha" for p in positions.values())
    alpha_opened_this_batch = False

    for sig in signals:
        key = idempotency_key(sig)
        if key in seen:
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:idempotent"))
            continue

        if sig["action"] == "enter" and not allow_new_entries:
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, block_reason or "blocked:fail_closed"))
            seen.add(key)
            continue

        ob_ok, ob_reason = _passes_orderbook(sig, config)
        if sig["action"] == "enter" and not ob_ok:
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, ob_reason))
            seen.add(key)
            continue

        asset = sig["asset"]
        side = sig["side"]
        price = float(sig["price"])
        stop_price = float(sig["stop_price"])
        role = str(sig.get("role", "core")).lower()

        if sig["action"] == "enter":
            if role == "quarantine":
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:quarantine"))
                seen.add(key)
                continue
            if role == "alpha" and (alpha_open or alpha_opened_this_batch):
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:alpha_limit"))
                seen.add(key)
                continue
            if current_role_r(role) >= role_r_limit.get(role, 0.0):
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:role_r_limit"))
                seen.add(key)
                continue

            target_r = float(sig.get("target_r", 1.0))
            qty = position_size_by_risk(
                equity=equity,
                entry_price=price,
                stop_price=stop_price,
                base_risk=float(config.get("capital", {}).get("base_risk_per_trade", 0.01)) * target_r,
                r_multiplier=float(system_state.get("r_multiplier", 1.0)),
            )
            max_single = equity * float(config.get("capital", {}).get("max_single_position_equity_pct", 0.1))
            if price > 0:
                qty = min(qty, max_single / price)

            # hard minimum order size
            if qty < 1.0 and asset.endswith("USDT") is False:
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:min_share"))
                seen.add(key)
                continue
            if asset.endswith("USDT"):
                min_qty = 0.000001
                if qty < min_qty:
                    append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:min_crypto_qty"))
                    seen.add(key)
                    continue

            notional = qty * price
            if qty <= 0 or not can_enter_position(equity, exposure, notional, config):
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:risk_limit"))
                seen.add(key)
                continue
            if asset.endswith("USDT"):
                total_crypto = 0.0
                for a, p in positions.items():
                    if a.endswith("USDT"):
                        total_crypto += abs(float(p.get("qty", 0.0))) * float(prices.get(a, float(p.get("avg_price", 0.0))))
                if (total_crypto + notional) / max(equity, 1e-9) > 0.30:
                    append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:crypto_cap_total"))
                    seen.add(key)
                    continue
                cap = role_crypto_cap.get(asset, 0.05)
                if notional / max(equity, 1e-9) > cap:
                    append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:crypto_cap_asset"))
                    seen.add(key)
                    continue

            order = route_order(mode, sig)
            if order.get("status") != "filled":
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:order_rejected"))
                seen.add(key)
                continue

            fill_price = float(order["fill_price"])
            signed_qty = qty if side == "long" else -qty
            positions[asset] = {
                "engine": sig["engine"],
                "signal_type": sig["signal_type"],
                "role": role,
                "side": side,
                "qty": signed_qty,
                "avg_price": fill_price,
                "stop_price": stop_price,
                "status": "open",
            }
            cash -= abs(qty * fill_price)
            exposure += abs(qty * fill_price)
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, True))
            pending_orders.append({"signal_hash": key, "asset": asset, "status": "filled", "order_id": order["order_id"]})
            if role == "alpha":
                alpha_opened_this_batch = True

        elif sig["action"] == "exit":
            pos = positions.get(asset)
            if not pos or float(pos.get("qty", 0.0)) == 0:
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:no_position"))
                seen.add(key)
                continue
            order = route_order(mode, sig)
            if order.get("status") != "filled":
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:order_rejected"))
                seen.add(key)
                continue
            fill_price = float(order["fill_price"])
            qty = float(pos["qty"])
            pnl = (fill_price - float(pos["avg_price"])) * qty
            if pos.get("side") == "short":
                pnl = -pnl
            cash += abs(qty * fill_price) + pnl
            trades.append(
                {
                    "timestamp": sig["timestamp"],
                    "engine": pos.get("engine", "unknown"),
                    "asset": asset,
                    "signal_type": pos.get("signal_type", "unknown"),
                    "r": pnl / max(1e-9, abs(float(pos["avg_price"]) - float(pos["stop_price"])) * abs(qty)),
                    "pnl": pnl,
                    "volatility": sig.get("atr", 0.0),
                }
            )
            positions.pop(asset, None)
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, True))
            pending_orders.append({"signal_hash": key, "asset": asset, "status": "filled", "order_id": order["order_id"]})
        elif sig["action"] == "reduce":
            pos = positions.get(asset)
            if not pos or float(pos.get("qty", 0.0)) == 0:
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:no_position"))
                seen.add(key)
                continue
            frac = float(sig.get("reduce_fraction", 0.5))
            frac = max(0.0, min(frac, 1.0))
            old_qty = float(pos["qty"])
            reduce_qty = abs(old_qty) * frac
            remain_qty = abs(old_qty) - reduce_qty
            order = route_order(mode, sig)
            if order.get("status") != "filled":
                append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, False, "blocked:order_rejected"))
                seen.add(key)
                continue
            fill_price = float(order["fill_price"])
            pnl = (fill_price - float(pos["avg_price"])) * (reduce_qty if old_qty > 0 else -reduce_qty)
            if pos.get("side") == "short":
                pnl = -pnl
            cash += abs(reduce_qty * fill_price) + pnl
            trades.append(
                {
                    "timestamp": sig["timestamp"],
                    "engine": pos.get("engine", "unknown"),
                    "asset": asset,
                    "signal_type": pos.get("signal_type", "unknown"),
                    "r": pnl / max(1e-9, abs(float(pos["avg_price"]) - float(pos["stop_price"])) * max(reduce_qty, 1e-9)),
                    "pnl": pnl,
                    "volatility": sig.get("atr", 0.0),
                }
            )
            if remain_qty <= 0:
                positions.pop(asset, None)
            else:
                positions[asset]["qty"] = remain_qty if old_qty > 0 else -remain_qty
            append_jsonl(base_dir / "logs" / "decisions.log", _decision_row(sig, True))
            pending_orders.append({"signal_hash": key, "asset": asset, "status": "filled", "order_id": order["order_id"]})

        seen.add(key)

    idempotency["seen"] = list(seen)
    system_state["cash"] = round(cash, 4)
    return positions, idempotency, trades, pending_orders
