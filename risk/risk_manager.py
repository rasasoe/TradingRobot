from __future__ import annotations

from typing import Any


def calculate_total_equity(cash: float, positions: dict[str, Any], prices: dict[str, float]) -> float:
    mtm = 0.0
    for asset, pos in positions.items():
        qty = float(pos.get("qty", 0.0))
        if qty == 0:
            continue
        px = prices.get(asset, float(pos.get("avg_price", 0.0)))
        mtm += qty * px
    return round(cash + mtm, 4)


def calculate_exposure(positions: dict[str, Any], prices: dict[str, float]) -> float:
    exposure = 0.0
    for asset, pos in positions.items():
        qty = abs(float(pos.get("qty", 0.0)))
        if qty == 0:
            continue
        px = prices.get(asset, float(pos.get("avg_price", 0.0)))
        exposure += qty * px
    return round(exposure, 4)


def can_enter_position(
    equity: float,
    exposure: float,
    proposed_notional: float,
    config: dict[str, Any],
) -> bool:
    max_total = float(config["capital"]["max_total_exposure"])
    max_single = float(config["capital"]["max_single_position_equity_pct"])
    if equity <= 0:
        return False
    if proposed_notional > equity * max_single:
        return False
    new_ratio = (exposure + proposed_notional) / equity
    return new_ratio <= max_total


def position_size_by_risk(
    equity: float,
    entry_price: float,
    stop_price: float,
    base_risk: float,
    r_multiplier: float,
) -> float:
    risk_budget = equity * base_risk * r_multiplier
    per_unit_risk = abs(entry_price - stop_price)
    if per_unit_risk <= 0:
        return 0.0
    return round(max(0.0, risk_budget / per_unit_risk), 6)
