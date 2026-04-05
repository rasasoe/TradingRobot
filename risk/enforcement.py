from __future__ import annotations

from typing import Any


def evaluate_enforcement(
    time_sync_ok: bool,
    pnl_ok: bool,
    drift_checked: bool,
    capital_event_block: bool,
    pnl_fail_streak: int,
) -> tuple[bool, list[str], list[str]]:
    block: list[str] = []
    violation: list[str] = []

    if not time_sync_ok:
        block.append("TIME_SYNC_UNMATCHED")
        violation.append("TIME_SYNC_UNMATCHED")

    if not pnl_ok:
        block.append("PNL_LOG_FAILED")
        if pnl_fail_streak >= 2:
            violation.append("PNL_LOG_FAILED_CONSECUTIVE")

    if not drift_checked:
        block.append("DRIFT_STATUS_UNCHECKED")
        violation.append("DRIFT_STATUS_UNCHECKED")

    if capital_event_block:
        block.append("CAPITAL_EVENT_CANDLE")

    allow_new_entries = len(block) == 0
    return allow_new_entries, block, violation
