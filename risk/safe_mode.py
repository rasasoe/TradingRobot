from __future__ import annotations


def update_safe_mode(system_state: dict, violation_reasons: list[str], streak_limit: int) -> None:
    if violation_reasons:
        system_state["violation_streak"] = int(system_state.get("violation_streak", 0)) + 1
        if int(system_state["violation_streak"]) >= int(streak_limit):
            system_state["safe_mode"] = True
    else:
        system_state["violation_streak"] = 0


def can_enter_new_position(system_state: dict, allow_from_enforcement: bool) -> bool:
    if not allow_from_enforcement:
        return False
    return not bool(system_state.get("safe_mode", False))
