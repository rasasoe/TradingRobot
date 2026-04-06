from __future__ import annotations

from typing import Any

from state.store import append_jsonl


def apply_qam_policy(
    base_dir,
    ts: str,
    stock_signals: list[dict[str, Any]],
    crypto_signals: list[dict[str, Any]],
    management_signals: list[dict[str, Any]],
    allow_new_entries: bool,
    block_reason: str,
    safe_mode: bool,
    alert_severity: str = "",
    alert_tag: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Always keep protection/exit signals
    forced_management = [s for s in management_signals if s.get("action") in {"exit", "reduce"}]

    # New entries are blocked in fail-closed or safe-mode
    if safe_mode or (not allow_new_entries):
        final = forced_management
        decision = {
            "timestamp": ts,
            "mode": "SAFE MODE" if safe_mode else "FAIL-CLOSED",
            "allow_entries": False,
            "stock_in": len(stock_signals),
            "crypto_in": len(crypto_signals),
            "mgmt_in": len(management_signals),
            "out": len(final),
            "reason": block_reason or "blocked_by_policy",
            "alert_severity": alert_severity,
            "alert_tag": alert_tag,
        }
        append_jsonl(base_dir / "logs" / "qam_decision.log", decision)
        return final, decision

    # Alpha mutual exclusion in same batch
    s_alpha = [s for s in stock_signals if str(s.get("role", "")).lower() == "alpha" and s.get("action") == "enter"]
    c_alpha = [s for s in crypto_signals if str(s.get("role", "")).lower() == "alpha" and s.get("action") == "enter"]
    if s_alpha and c_alpha:
        crypto_signals = [s for s in crypto_signals if s not in c_alpha]

    # Stock entry restriction: only SPY or stock alpha
    stock_filtered: list[dict[str, Any]] = []
    for s in stock_signals:
        if s.get("action") != "enter":
            stock_filtered.append(s)
            continue
        if s.get("asset") == "SPY" or str(s.get("role", "")).lower() == "alpha":
            stock_filtered.append(s)

    final = stock_filtered + crypto_signals + forced_management
    decision = {
        "timestamp": ts,
        "mode": "NORMAL",
        "allow_entries": True,
        "stock_in": len(stock_signals),
        "crypto_in": len(crypto_signals),
        "mgmt_in": len(management_signals),
        "out": len(final),
        "reason": "qam_policy_applied",
        "alert_severity": alert_severity,
        "alert_tag": alert_tag,
    }
    append_jsonl(base_dir / "logs" / "qam_decision.log", decision)
    return final, decision
