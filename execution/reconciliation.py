from __future__ import annotations

from pathlib import Path

from state.store import load_pending_orders, save_pending_orders


def reconcile_orders(base_dir: Path, mode: str) -> dict:
    pending = load_pending_orders(base_dir)
    changed = 0

    # paper/live_small skeleton: mark lingering pending as reconciled
    for row in pending:
        if row.get("status") == "pending":
            row["status"] = "reconciled"
            changed += 1

    save_pending_orders(base_dir, pending)
    return {"mode": mode, "reconciled": changed, "total": len(pending)}
